import sys
import pandas as pd
import time
from ast import literal_eval
import requests as r
from lookups import OLD_TO_NEW_CBSAID, MONTH_FORMAT, REDFIN_MSA_TO_CBSA
from enums import DefaultGeoIds, ProductionEnvironment
from database import mongoclient
from enums import GeoLevels
from copy import deepcopy

BUILDING_PERMIT_URL_CBSA = "https://www2.census.gov/econ/bps/Metro/ma{}{}c.txt"
BUILDING_PERMIT_URL_COUNTY = "https://www2.census.gov/econ/bps/Metro/ma{}{}c.txt"

MONTHS = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
# MONTHS = ['12']

BUILDING_PERMIT_YEARS = [2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022]
# BUILDING_PERMIT_YEARS = [2021]


def run_cbsa_building_permit(geo_level, geoid_field, geoname_field):
    retry_count = 0
    latest_insert_year = 2007
    category_name = 'buildingpermits'
    geoid_field = geoid_field

    geographies_df = mongoclient.query_geography(geo_level=GeoLevels.CBSA, stateid=DefaultGeoIds.CBSA)
    cbsaid_list = list(geographies_df[geoid_field])

    column_map = {
        'Survey Date': 'survey_date',
        'CBSA Code': geoid_field,
        'CBSA Name': 'cbsaidname',
        '1-unit Units': 'unit_1',
        '2-units Units': 'units_2',
        '3-4 units Units': 'units_3_to_4',
        '5+ units Units': 'units_5plus'
    }

    collection_find_finished_runs = {'category': category_name, 'geo_level': GeoLevels.CBSA.value}
    latest_insert = mongoclient.get_finished_runs(collection_find_finished_runs)

    if len(latest_insert) > 0:
        latest_insert_year = latest_insert.year.iloc[0]

    for year in BUILDING_PERMIT_YEARS:
        if year <= latest_insert_year:
            print('Skipping year {}. Already found data.'.format(year))
            continue

        building_permit_df = pd.DataFrame(columns=[geoid_field,'dates','unit_1','units_2','units_3_to_4','units_5plus', 'total'])
        building_permit_dict = {}
        latest_insert_month = ''

        for month in MONTHS:
            df = None
            try:
                query_year = str(year)[-2:]
                query_url = BUILDING_PERMIT_URL_CBSA.format(query_year, month)
                print("Sending building permits request: {}".format(query_url))
                data = r.get(query_url)
            except:
                print("Error making GET request. Trying again")
                print("Sleep 5 seconds before retrying census2 api calls")
                time.sleep(5)
                retry_count += 1
                continue

            if data.status_code != 200:
                retry_count += 1
                print("ERROR - bad request. Status code: {}. Query: {}".format(data.status_code, query_url))

                if retry_count > 3:
                    print("ERROR - too many bad requests. Status code: {}. Query: {}".format(data.status_code, query_url))
                    break

                if data.reason == 'Not Found':
                    continue
                else:
                    print('Unexpected error on query: ', data)
                    sys.exit()

            headers = []
            for i, line in enumerate(data.text.splitlines()):
                line_list = line.split(',')
                if i == 0:
                    headers = line_list
                elif i == 1:
                    header1_length = len(headers)
                    headers2_length = len(line_list)

                    if headers2_length - header1_length != 1:
                        print("Header2 length is greater than header 1")
                        sys.exit()

                    append_headers = line_list

                    for i, aheader in enumerate(append_headers):
                        if i == header1_length:
                            headers.append(aheader)
                            break
                        headers[i] = headers[i] + ' ' + aheader

                    if df is None:
                        df = pd.DataFrame(columns=headers)
                        df = df.rename(columns=column_map)

                else:
                    if len(line_list) < 2:
                        continue

                    df_length = len(df)
                    df.loc[df_length] = line_list

            common = df.merge(building_permit_df, on=[geoid_field, geoid_field])
            missing_from_current_month = df[(~df.cbsacode.isin(common.cbsacode))]

            if len(building_permit_df) > 0 and len(missing_from_current_month) > 0:
                print('ERROR: Why are we missing msa in same year?')
                sys.exit()

            for i, row in df.iterrows():
                cbsaid = row[geoid_field]

                if cbsaid in OLD_TO_NEW_CBSAID.keys():
                    print('updating cbsacode')
                    cbsaid = OLD_TO_NEW_CBSAID[cbsaid]

                if cbsaid in REDFIN_MSA_TO_CBSA.keys():
                    print('updating cbsacode')
                    cbsaid = REDFIN_MSA_TO_CBSA[cbsaid]


                if cbsaid not in cbsaid_list:
                    print('No cbsa match for cbsacode: {}. cbsaname: {}'.format(cbsaid, row.cbsaidname))
                    continue

                survey_date = row['survey_date']
                year_string = survey_date[:4]
                month_string = survey_date[-2:]

                date_string = MONTH_FORMAT[month_string] + ' ' + year_string
                unit_1 = int(row['unit_1'])
                units_2_to_4 = int(row['units_2']) + int(row['units_3_to_4'])
                units_5plus = int(row['units_5plus'])
                total = unit_1 + units_2_to_4 + units_5plus

                if cbsaid in building_permit_dict.keys():
                    building_permit_dict[cbsaid][category_name]['dates'].append(date_string)
                    building_permit_dict[cbsaid][category_name]['unit_1'].append(unit_1)
                    building_permit_dict[cbsaid][category_name]['units_2_to_4'].append(units_2_to_4)
                    building_permit_dict[cbsaid][category_name]['units_5plus'].append(units_5plus)
                    building_permit_dict[cbsaid][category_name]['total'].append(total)
                else:
                    building_permit_dict[cbsaid] = {
                        geoid_field: cbsaid,
                        'geolevel':GeoLevels.CBSA.value,
                        category_name: {
                            'dates': [date_string],
                            'unit_1': [unit_1],
                            'units_2_to_4': [units_2_to_4],
                            'units_5plus': [units_5plus],
                            'total': [total]
                        }
                    }

                latest_insert_month = month

        success = store_building_permits(building_permit_dict, geo_level, latest_insert_year, geoid_field, category_name)

        collection_add_finished_run = {
            'year': year,
            # 'latest_insert_month': latest_insert_month,
            'category': category_name,
            'geo_level': GeoLevels.CBSA.value,
        }

        if success and latest_insert_month == '12':
            mongoclient.update_finished_run(collection_add_finished_run, geo_level=geo_level, category=category_name)

def store_building_permits(building_permit_dict, geo_level, latest_insert_year, geoid_field, category_name, prod_env=ProductionEnvironment.MARKET_PROFILES):
    client = mongoclient.connect_to_client(prod_env=prod_env)
    dbname = 'MarketProfiles'
    db = client[dbname]
    collection = db['cbsahistoricalprofiles']

    collection_filter = {}

    existing_collections = collection.find(collection_filter, {'_id': False})
    existing_list = list(existing_collections)

    if len(existing_list) > 0:
        update_existing_market_trends(existing_list, building_permit_dict, latest_insert_year, geoid_field, category_name)
        # add_new_market_trends(existing_list, market_trends_dict, geoid_field)
    else:
        for k, results in building_permit_dict.items():
            existing_list.append(results)

    success = mongoclient.batch_inserts_with_list(existing_list, collection, collection_filter, geoid_field)

    if success:
        print("Successfully stored batch into Mongo. Rows inserted: ", len(existing_list))
        return success

def update_existing_market_trends(existing_list, building_permit_dict, latest_insert_year, geoid_field, category_name):
    '''
    Function will update existing data for given category.

    :param existing_list:
    :param market_trends_dict:
    :param download_year:
    :param geoid_field:
    :param category_name:
    :return:
    '''
    for existing_item in existing_list:
        geoid = existing_item[geoid_field]

        # If geoid does not exist in market trends, then check if the existing data has realestatetrends.
        # If so, delete realestatetrends because it will result in a time gap. For example, 2012-2013, then jumping to 2015.
        if geoid not in building_permit_dict.keys():
            # print('DID NOT FIND EXISTING GEOID IN MARKET TRENDS. GEOID: {}'.format(geoid))
            if category_name in existing_item.keys():
                print('!!! DELETING REALESTATETRENDS BECAUSE THERE IS A TIME GAP IN HISTORICAL DATA. GEOID: {}'.format(geoid))
                del existing_item[category_name]
            continue

        if category_name not in existing_item.keys():
            # If existing geo does not have realestatetrends, then add the key/data and continue
            existing_item[category_name] = building_permit_dict[geoid][category_name]
            continue
        else:
            existing_data = existing_item[category_name]

            for date in existing_data['dates']:
                if date in building_permit_dict[geoid][category_name]['dates']:
                    print('Skipping adding building permit because data exists.')
                    continue

            # building_permit_keys = list(building_permit_dict[geoid][category_name].keys())
            # building_permit_keys.remove('dates')
            for i, month in enumerate(building_permit_dict[geoid][category_name]['dates']):
                if month in existing_data['dates']:
                    print('Skipping adding building permit because data exists.')
                    continue
                else:
                    for key in building_permit_dict[geoid][category_name].keys():
                        existing_data[key].append(building_permit_dict[geoid][category_name][key][i])




        #
        # # check for keys (dates, unit_1, units_2, etc)
        # for key in copy_existing_data.keys():
        #     for i, month in enumerate(building_permit_dict[geoid][category_name][key]['dates']):
        #         if month not in existing_data[key]['dates']:
        #             append_value = building_permit_dict[geoid][category_name][key][existing_data_sub_key][i]
        #             existing_data[key][existing_data_sub_key].append(append_value)



            # # check for subkeys (dates, unit_1, etc.)
            # for existing_data_sub_key in existing_data[key]:
            #     if existing_data_sub_key in building_permit_dict[geoid][category_name][key]:
            #         continue
            #     else:
            #         if download_year == BUILDING_PERMIT_YEARS[-1]:
            #         # append existing data with new data from market_trends_dict
            #         for i, month in enumerate(building_permit_dict[geoid][category_name][key]['dates']):
            #             if month not in existing_data[key]['dates']:
            #                 append_value = building_permit_dict[geoid][category_name][key][existing_data_sub_key][i]
            #                 existing_data[key][existing_data_sub_key].append(append_value)
            #             elif existing_data_sub_key not in market_trends_dict[geoid][category_name][key]:
            #                 existing_data[key][existing_data_sub_key] = existing_data[key][existing_data_sub_key] + \
            #                                                             market_trends_dict[geoid][category_name][key][existing_data_sub_key]

