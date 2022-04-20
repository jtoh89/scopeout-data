import requests as r
import pandas as pd
from ast import literal_eval
import utils.utils
from database import mongoclient
from enums import GeoLevels, ProductionEnvironment, DefaultGeoIds
from census import censuslookups
from fredapi import Fred
from dotenv import load_dotenv
from lookups import OLD_TO_NEW_CBSAID
from census.censushelpers import calculate_category_percentage, check_percentages, sum_categories, sum_categories_and_total, sum_categories_with_owner_renter
import time
import os
import sys
from globals import SCOPEOUT_YEAR, CENSUS_LATEST_YEAR, CENSUS_YEARS
from datetime import datetime

# CENSUS_LATEST_YEAR = 2014
# CENSUS_YEARS = [2013, CENSUS_LATEST_YEAR]

STATES1 = [
    '01','02','04','05','06','08','09','10','11','12','13','15','16','17','18','19','20','21','22','23','24',
    '25','26','27','28'
]

STATES2 = [
    '29','30','31','32','33','34','35','36','37','38','39','40','41','42','44','45','46',
    '47','48','49','50','51','53','54','55','56'
]

#
STATES = ['01','02','04','05','06','08','09','10','11','12','13','15','16','17','18','19','20','21','22','23','24',
    '25','26','27','28','29','30','31','32','33','34','35','36','37','38','39','40','41','42','44','45','46',
    '47','48','49','50','51','53','54','55','56']

def update_us_median_income_fred():
    '''
    Function will get national unemployment data from Fred API and update median household income data for
    US data.
    :return: None
    '''
    load_dotenv()
    fred_api = os.getenv("FRED_API")
    fred = Fred(api_key=fred_api)
    data = fred.get_series('MEHOINUSA672N')
    s = pd.Series(data, name="MedianHouseholdIncome")
    df = s.to_frame()
    df['Date'] = df.index
    df = df.reset_index(drop=True)

    fred_income_array = []
    for i, row in df.iterrows():
        date_string = str(row['Date'])
        year = int(date_string[:4])

        if year < CENSUS_YEARS[0] or year > CENSUS_LATEST_YEAR:
            continue

        medianhouseholdincome = int(row['MedianHouseholdIncome'])
        fred_income_array.append(medianhouseholdincome)


    collection_filter = {
        'scopeoutyear': {'$eq': SCOPEOUT_YEAR},
        'geoid': DefaultGeoIds.USA.value,
    }

    us_med_income = mongoclient.query_collection(database_name="CensusData1",
                                                 collection_name="CensusData",
                                                 collection_filter=collection_filter,
                                                 prod_env=ProductionEnvironment.CENSUS_DATA1)

    us_med_income_data = us_med_income.data[0]['Median Household Income']

    for i, row in enumerate(us_med_income_data['All']):
        income_adjustment = utils.utils.calculate_percent_change(starting_data=row,
                                                                 ending_data=fred_income_array[i],
                                                                 move_decimal=False,
                                                                 decimal_places=7)
        adjustment = row * income_adjustment
        us_med_income_data['All'][i] = int(round(row + adjustment,0))

        us_med_income_data['Renters'][i] = int(round(us_med_income_data['Renters'][i] + adjustment, 0))
        us_med_income_data['Owners'][i] = int(round(us_med_income_data['Owners'][i] + adjustment, 0))


    final_data_dict = {}
    final_data_dict[DefaultGeoIds.USA.value] = {
        'data': {
            'Median Household Income': us_med_income_data
        }
    }


    success = mongoclient.store_census_data(geo_level=GeoLevels.USA,
                                            state_id=DefaultGeoIds.USA.value,
                                            filtered_dict=final_data_dict,
                                            prod_env=ProductionEnvironment.CENSUS_DATA1)

    if not success:
        print("!!! FATAL - failed to update US median household income!!!")

    print(us_med_income)


def run_census_data_import(geo_level, prod_env, force_run=False):
    '''
    Downloads census data variables. Function iterates through states, checks finished runs, and downloads
    data that are currently missing. Specify SCOPEOUT_YEAR when new year releases.
    :param geo_level:
    :param prod_env:
    :return: None
    '''
    lookups = censuslookups.get_census_lookup()
    all_categories = lookups['Category'].drop_duplicates()
    STATES_RUN = [STATES]
    if force_run:
        STATES_RUN = [force_run['stateid']]
        mongoclient.delete_finished_run({
            'state_id': force_run['stateid'],
            'geo_level': geo_level.value,
            'category': force_run['category']
            })


    collection_find_finished_runs = {
        'scopeout_year': SCOPEOUT_YEAR,
        'geo_level': geo_level.value,
    }
    finished_runs = mongoclient.get_finished_runs(collection_find_finished_runs)

    for i, state_array in enumerate(STATES_RUN):
        states_to_run = STATES
        if geo_level == GeoLevels.TRACT:
            if i == 0:
                states_to_run = STATES1
            else:
                states_to_run = STATES2
                prod_env = ProductionEnvironment.CENSUS_DATA2

        for i, stateid in enumerate(states_to_run):
            #usa and cbsa data does not need more than 1 iteration
            if geo_level in [GeoLevels.USA, GeoLevels.CBSA] and i > 0:
                break


            if geo_level == GeoLevels.USA:
                stateid = DefaultGeoIds.USA.value

            if geo_level == GeoLevels.CBSA:
                stateid = DefaultGeoIds.CBSA.value

            geographies_df = mongoclient.query_geography(geo_level=geo_level, stateid=stateid)

            print('Starting import for stateid: {}. geolevel: {}'.format(stateid, geo_level.value))

            finished_cats = []
            if len(finished_runs) > 0:
                finished_cats = finished_runs[finished_runs['state_id'] == stateid]['category'].values

            for i2, category in all_categories.items():
                if category in finished_cats:
                    print("Skipping category: " + category + ". State: ", stateid)
                    continue

                if geo_level != GeoLevels.CBSA and category == 'Total Households':
                    continue
                # Filter look ups for current category
                variables_df = lookups[lookups['Category'] == category]
                # variables_df = lookups[lookups['Category'] == 'Housing Unit Growth']

                print("Starting import for category: " + category + ". State: ", stateid)
                success = get_and_store_census_data(geo_level=geo_level,
                                                    state_id=stateid,
                                                    variables_df=variables_df,
                                                    geographies_df=geographies_df,
                                                    prod_env=prod_env)

                if not success:
                    print("*** END RUN - get_and_store_census_data Failed ***")
                    sys.exit()


                if prod_env == ProductionEnvironment.CENSUS_DATA1:
                    tablename = "censusdata1"
                    dbname = 'CensusData1'
                elif prod_env == ProductionEnvironment.CENSUS_DATA2:
                    tablename = "censusdata2"
                    dbname = 'CensusData2'
                else:
                    print("!!! FATAL - Census data stored in censusdata1 or censusdata2 only !!!")
                    sys.exit()

                collection_add_finished_run = {
                    'scopeout_year': SCOPEOUT_YEAR,
                    'state_id': stateid,
                    'geo_level': geo_level.value,
                    'dbname': dbname,
                    'tablename': tablename,
                    'category': category,
                }
                mongoclient.add_finished_run(collection_add_finished_run)


def get_and_store_census_data(geo_level, state_id, variables_df, geographies_df, prod_env):
    '''
    Stores CensusData object into Mongo. state_id is required to use census2 api. All geolevels for the state will be stored.
    If mongo inserts are successfull, function will return true. If not, it will return false.
    :param geo_level:
    :param state_id:
    :param variables_df:
    :return: bool
    '''

    category = variables_df['Category'].iloc[0]
    aggregate_type = variables_df['AggregateType'].iloc[0]
    historical = variables_df['Historical'].iloc[0]
    variable_list = list(variables_df['VariableID'])
    county_batches = False

    results_dict = {}
    geo_id = None
    missing_geo = []
    for year in CENSUS_YEARS:
        # if we don't need historical then skip until we reach CENSUS_END_YEAR
        if not historical and year != CENSUS_LATEST_YEAR:
            continue

        query_url = build_query(year, geo_level, state_id, variable_list)
        df = census_api(query_url)

        if geo_level == GeoLevels.CBSA:
            df = df.rename(columns={'metropolitan statistical area/micropolitan statistical area':'cbsa'})
            remap_cbsa_code = {
                '19430':'19380',
                '39150':'39140',
                '30100':'30060',
                '49060':'11680',
            }

            df['cbsa'] = df['cbsa'].replace(remap_cbsa_code)
            df['cbsa'] = df['cbsa'].replace(OLD_TO_NEW_CBSAID)

        census_geoname_lookup = df[['NAME', geo_level.value]]

        df = df.drop(columns=['NAME'])

        if len(df) == 0:
            if year == CENSUS_LATEST_YEAR:
                print("!!! DATA DOES NOT EXIST FOR CENSUS_END_YEAR")
                sys.exit()
            continue

        year_string = '{}'.format(year)

        for i, row in df.iterrows():
            if aggregate_type in ['Percentage']:
                category_sum_dict = sum_categories(variable_data_dict=row.to_dict(), variables_df=variables_df)
                aggregate_dict = calculate_category_percentage(category_sum_dict)
            elif aggregate_type in ['PercentageWithSubCategories']:
                category_sum_dict = sum_categories(variable_data_dict=row.to_dict(), variables_df=variables_df)
                aggregate_dict = calculate_category_percentage(category_sum_dict)
                check_percentages(aggregate_dict)
            elif aggregate_type == 'PercentageWithSubCategoriesWithOwnerRenter':
                aggregate_dict = sum_categories_with_owner_renter(variable_data_dict=row.to_dict(), variables_df=variables_df)
                if category != '% Income on Housing Costs':
                    check_percentages(aggregate_dict, True)
            elif aggregate_type == 'PercentageNoTotal':
                category_sum_dict = sum_categories_and_total(variable_data_dict=row.to_dict(), variables_df=variables_df)
                aggregate_dict = calculate_category_percentage(category_sum_dict)
                check_percentages(aggregate_dict)
            else:
                # path for aggregate_type None
                aggregate_dict = sum_categories(variable_data_dict=row.to_dict(), variables_df=variables_df)

            if geo_level == GeoLevels.STATE:
                geo_info = geographies_df.loc[geographies_df['fipsstatecode'] == state_id]
                geo_id = state_id
            elif geo_level == GeoLevels.CBSA:
                geo_id = row.cbsa
                geo_info = geographies_df.loc[geographies_df['cbsacode'] == geo_id]
            elif geo_level == GeoLevels.COUNTY:
                geo_id = row.state + row.county
                geo_info = geographies_df.loc[geographies_df['countyfullcode'] == geo_id]
            elif geo_level == GeoLevels.TRACT:
                geo_id = row.state + row.county + row.tract
                geo_info = geographies_df.loc[geographies_df['tractcode'] == geo_id]
            elif geo_level == GeoLevels.USA:
                geo_id = DefaultGeoIds.USA.value
                geo_info = geographies_df


            if len(geo_info) < 1:
                if geo_level == GeoLevels.COUNTY:
                    geo_id = row.county
                elif geo_level == GeoLevels.TRACT:
                    geo_id = row.tract

                missing_geo_name = census_geoname_lookup[census_geoname_lookup[geo_level.value] == geo_id]['NAME'].iloc[0]
                #Skip Puerto Rico metros
                if 'PR Metro' in missing_geo_name or 'PR Micro' in missing_geo_name:
                    continue

                missing_geo.append({'geo_id': geo_id,
                                    'geo_level': geo_level.value,
                                    'geo_name': missing_geo_name,
                                    'category': category,
                                    'year': year})
                continue

            data_dict = {}
            if historical:
                for k, v in aggregate_dict.items():
                    data_dict[k] = [v]

                data_dict['years'] = [year_string]
            else:
                data_dict[category] = aggregate_dict

            census_result_object = {}
            census_result_object['scopeoutyear'] = SCOPEOUT_YEAR
            census_result_object['geoinfo'] = geo_info.to_dict('records')[0]
            census_result_object['geoid'] = geo_id
            census_result_object['stateid'] = state_id
            census_result_object['geolevel'] = geo_level.value
            census_result_object['data'] = data_dict

            if geo_level == GeoLevels.TRACT:
                census_result_object['countyfullcode'] = geo_info.countyfullcode.iloc[0]
                county_batches = True

            if historical:
                census_result_object['data'] = {category: data_dict}

            if geo_id in results_dict.keys():
                if historical:
                    for k, v in results_dict[geo_id]['data'][category].items():
                        v.append(data_dict[k][0])
                else:
                    results_dict[geo_id]['data'] = census_result_object['data']
            else:
                if historical:
                    results_dict[geo_id] = census_result_object
                else:
                    results_dict[geo_id] = census_result_object


    mongoclient.store_missing_geo_for_census_data(missing_geo, geo_level, state_id, category)
    filtered_dict = filter_existing_data(results_dict, geo_level, category, prod_env, state_id)

    if len(filtered_dict) < 1:
        print("ENDING RUN, NO MORE GEOGRAPHIES TO RUN")
        return True

    return mongoclient.store_census_data(geo_level=geo_level,
                                         state_id=state_id,
                                         filtered_dict=filtered_dict,
                                         prod_env=prod_env,
                                         county_batches=county_batches)


def filter_existing_data(results_dict, geo_level, category, prod_env, state_id=None):
    mongo_info = mongoclient.get_mongo_info_from_environment(prod_env)

    collection_filter = {
        'scopeoutyear': {'$eq': SCOPEOUT_YEAR},
        'geolevel': {'$eq': geo_level.value},
        'stateid': {'$eq': state_id},
    }

    existing_data_df = mongoclient.query_collection(database_name=mongo_info['dbname'],
                                                    collection_name=mongo_info['collectionname'],
                                                    collection_filter=collection_filter,
                                                    prod_env=prod_env)
    existing_geoids = []

    for i, row in existing_data_df.iterrows():
        existing_cats = list(row.data.keys())
        if category in existing_cats:
            existing_geoids.append(row.geoid)



    filtered_geoids = []
    for k, results in results_dict.items():
        if k in existing_geoids:
            filtered_geoids.append(k)


    for existing_geoid in filtered_geoids:
        del results_dict[existing_geoid]

    return results_dict


# def add_market_trend():


def build_query(year, geo_level, state_id, variable_list):
    '''
    Builds query for census2 api based on geolevel, stateid.
    State will request state data for state_id.
    County will request all counties in state_id.
    Tract will request all tracts in all counties in state_id

    :param year:
    :param geo_level:
    :param state_id:
    :param variable_list:
    :return: string
    '''

    query_url = ''

    query_variables = ''
    for variable in variable_list:
        query_variables += variable + ','

    query_variables = query_variables[:-1]

    if geo_level == GeoLevels.STATE:
        query_url = 'https://api.census.gov/data/{}/acs/acs5?get=NAME,{}&for=state:{}' \
            .format(str(year), query_variables, state_id)
    elif geo_level == GeoLevels.CBSA:
        query_url = 'https://api.census.gov/data/{}/acs/acs5?get=NAME,{}&for=metropolitan%20statistical%20area/micropolitan%20statistical%20area:*'.format(str(year), query_variables)
    elif geo_level == GeoLevels.COUNTY:
        query_url = 'https://api.census.gov/data/{}/acs/acs5?get=NAME,{}&for=county:*&in=state:{}' \
            .format(str(year), query_variables, state_id)
    elif geo_level == GeoLevels.TRACT:
        query_url = 'https://api.census.gov/data/{}/acs/acs5?get=NAME,{}&for=tract:*&in=state:{}%20county:*' \
            .format(str(year), query_variables, state_id)
    elif geo_level == GeoLevels.USA:
        query_url = 'https://api.census.gov/data/{}/acs/acs5?get=NAME,{}&for=us:1' \
            .format(str(year), query_variables)

    query_url = query_url + '&key=32e910ae670ecaa2d3bb41d33bebf6e9131a500d'

    return query_url

def census_api(query_url):
    '''
    Runs api query provided. Returns dataframe.

    :param query_url:
    :return: dataframe
    '''

    print("Sending census api request: {}".format(query_url))

    retry_count = 0
    retry = True

    while retry:
        if retry_count > 4:
            return pd.DataFrame()

        try:
            data = r.get(query_url)
        except:
            print("Error making GET request. Trying again")
            print("Sleep 5 seconds before retrying census2 api calls")
            time.sleep(5)
            retry_count += 1
            continue

        if data.status_code != 200:
            retry_count += 1
            print("ERROR - bad request: {}".format(data.text))
            print("Sleep 5 seconds before retrying census2 api calls")
            time.sleep(5)
            continue


        row_list = []
        headers = []
        count = 0

        for line in data.text.splitlines():
            if line[:2] == '[[':
                line = line[1:-1]
            elif line[-2:] == ']]':
                line = line[:-1]
            else:
                line = line[:-1]

            if 'null' in line:
                line = line.replace('null', '0')

            line_list = literal_eval(line)
            if count == 0:
                headers = line_list
                count += 1
            else:
                if len(line_list) != len(headers):
                    raise ValueError('There is a mismatch in columns and values')
                else:
                    row_list.append(line_list)
                count += 1

        df = pd.DataFrame(row_list, columns=headers)
        df = df.reset_index(drop=True)

        return df




