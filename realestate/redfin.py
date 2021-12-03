import sys
import pandas as pd
import os
import csv
from database import mongoclient
from enums import GeoLevels
from enums import DefaultGeoIds
from enums import ProductionEnvironment
from lookups import MONTH_FORMAT, REDFIN_MSA_TO_CBSA, REDFIN_COUNTYID_TO_FIPS, REDFIN_USA_TO_FIPS, MONTH_TO_INDEX, INDEX_TO_MONTH
from database import mongoclient
from copy import deepcopy

REDFIN_MIN_YEAR = 2015
REDFIN_MAX_YEAR = 2021

def import_redfin_data(geo_level, default_geoid, geoid_field, geoname_field):
    currpath = os.path.dirname(os.path.abspath(__file__))
    rootpath = os.path.dirname(os.path.abspath(currpath))

    geographies_df = mongoclient.query_geography(geo_level=geo_level, stateid=default_geoid)
    geo_list = list(geographies_df[geoid_field])

    collection_find_finished_runs = {
        'category': 'realestatetrends',
        'geo_level': geo_level.value
    }

    finished_runs = mongoclient.get_finished_runs(collection_find_finished_runs)

    if len(finished_runs) > 0:
        last_finished_year = finished_runs.year.iloc[0]
    else:
        last_finished_year = 0

    redfin_dict = {}
    missing_geos_df = pd.DataFrame(columns=[geoid_field, geoname_field])

    file_dir = ''
    if geo_level == GeoLevels.USA:
        file_dir = '/files/us_national_market_tracker.tsv'
    elif geo_level == GeoLevels.CBSA:
        file_dir = '/files/redfin_metro_market_tracker.tsv'
    elif geo_level == GeoLevels.COUNTY:
        file_dir = '/files/county_market_tracker.tsv'

    with open(rootpath + file_dir) as file:
        category_name = 'realestatetrends'
        df = pd.read_csv(file, sep='\t')

        df = df[['period_begin', 'is_seasonally_adjusted', 'property_type', 'table_id', 'region', 'median_sale_price', 'median_ppsf', 'months_of_supply', 'median_dom', 'price_drops', 'state_code']]

        df[['median_sale_price', 'median_ppsf', 'months_of_supply', 'median_dom', 'price_drops']] = df[['median_sale_price', 'median_ppsf', 'months_of_supply', 'median_dom', 'price_drops']].fillna(0)

        df = df[df.is_seasonally_adjusted == 'f']
        df['date'] = pd.to_datetime(df['period_begin'])
        df = df.sort_values(by='date')
        df = df.reset_index()

        last_month = list(df['period_begin'].drop_duplicates())[-1:][0][5:7]

        if geo_level == GeoLevels.USA:
            df['table_id'] = df['table_id'].astype(str).replace(REDFIN_USA_TO_FIPS)
        if geo_level == GeoLevels.CBSA:
            df['table_id'] = df['table_id'].astype(str).replace(REDFIN_MSA_TO_CBSA)
        elif geo_level == GeoLevels.COUNTY:
            df['table_id'] = df['table_id'].astype(str).replace(REDFIN_COUNTYID_TO_FIPS)

        print('Create redfin data dictionaries')
        download_year = 0
        store_last_month = False
        for i, row in df.iterrows():
            geoid = row.table_id

            if i == (len(df) - 1):
                store_last_month = True

            if not store_last_month and geoid not in geo_list and geoid not in list(missing_geos_df[geoid_field]):
                print('No match for redfin geo. geoid: {}. geoname: {}'.format(geoid, row.region))
                missing_geos_df = missing_geos_df.append({geoid_field: geoid, geoname_field: row.region}, ignore_index=True)
                continue

            property_type = row.property_type

            if not store_last_month and property_type not in ['All Residential', 'Multi-Family (2-4 Unit)', 'Single Family Residential']:
                continue

            year_string = row.period_begin[:4]
            month_string = row.period_begin[5:7]
            date_string = MONTH_FORMAT[month_string] + ' ' + year_string

            current_year = int(year_string)

            if REDFIN_MIN_YEAR > current_year:
                continue
            elif last_finished_year >= current_year:
                continue

            if download_year == 0:
                download_year = current_year


            if download_year != current_year or store_last_month:
                check_full_year(redfin_dict, category_name, download_year, last_month)

                success = store_market_trends_redfin_data(redfin_dict,
                                                          category_name,
                                                          download_year,
                                                          geoid_field=geoid_field,
                                                          geo_level=geo_level)

                if download_year == REDFIN_MAX_YEAR and last_month != '12':
                    download_year = download_year - 1

                if success:
                    collection_add_finished_run = {
                        'category': category_name,
                        'geo_level': geo_level.value,
                        'year': download_year
                    }

                    mongoclient.update_finished_run(collection_add_finished_run, geo_level=geo_level, category=category_name)
                    download_year = current_year

                    redfin_dict = {}
                else:
                    print('Insert Market Trends failed')
                    sys.exit()

            median_sale_price = row.median_sale_price
            median_ppsf = int(round(row.median_ppsf,0))
            months_of_supply = round(row.months_of_supply,2)
            median_dom = row.median_dom
            price_drops = round(row.price_drops * 100, 1)

            if geoid in redfin_dict.keys():
                redfin_data = redfin_dict[geoid][category_name]

                if property_type in redfin_data.keys():
                    redfin_data[property_type]['dates'].append(date_string)
                    redfin_data[property_type]['median_sale_price'].append(median_sale_price)
                    redfin_data[property_type]['median_ppsf'].append(median_ppsf)
                    redfin_data[property_type]['months_of_supply'].append(months_of_supply)
                    redfin_data[property_type]['median_dom'].append(median_dom)
                    redfin_data[property_type]['price_drops'].append(price_drops)
                else:
                    redfin_data[property_type] = {
                        'dates': [date_string],
                        'median_sale_price': [median_sale_price],
                        'median_ppsf': [median_ppsf],
                        'months_of_supply': [months_of_supply],
                        'median_dom': [median_dom],
                        'price_drops': [price_drops]
                    }
            else:
                redfin_dict[geoid] = {
                    geoid_field: geoid,
                    'geolevel': geo_level.value,
                    category_name: {
                        property_type: {
                            'dates': [date_string],
                            'median_sale_price': [median_sale_price],
                            'median_ppsf': [median_ppsf],
                            'months_of_supply': [months_of_supply],
                            'median_dom': [median_dom],
                            'price_drops': [price_drops]
                        }
                    }
                }

def check_full_year(redfin_dict, category_name, download_year, last_month):
    last_month_string = MONTH_FORMAT[last_month]
    copy_redfin_dict = deepcopy(redfin_dict)

    for k, data in copy_redfin_dict.items():
        realestatetrenddata = data[category_name]

        for property_type in realestatetrenddata.keys():
            dates_length = len(realestatetrenddata[property_type]['dates'])

            # if there are less than 7 data points, just delete record. Too many dates to fill in.
            if dates_length < 7:
                redfin_dict[k][category_name].pop(property_type)
                continue

            if dates_length < 12:
                fill_missing_months(realestatetrenddata[property_type], download_year, last_month_string)


def fill_missing_months(realestatetrenddata_by_ptype, download_year, last_month_string):
    data_keys = list(realestatetrenddata_by_ptype.keys())
    data_keys.remove('dates')

    for i, date in enumerate(realestatetrenddata_by_ptype['dates']):
        month_short = date.split()[0]

        if download_year == REDFIN_MAX_YEAR and month_short == last_month_string:
            continue

        if INDEX_TO_MONTH[i] != month_short:
            realestatetrenddata_by_ptype['dates'].insert(i, INDEX_TO_MONTH[i] + ' ' + str(download_year))

            for data_key in data_keys:
                realestatetrenddata_by_ptype[data_key].insert(i, None)


def store_market_trends_redfin_data(market_trends_dict, category_name, download_year, geoid_field, geo_level, prod_env=ProductionEnvironment.MARKET_TRENDS):
    '''
    Function will will query existing data for given category_name and update the existing data with market_trends_dict.
    After update, it will store updated existing data into mongo.

    :param market_trends_dict:
    :param category_name:
    :param download_year:
    :param geoid_field:
    :param geo_level:
    :param prod_env:
    :return:
    '''
    print("Storing MarketTrends redfin data into Mongo")

    client = mongoclient.connect_to_client(prod_env=prod_env)
    dbname = 'MarketTrends'
    db = client[dbname]
    collection = db['markettrends']

    collection_filter = {'geolevel': geo_level.value}

    existing_collections = collection.find(collection_filter, {'_id': False})
    existing_list = list(existing_collections)

    if len(existing_list) > 0:
        update_existing_market_trends(existing_list, market_trends_dict, download_year, geoid_field, category_name)
        add_new_market_trends(existing_list, market_trends_dict, geoid_field)
    else:
        for k, results in market_trends_dict.items():
            existing_list.append(results)

    success = mongoclient.store_market_trends(existing_list, collection, collection_filter, geoid_field)

    if success:
        print("Successfully stored batch into Mongo. Rows inserted: ", len(existing_list))
        return success

def add_new_market_trends(existing_list, market_trends_dict, geoid_field):
    '''
    Function will find any missing data in market_trends_dict and insert into existing data set.

    :param existing_list:
    :param market_trends_dict:
    :param geoid_field:
    :return:
    '''
    existing_geoids = []
    for existing_data in existing_list:
        existing_geoids.append(existing_data[geoid_field])

    for k, new_data in market_trends_dict.items():
        if k not in existing_geoids:
            existing_list.append(new_data)


def update_existing_market_trends(existing_list, market_trends_dict, download_year, geoid_field, category_name):
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
        if geoid not in market_trends_dict.keys():
            print('DID NOT FIND EXISTING GEOID IN MARKET TRENDS. GEOID: {}'.format(geoid))
            if category_name in existing_item.keys():
                print('!!! DELETING REALESTATETRENDS BECAUSE THERE IS A TIME GAP IN HISTORICAL DATA. GEOID: {}'.format(geoid))
                del existing_item[category_name]
            continue

        if category_name not in existing_item.keys():
            # If existing geo does not have realestatetrends, then add the key/data and continue
            existing_item[category_name] = market_trends_dict[geoid][category_name]
            continue
        else:
            existing_data = existing_item[category_name]

        copy_existing_data = deepcopy(existing_data)
        # check for keys (all residential, single family residential, etc)
        for key in copy_existing_data.keys():
            if key not in market_trends_dict[geoid][category_name].keys():
                # if key is not found in market trend, then there will be a time gap because newer dates do not have data.
                # to prevent time gap, reset the data by deleting the key
                print('!!! DELETING PROPERTYTYPE: {} BECAUSE AN ENTIRE YEAR IS MISSING. GEOID: {}'.format(key,geoid))
                del existing_data[key]
                continue

            # check for subkeys (dates, median_sale_price, etc)
            for existing_data_sub_key in list(existing_data[key].keys()):
                if existing_data_sub_key not in market_trends_dict[geoid][category_name][key].keys():
                    if existing_data_sub_key == 'dates':
                        print('!!! ERROR - why are dates missing in markettrendss')
                        sys.exit()

                    print('!!! DELETING SUBKEY: {} BECAUSE THERE WILL BE A TIME GAP IN HISTORICAL DATA. GEOID: {}'.format(existing_data_sub_key,geoid))
                    del existing_data[key][existing_data_sub_key]
                else:
                    # append existing data with new data from market_trends_dict
                    if download_year == REDFIN_MAX_YEAR:
                        for i, month in enumerate(market_trends_dict[geoid][category_name][key]['dates']):
                            if month not in existing_data[key]['dates']:
                                append_value = market_trends_dict[geoid][category_name][key][existing_data_sub_key][i]
                                existing_data[key][existing_data_sub_key].append(append_value)
                    else:
                        existing_data[key][existing_data_sub_key] = existing_data[key][existing_data_sub_key] + \
                                                                    market_trends_dict[geoid][category_name][key][existing_data_sub_key]




def import_redfin_data_old(geo_level, default_geoid, geoid_field, geoname_field, batches=False):
    currpath = os.path.dirname(os.path.abspath(__file__))
    rootpath = os.path.dirname(os.path.abspath(currpath))

    geographies_df = mongoclient.query_geography(geo_level=geo_level, stateid=default_geoid)

    collection_find_finished_runs = {
        'category': 'realestatetrends'
    }
    finished_runs = mongoclient.get_finished_runs(collection_find_finished_runs)
    if len(finished_runs) > 0:
        last_finished_year = finished_runs.year.iloc[0]
    else:
        last_finished_year = 0
    if len(geographies_df) < 1:
        geo_list = []
    else:
        geo_list = list(geographies_df[geoid_field])

    redfin_dict = {}
    missing_geos_df = pd.DataFrame(columns=[geoid_field, geoname_field])

    file_dir = ''
    if geo_level == GeoLevels.CBSA:
        file_dir = '/files/redfin_metro_market_tracker.tsv'
    elif geo_level == GeoLevels.COUNTY:
        file_dir = '/files/county_market_tracker.tsv'
    elif geo_level == GeoLevels.ZIPCODE:
        file_dir = '/files/zip_code_market_tracker.tsv'

    with open(rootpath + file_dir) as file:
        category_name = 'realestatetrends'
        df = pd.read_csv(file, sep='\t')

        df = df[['period_begin', 'is_seasonally_adjusted', 'property_type', 'table_id', 'region', 'median_sale_price', 'median_ppsf', 'months_of_supply', 'median_dom', 'price_drops', 'state_code']]

        df[['median_sale_price', 'median_ppsf', 'months_of_supply', 'median_dom', 'price_drops']] = df[['median_sale_price', 'median_ppsf', 'months_of_supply', 'median_dom', 'price_drops']].fillna(0)

        df = df[df.is_seasonally_adjusted == 'f']
        df['table_id'] = df['table_id'].astype(str).replace(REDFIN_MSA_TO_CBSA)
        df['date'] = pd.to_datetime(df['period_begin'])
        df = df.sort_values(by='date')
        df = df.reset_index()

        print('Create redfin data dictionaries')
        download_year = 0
        for i, row in df.iterrows():
            geoid = row.table_id

            if geo_level == GeoLevels.ZIPCODE:
                geoid = row.region
                geoid = geoid.replace('Zip Code: ', '')

            #
            # if len(geo_list) > 0 and geoid not in geo_list and geoid not in list(missing_geos_df[geoid_field]):
            #     print('No match for redfin geo. geoid: {}. geoname: {}'.format(geoid, row.region))
            #     missing_geos_df = missing_geos_df.append({geoid_field: geoid, geoname_field: row.region}, ignore_index=True)
            #     continue

            property_type = row.property_type

            if property_type not in ['All Residential', 'Multi-Family (2-4 Unit)', 'Single Family Residential']:
                continue

            year_string = row.period_begin[:4]
            month_string = row.period_begin[5:7]
            date_string = MONTH_FORMAT[month_string] + ' ' + year_string

            current_year = int(year_string)

            if last_finished_year >= current_year:
                continue

            if download_year == 0:
                download_year = current_year

            if download_year != current_year:
                success = mongoclient.store_market_trends_data(redfin_dict,
                                                               category_name,
                                                               geoid_field=geoid_field,
                                                               geo_level=geo_level,
                                                               year=0,
                                                               overwrite=False,
                                                               batches=batches)

                if success:
                    collection_add_finished_run = {
                        'category': category_name,
                        'geo_level': geo_level.value,
                        'year': download_year
                    }

                    mongoclient.update_finished_run(collection_add_finished_run, category=category_name)
                    download_year = current_year
                else:
                    print('Insert Market Trends failed')
                    sys.exit()
                # mongoclient.add_finished_run(collection_add_finished_run)


            median_sale_price = row.median_sale_price
            median_ppsf = int(round(row.median_ppsf,0))
            months_of_supply = row.months_of_supply
            median_dom = row.median_dom
            price_drops = row.price_drops

            if geoid in redfin_dict.keys():
                redfin_data = redfin_dict[geoid][category_name]

                if property_type in redfin_data.keys():
                    redfin_data[property_type]['dates'].append(date_string)
                    redfin_data[property_type]['median_sale_price'].append(median_sale_price)
                    redfin_data[property_type]['median_ppsf'].append(median_ppsf)
                    redfin_data[property_type]['months_of_supply'].append(months_of_supply)
                    redfin_data[property_type]['median_dom'].append(median_dom)
                    redfin_data[property_type]['price_drops'].append(price_drops)
                else:
                    redfin_data[property_type] = {
                        'dates': [date_string],
                        'median_sale_price': [median_sale_price],
                        'median_ppsf': [median_ppsf],
                        'months_of_supply': [months_of_supply],
                        'median_dom': [median_dom],
                        'price_drops': [price_drops]
                    }
            else:
                redfin_dict[geoid] = {
                    geoid_field: geoid,
                    'geolevel': geo_level.value,
                    'year': download_year,
                    category_name: {
                        property_type: {
                            'dates': [date_string],
                            'median_sale_price': [median_sale_price],
                            'median_ppsf': [median_ppsf],
                            'months_of_supply': [months_of_supply],
                            'median_dom': [median_dom],
                            'price_drops': [price_drops]
                        }
                    }
                }

            # if len(missing_geos_df) > 0:
            missing_geos_df.to_csv('missing_redfin.csv')





        #
        # if success and latest_insert_month == '12':
        # else:
        #     continue


