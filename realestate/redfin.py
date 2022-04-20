import sys
import pandas as pd
import os
import csv
import math
from database import mongoclient
from lookups import MONTH_FORMAT, INDEX_TO_MONTH, REDFIN_PROPERTY_TYPES_LOWERCASE_CONVERSION, REDFIN_MSA_TO_CBSA, REDFIN_COUNTYID_TO_FIPS, REDFIN_USA_TO_FIPS, MONTH_TO_INDEX, INDEX_TO_MONTH
from database import mongoclient
from copy import deepcopy
from realestate import initialize
from enums import GeoLevels, DefaultGeoIds, ProductionEnvironment, GeoIdField, GeoNameField
import datetime
from utils.utils import calculate_percentiles_from_list, string_to_float, assign_legend_details, assign_color
from models import zipcodemarketmap
from utils.utils import calculate_percentiles_from_list, string_to_float, string_to_int, nat_to_none
import numpy as np

REDFIN_MIN_YEAR = 2015
REDFIN_MAX_YEAR = 2022
REDFIN_PROPERTY_TYPES = ['All Residential', 'Single Family Residential', 'Multi-Family (2-4 Unit)']
REDFIN_PROPERTY_TYPES_LOWERCASE = ['all', 'singlefamily', 'multifamily']
REDFIN_DATA_CATEGORIES = ['median_sale_price', 'median_ppsf', 'months_of_supply', 'median_dom', 'price_drops']


def import_redfin_historical_data(geo_level, default_geoid, geoid_field, geoname_field, collection_name):
    initialize_historical_profiles(geo_level, collection_name)
    currpath = os.path.dirname(os.path.abspath(__file__))
    rootpath = os.path.dirname(os.path.abspath(currpath))

    geographies_df = mongoclient.query_geography(geo_level=geo_level, stateid=default_geoid)
    geo_list = list(geographies_df[geoid_field])

    file_dir = ''
    if geo_level == GeoLevels.USA:
        file_dir = '/files/us_national_market_tracker.tsv'
    elif geo_level == GeoLevels.CBSA:
        file_dir = '/files/redfin_metro_market_tracker.tsv'
    elif geo_level == GeoLevels.COUNTY:
        file_dir = '/files/county_market_tracker.tsv'
    elif geo_level == GeoLevels.ZIPCODE:
        file_dir = '/files/zip_code_market_tracker.tsv'

    geo_data_dict = {}

    with open(rootpath + file_dir, 'r') as csvfile:
        datareader = csv.reader(csvfile, delimiter='\t')
        headers = []

        for i, row in enumerate(datareader):
            if i < 1:
                headers = row
                continue

            if i % 100000 == 0:
                print("Reading {} total lines from redfin zip file".format(i+1))

            ### create dict
            row_dict = dict(zip(headers, row))

            geoid = row_dict['table_id']

            if row_dict['is_seasonally_adjusted'] != 'f':
                continue

            if row_dict['property_type'] != 'All Residential':
                continue

            if geo_level == GeoLevels.USA:
                geoid = REDFIN_USA_TO_FIPS[geoid]
            elif geo_level == GeoLevels.CBSA:
                if geoid in REDFIN_MSA_TO_CBSA.keys():
                    geoid = REDFIN_MSA_TO_CBSA[geoid]
            elif geo_level == GeoLevels.COUNTY:
                if geoid in REDFIN_COUNTYID_TO_FIPS.keys():
                    geoid = REDFIN_COUNTYID_TO_FIPS[geoid]
            elif geo_level == GeoLevels.ZIPCODE:
                geoid = row_dict['region'].replace('Zip Code: ','')
                geoid = geoid.zfill(5)


            if geoid not in geo_list and geo_level != GeoLevels.ZIPCODE:
                continue

            year, month, day = row_dict['period_end'].split('-')

            if int(year) < REDFIN_MIN_YEAR:
                continue

            try:
                median_sale_price = string_to_int(row_dict['median_sale_price'])
                median_sale_price_mom = string_to_float(row_dict['median_sale_price_mom'], 2)
                median_sale_price_yoy = string_to_float(row_dict['median_sale_price_yoy'], 2)
                median_dom = string_to_int(row_dict['median_dom'])
                median_dom_mom = string_to_float(row_dict['median_dom_mom'], 2)
                median_dom_yoy = string_to_float(row_dict['median_dom_yoy'], 2)
                median_ppsf = string_to_int(row_dict['median_ppsf'])
                median_ppsf_mom = string_to_float(row_dict['median_ppsf_mom'], 2)
                median_ppsf_yoy = string_to_float(row_dict['median_ppsf_yoy'], 2)
                months_of_supply = string_to_float(row_dict['months_of_supply'], 2)
                price_drops = string_to_float(row_dict['price_drops'], 2)
            except Exception as e:
                print(e)
                sys.exit()

            if geoid not in geo_data_dict.keys():
                geo_data_dict[geoid] = {
                    'realestatetrends': {
                        'dates': [datetime.datetime(int(year), int(month), int(day))],
                        'mediansaleprice': [median_sale_price],
                        'mediansalepricemom': [median_sale_price_mom],
                        'mediansalepriceyoy': [median_sale_price_yoy],
                        'mediandom': [median_dom],
                        'mediandommom': [median_dom_mom],
                        'mediandomyoy': [median_dom_yoy],
                        'medianppsf': [median_ppsf],
                        'medianppsfmom': [median_ppsf_mom],
                        'medianppsfyoy': [median_ppsf_yoy],
                        'monthsofsupply': [months_of_supply],
                        'pricedrops': [price_drops]
                    }
                }
            else:
                existing_geo_data = geo_data_dict[geoid]['realestatetrends']
                existing_geo_data['dates'].append(datetime.datetime(int(year), int(month), int(day)))
                existing_geo_data['mediansaleprice'].append(median_sale_price)
                existing_geo_data['mediansalepricemom'].append(median_sale_price_mom)
                existing_geo_data['mediansalepriceyoy'].append(median_sale_price_yoy)
                existing_geo_data['mediandom'].append(median_dom)
                existing_geo_data['mediandommom'].append(median_dom_mom)
                existing_geo_data['mediandomyoy'].append(median_dom_yoy)
                existing_geo_data['medianppsf'].append(median_ppsf)
                existing_geo_data['medianppsfmom'].append(median_ppsf_mom)
                existing_geo_data['medianppsfyoy'].append(median_ppsf_yoy)
                existing_geo_data['monthsofsupply'].append(months_of_supply)
                existing_geo_data['pricedrops'].append(price_drops)



    insert_list = []

    for k, zip_data in geo_data_dict.items():
        temp_df = pd.DataFrame.from_dict(zip_data['realestatetrends']).sort_values(by='dates')
        temp_df = temp_df.replace({np.nan: None})

        # check if at least 12 months exists
        if len(temp_df) < 12:
            continue

        if temp_df.iloc[len(temp_df)-1].dates.year != REDFIN_MAX_YEAR:
            # print("skipping zipcode: {}. Does not have any data for current year: {}".format(k, REDFIN_MAX_YEAR))
            continue

        temp_dict = {
            '{}'.format(geoid_field): k,
            'realestatetrends': {
                'dates': [],
                'mediansaleprice': [],
                'mediansalepricemom': [],
                'mediansalepriceyoy': [],
                'mediandom': [],
                'mediandommom': [],
                'mediandomyoy': [],
                'medianppsf': [],
                'medianppsfmom': [],
                'medianppsfyoy': [],
                'monthsofsupply': [],
                'pricedrops': []
            }
        }

        prev_month = 1
        prev_year = REDFIN_MIN_YEAR
        for i, row in temp_df.iterrows():
            if row['dates'].year > prev_year:
                prev_year = row['dates'].year
                prev_month = 1

            if row['dates'].month != 1 and row['dates'].month - 1 != prev_month:
                fill_missing_dates(row, temp_dict, prev_month)
                prev_month = row['dates'].month
            else:
                prev_month = row['dates'].month
            try:
                temp_dict['realestatetrends']['dates'].append(INDEX_TO_MONTH[row['dates'].month-1] + ' ' + str(row['dates'].year))
            except Exception as e:
                print(e)
                sys.exit()
            try:
                temp_dict['realestatetrends']['mediansaleprice'].append(nat_to_none(row.mediansaleprice))
                temp_dict['realestatetrends']['mediansalepricemom'].append(nat_to_none(row.mediansalepricemom))
                temp_dict['realestatetrends']['mediansalepriceyoy'].append(nat_to_none(row.mediansalepriceyoy))
                temp_dict['realestatetrends']['mediandom'].append(nat_to_none(row.mediandom))
                temp_dict['realestatetrends']['mediandommom'].append(nat_to_none(row.mediandommom))
                temp_dict['realestatetrends']['mediandomyoy'].append(nat_to_none(row.mediandomyoy))
                temp_dict['realestatetrends']['medianppsf'].append(nat_to_none(row.medianppsf))
                temp_dict['realestatetrends']['medianppsfmom'].append(nat_to_none(row.medianppsfmom))
                temp_dict['realestatetrends']['medianppsfyoy'].append(nat_to_none(row.medianppsfyoy))
                temp_dict['realestatetrends']['monthsofsupply'].append(nat_to_none(row.monthsofsupply))
                temp_dict['realestatetrends']['pricedrops'].append(nat_to_none(row.pricedrops))
            except Exception as e:
                print(e)
                sys.exit()
        insert_list.append(temp_dict)


    new_insert_list = update_existing_historical_profile(insert_list, geoid_field, collection_name, geo_level)

    client = mongoclient.connect_to_client(prod_env=ProductionEnvironment.MARKET_PROFILES)
    dbname = 'MarketProfiles'
    db = client[dbname]
    collection = db[collection_name]

    collection_filter = {}

    success = mongoclient.batch_inserts_with_list(new_insert_list, collection, collection_filter, geoid_field)

    if not success:
        print("!!! geo historical batch insert failed !!!", len(new_insert_list))
        return success

def fill_missing_dates(row, temp_dict, prev_month):
    num_months_between = row['dates'].month - prev_month - 1
    add_month = prev_month + 1

    # reset temp_dict if a huge gap
    if num_months_between > 3:
        temp_dict['realestatetrends'] = {
            'dates': [],
            'mediansaleprice': [],
            'mediansalepricemom': [],
            'mediansalepriceyoy': [],
            'mediandom': [],
            'mediandommom': [],
            'mediandomyoy': [],
            'medianppsf': [],
            'medianppsfmom': [],
            'medianppsfyoy': [],
            'monthsofsupply': [],
            'pricedrops': []
        }
    else:
        for month_index in range(num_months_between):
            temp_dict['realestatetrends']['dates'].append(INDEX_TO_MONTH[add_month-1] + ' ' + str(row['dates'].year))
            temp_dict['realestatetrends']['mediansaleprice'].append(None)
            temp_dict['realestatetrends']['mediansalepricemom'].append(None)
            temp_dict['realestatetrends']['mediansalepriceyoy'].append(None)
            temp_dict['realestatetrends']['mediandom'].append(None)
            temp_dict['realestatetrends']['mediandommom'].append(None)
            temp_dict['realestatetrends']['mediandomyoy'].append(None)
            temp_dict['realestatetrends']['medianppsf'].append(None)
            temp_dict['realestatetrends']['medianppsfmom'].append(None)
            temp_dict['realestatetrends']['medianppsfyoy'].append(None)
            temp_dict['realestatetrends']['monthsofsupply'].append(None)
            temp_dict['realestatetrends']['pricedrops'].append(None)
            add_month += 1


def update_existing_historical_profile(insert_list, geoid_field, collection_name, geo_level):
    existing_historical_profiles = mongoclient.query_collection(database_name="MarketProfiles",
                                                               collection_name=collection_name,
                                                               collection_filter={},
                                                               prod_env=ProductionEnvironment.MARKET_PROFILES).drop(columns=["_id"])
    new_insert_list = []
    for updated_profile in insert_list:
        existing_profile = existing_historical_profiles[existing_historical_profiles[geoid_field] == updated_profile[geoid_field]]

        if len(existing_profile) == 0:
            if geo_level == GeoLevels.ZIPCODE:
                new_insert_list.append({
                    'zipcode': updated_profile[geoid_field],
                    'geolevel': geo_level.value,
                    'geoname': updated_profile[geoid_field],
                    'realestatetrends': updated_profile['realestatetrends']
                })
            continue

        existing_profile = existing_profile.iloc[0].to_dict()
        existing_profile['realestatetrends'] = updated_profile['realestatetrends']

        if 'rentaltrends' in existing_profile.keys() and existing_profile['rentaltrends'] != existing_profile['rentaltrends']:
            del existing_profile['rentaltrends']

        new_insert_list.append(existing_profile)

    return new_insert_list






########################
## V1 below
########################



def DEPRECATED_import_historical_redfin_data(geo_level, default_geoid, geoid_field, geoname_field):
    initialize_historical_profiles(geo_level)
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
        last_full_finished_year = finished_runs.last_full_download_year.iloc[0]
    else:
        last_full_finished_year = 0

    redfin_dict = {}

    file_dir = ''
    if geo_level == GeoLevels.USA:
        file_dir = '/files/us_national_market_tracker.tsv'
    elif geo_level == GeoLevels.CBSA:
        file_dir = '/files/redfin_metro_market_tracker.tsv'
    elif geo_level == GeoLevels.COUNTY:
        file_dir = '/files/county_market_tracker.tsv'
    elif geo_level == GeoLevels.ZIPCODE:
        file_dir = '/files/zip_code_market_tracker.tsv'
        print("!!! This function does not importer historicals for ZIPCODE!!!")
        sys.exit()

    with open(rootpath + file_dir) as file:
        category_name = 'realestatetrends'
        df = pd.read_csv(file, sep='\t')

        df = df[['period_begin', 'is_seasonally_adjusted', 'property_type', 'table_id', 'region', 'median_sale_price', 'median_ppsf', 'months_of_supply', 'median_dom', 'price_drops', 'state_code']]

        df[['median_sale_price', 'median_ppsf', 'months_of_supply', 'median_dom', 'price_drops']] = df[['median_sale_price', 'median_ppsf', 'months_of_supply', 'median_dom', 'price_drops']].fillna(0)

        df = df[df.is_seasonally_adjusted == 'f']
        df['dates'] = pd.to_datetime(df['period_begin'])
        df = df.sort_values(by='dates')
        df = df.reset_index()

        last_month_in_dataset = list(df['period_begin'].drop_duplicates())[-1:][0][5:7]

        if geo_level == GeoLevels.USA:
            df['table_id'] = df['table_id'].astype(str).replace(REDFIN_USA_TO_FIPS)
        if geo_level == GeoLevels.CBSA:
            df['table_id'] = df['table_id'].astype(str).replace(REDFIN_MSA_TO_CBSA)
        elif geo_level == GeoLevels.COUNTY:
            df['table_id'] = df['table_id'].astype(str).replace(REDFIN_COUNTYID_TO_FIPS)
        # elif geo_level == GeoLevels.ZIPCODE:
        #     df['table_id'] = df['table_id'].astype(str).replace(REDFIN_COUNTYID_TO_FIPS)

        print('Create redfin data dictionaries')
        download_year = 0
        last_row = False
        insert_last_row = True
        for i, row in df.iterrows():
            geoid = row.table_id

            if i == (len(df) - 1):
                last_row = True

            if geoid not in geo_list:
                if last_row:
                    insert_last_row = False
                else:
                    continue

            property_type = row.property_type

            if property_type not in ['All Residential', 'Multi-Family (2-4 Unit)', 'Single Family Residential']:
                if last_row:
                    insert_last_row = False
                else:
                    continue

            property_type = REDFIN_PROPERTY_TYPES_LOWERCASE_CONVERSION[property_type]

            year_string = row.period_begin[:4]
            month_string = row.period_begin[5:7]
            date_string = MONTH_FORMAT[month_string] + ' ' + year_string

            current_year = int(year_string)

            if not last_row and REDFIN_MIN_YEAR > current_year:
                continue
            elif not last_row and last_full_finished_year >= current_year:
                continue

            if download_year == 0:
                download_year = current_year

            if download_year != current_year or last_row:
                if last_row and insert_last_row:
                    append_last_row_to_redfin_dict(row, redfin_dict, geoid, category_name, property_type, date_string)

                check_full_year(redfin_dict, category_name, download_year, last_month_in_dataset, geoid_field)

                success = store_market_trends_redfin_data(redfin_dict,
                                                          category_name,
                                                          download_year,
                                                          geoid_field=geoid_field,
                                                          geo_level=geo_level)

                if download_year == REDFIN_MAX_YEAR and last_month_in_dataset != '12':
                    download_year = download_year - 1

                if success:
                    collection_add_finished_run = {
                        'category': category_name,
                        'geo_level': geo_level.value,
                        'last_full_download_year': download_year
                    }

                    mongoclient.update_finished_run(collection_add_finished_run, geo_level=geo_level, category=category_name)
                    download_year = current_year

                    redfin_dict = {}
                else:
                    print('Insert Market Trends failed')
                    sys.exit()

            if last_row:
                continue

            median_sale_price = int(round(row.median_sale_price,0))
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
                geoname = geographies_df[geographies_df[geoid_field] == geoid]

                if len(geoname) < 1:
                    print('No geoname found for: {}'.format(geoid))
                    continue
                else:
                    geoname = geoname.iloc[0][geoname_field]

                redfin_dict[geoid] = {
                    geoid_field: geoid,
                    'geolevel': geo_level.value,
                    'geoname': geoname,
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


def append_last_row_to_redfin_dict(row, redfin_dict, geoid, category_name, property_type, date_string):
    median_sale_price = int(round(row.median_sale_price,0))
    median_ppsf = int(round(row.median_ppsf,0))
    months_of_supply = round(row.months_of_supply,2)
    median_dom = row.median_dom
    price_drops = round(row.price_drops * 100, 1)

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

def check_full_year(redfin_dict, category_name, download_year, last_month_in_dataset, geoid_field):
    last_month_in_dataset_string = MONTH_FORMAT[last_month_in_dataset]
    copy_redfin_dict = deepcopy(redfin_dict)

    for k, data in copy_redfin_dict.items():
        realestatetrenddata = data[category_name]

        for property_type in REDFIN_PROPERTY_TYPES_LOWERCASE:
            if property_type not in realestatetrenddata.keys():
                continue

            dates_length = len(realestatetrenddata[property_type]['dates'])

            # # if there are less than 7 data points, just delete record. Too many dates to fill in.
            if download_year != REDFIN_MAX_YEAR and dates_length < 7:
                redfin_dict[k][category_name].pop(property_type)
                continue

            if dates_length < 12:
                print('Filling missing months for geoid: {}'.format(data[geoid_field]))
                fill_missing_months(redfin_dict[k][category_name][property_type], download_year, last_month_in_dataset_string)

def fill_missing_months(realestatetrenddata_by_ptype, download_year, last_month_in_dataset_string):
    data_keys = list(realestatetrenddata_by_ptype.keys())
    data_keys.remove('dates')

    break_loop = False
    for i, date in enumerate(realestatetrenddata_by_ptype['dates']):
        month_short = date.split()[0]

        if break_loop:
            continue

        if download_year == REDFIN_MAX_YEAR and month_short == last_month_in_dataset_string:
            break_loop = True

        if INDEX_TO_MONTH[i] != month_short:
            realestatetrenddata_by_ptype['dates'].insert(i, INDEX_TO_MONTH[i] + ' ' + str(download_year))

            for redfin_cat in REDFIN_DATA_CATEGORIES:
                realestatetrenddata_by_ptype[redfin_cat].insert(i, None)

    # If data series is missing last few months, append
    # if len(realestatetrenddata_by_ptype['dates']) < 12 and download_year != REDFIN_MAX_YEAR:
    if len(realestatetrenddata_by_ptype['dates']) < 12:
        last_month = realestatetrenddata_by_ptype['dates'][-1].split()[0]
        last_month_index = MONTH_TO_INDEX[last_month]

        for k, month_string in INDEX_TO_MONTH.items():
            if k < last_month_index:
                continue
            elif download_year == REDFIN_MAX_YEAR and k > MONTH_TO_INDEX[last_month_in_dataset_string] - 1:
                continue
            else:
                realestatetrenddata_by_ptype['dates'].append(month_string + ' ' + str(download_year))
                for redfin_cat in REDFIN_DATA_CATEGORIES:
                    realestatetrenddata_by_ptype[redfin_cat].append(None)

def store_market_trends_redfin_data(market_trends_dict, category_name, download_year, geoid_field, geo_level, prod_env=ProductionEnvironment.MARKET_PROFILES):
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
    dbname = 'MarketProfiles'
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

    success = mongoclient.batch_inserts_with_list(existing_list, collection, collection_filter, geoid_field)

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

        if geoid == "38060":
            print("")

        # If geoid does not exist in market trends, then check if the existing data has realestatetrends.
        # If so, delete realestatetrends because it will result in a time gap. For example, 2012-2013, then jumping to 2015.
        if geoid not in market_trends_dict.keys():
            if category_name in existing_item.keys():
                print('!!! Deleting {} to update historical. GEOID: {}'.format(category_name, geoid))
                del existing_item[category_name]
            continue

        if category_name not in existing_item.keys():
            # If existing geo does not have realestatetrends, then add the key/data and continue
            if len(market_trends_dict[geoid][category_name]) > 0:
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

                if len(existing_data.keys()) == 0:
                    del existing_item['realestatetrends']
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
                    # if download_year == REDFIN_MAX_YEAR:
                    #     for i, month in enumerate(market_trends_dict[geoid][category_name][key]['dates']):
                    #         if month not in existing_data[key]['dates']:
                    #             append_value = market_trends_dict[geoid][category_name][key][existing_data_sub_key][i]
                    #             existing_data[key][existing_data_sub_key].append(append_value)
                    if download_year != REDFIN_MAX_YEAR:
                        existing_data[key][existing_data_sub_key] = existing_data[key][existing_data_sub_key] + \
                                                                    market_trends_dict[geoid][category_name][key][existing_data_sub_key]

            if download_year == REDFIN_MAX_YEAR:
                for i, month in enumerate(market_trends_dict[geoid][category_name][key]['dates']):
                    if month not in existing_data[key]['dates']:
                        existing_data[key]['dates'].append(month)

                        for cat in REDFIN_DATA_CATEGORIES:
                            existing_data[key][cat].append(market_trends_dict[geoid][category_name][key][cat][i])

def initialize_historical_profiles(geo_level, collection_name):
    if geo_level == GeoLevels.USA:
        initialize.initialize_market_trends(geo_level=GeoLevels.USA,
                                            default_geoid=DefaultGeoIds.USA,
                                            geoid_field=GeoIdField.USA.value,
                                            geoname_field=GeoNameField.USA.value,
                                            collection_name=collection_name
                                            )
    if geo_level == GeoLevels.CBSA:
        initialize.initialize_market_trends(geo_level=GeoLevels.CBSA,
                                            default_geoid=DefaultGeoIds.CBSA,
                                            geoid_field=GeoIdField.CBSA.value,
                                            geoname_field=GeoNameField.CBSA.value,
                                            collection_name=collection_name
                                            )
    elif geo_level == GeoLevels.COUNTY:
        initialize.initialize_market_trends(geo_level=GeoLevels.COUNTY,
                                            default_geoid=DefaultGeoIds.COUNTY.value,
                                            geoid_field=GeoIdField.COUNTY.value,
                                            geoname_field=GeoNameField.COUNTY.value,
                                            collection_name=collection_name
                                            )
    elif geo_level == GeoLevels.ZIPCODE:
        initialize.initialize_market_trends(geo_level=GeoLevels.ZIPCODE,
                                            default_geoid=DefaultGeoIds.ZIPCODE.value,
                                            geoid_field=GeoIdField.ZIPCODE.value,
                                            geoname_field=GeoNameField.ZIPCODE.value,
                                            collection_name=collection_name
                                            )




