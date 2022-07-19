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
from enums import GeoLevels, DefaultGeoIds, ProductionEnvironment, GeoIdField, GeoNameField, Database, Collections
import datetime
from utils.utils import string_to_int, nat_to_none, string_to_float
from models import zipcodemarketmap
from utils.production import calculate_percentiles_from_list, assign_legend_details, assign_color
import numpy as np
from dateutil import relativedelta
from dateutil.relativedelta import relativedelta

REDFIN_KEY = 'redfindata'
REDFIN_MIN_YEAR = 2016
REDFIN_MAX_YEAR = 2022
REDFIN_PROPERTY_TYPES = ['All Residential', 'Single Family Residential', 'Multi-Family (2-4 Unit)']
REDFIN_PROPERTY_TYPES_LOWERCASE = ['all', 'singlefamily', 'multifamily']
REDFIN_DATA_CATEGORIES = ['median_sale_price', 'median_ppsf', 'months_of_supply', 'median_dom', 'price_drops']


redfin_full_dict = [
'mediansaleprice','mediansalepricemom','mediansalepriceyoy','mediandom','mediandommom','mediandomyoy','medianppsf',
'medianlistprice','medianlistpricemom','monthsofsupply','pricedrops','inventory','homessold','soldabovelist'
]


def import_redfin_historical_data(geo_level, default_geoid, geoid_field, collection_name):
    latest_update_date = datetime.datetime(1900, 1, 1)
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
        col_names = []
        for i, row in enumerate(datareader):
            if i < 1:
                headers, col_names = get_header_column_list_and_names(row)
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

            if latest_update_date < datetime.datetime(int(year), int(month), int(day)):
                latest_update_date = datetime.datetime(int(year), int(month), int(day))

            if int(year) < REDFIN_MIN_YEAR:
                continue

            col_map_dict = {}

            try:
                for col_name in col_names:
                    if col_name in ['mediansaleprice','mediandom','medianppsf','medianlistprice','homessold']:
                        col_map_dict[col_name] = string_to_int(row_dict[col_name])
                    else:
                        col_map_dict[col_name] = string_to_float(row_dict[col_name], 5)

            except Exception as e:
                print("Parse error")
                print(e)
                sys.exit()

            if geoid not in geo_data_dict.keys():
                geo_data_dict[geoid] = {
                    '{}'.format(REDFIN_KEY): {
                        'dates': [datetime.datetime(int(year), int(month), int(day))],
                    }
                }

                for col_name in col_names:
                    geo_data_dict[geoid][REDFIN_KEY][col_name] = [col_map_dict[col_name]]


            else:
                existing_geo_data = geo_data_dict[geoid][REDFIN_KEY]
                existing_geo_data['dates'].append(datetime.datetime(int(year), int(month), int(day)))

                for col_name in col_names:
                    existing_geo_data[col_name].append(col_map_dict[col_name])

    insert_list = []

    for k, zip_data in geo_data_dict.items():
        temp_df = pd.DataFrame.from_dict(zip_data[REDFIN_KEY]).sort_values(by='dates')
        temp_df = temp_df.replace({np.nan: None})

        # check if at least 12 months exists
        if len(temp_df) < 12:
            continue

        if temp_df.iloc[len(temp_df)-1].dates.year != REDFIN_MAX_YEAR:
            # print("skipping zipcode: {}. Does not have any data for current year: {}".format(k, REDFIN_MAX_YEAR))
            continue

        temp_dict = {
            '{}'.format(geoid_field): k,
            '{}'.format(REDFIN_KEY): {
                'dates': [],
            }
        }

        for col_name in col_names:
            temp_dict[REDFIN_KEY][col_name] = []

        temp_df = temp_df.reset_index(drop=True)
        prev_date = None
        for i, row in temp_df.iterrows():
            if i == 0:
                prev_date = temp_df['dates'].iloc[0]
                continue

            diff = relativedelta(row['dates'], prev_date)
            month_diff = diff.months
            if month_diff != None and month_diff > 1:
                fill_missing_dates(temp_dict, prev_date, month_diff-1, col_names)

            prev_date = row['dates']
            try:
                temp_dict[REDFIN_KEY]['dates'].append(INDEX_TO_MONTH[row['dates'].month-1] + ' ' + str(row['dates'].year))
            except Exception as e:
                print(e)
                sys.exit()
            try:
                for col_name in col_names:
                    temp_dict[REDFIN_KEY][col_name].append(nat_to_none(row[col_name]))

            except Exception as e:
                print(e)
                sys.exit()

        insert_list.append(temp_dict)

    print("Updating existing historical profiles")
    new_insert_list = update_existing_historical_profile(insert_list, geoid_field, collection_name, geo_level)

    client = mongoclient.connect_to_client(prod_env=ProductionEnvironment.MARKET_PROFILES)
    dbname = Database.MARKET_PROFILES.value
    db = client[dbname]
    collection = db[collection_name]

    collection_filter = {}

    print("Inserting records to database")
    success = mongoclient.batch_inserts_with_list(new_insert_list, collection, collection_filter, geoid_field)

    if not success:
        print("!!! geo historical batch insert failed !!!", len(new_insert_list))
        return success
    else:
        print("Redfin import finished")
        try:
            mongoclient.update_latest_date('redfinziplatest', latest_update_date)
        except Exception as e:
            print("!!! ERROR storing latestupdatedate run to Mongo!!!", e)
            sys.exit()


def fill_missing_dates(temp_dict, prev_date, month_diff, col_names):

    add_date = prev_date + relativedelta(months=1)

    for month_index in range(month_diff):
        temp_dict[REDFIN_KEY]['dates'].append(INDEX_TO_MONTH[add_date.month-1] + ' ' + str(add_date.year))

        for col_name in col_names:
            temp_dict[REDFIN_KEY][col_name].append(None)

        add_date = add_date + relativedelta(months=1)


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
                    '{}'.format(REDFIN_KEY): updated_profile[REDFIN_KEY]
                })
            continue

        existing_profile = existing_profile.iloc[0].to_dict()
        existing_profile[REDFIN_KEY] = updated_profile[REDFIN_KEY]

        if 'rentaltrends' in existing_profile.keys() and existing_profile['rentaltrends'] != existing_profile['rentaltrends']:
            del existing_profile['rentaltrends']

        new_insert_list.append(existing_profile)

    return new_insert_list


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

def get_header_column_list_and_names(headers):
    for i in range(len(headers)):
        if headers[i] == 'median_sale_price':
            headers[i] = 'mediansaleprice'
        elif headers[i] == 'median_sale_price_mom':
            headers[i] = 'mediansalepricemom'
        elif headers[i] == 'median_sale_price_yoy':
            headers[i] = 'mediansalepriceyoy'
        elif headers[i] == 'median_dom':
            headers[i] = 'mediandom'
        elif headers[i] == 'median_dom_mom':
            headers[i] = 'mediandommom'
        elif headers[i] == 'median_dom_yoy':
            headers[i] = 'mediandomyoy'
        elif headers[i] == 'median_ppsf':
            headers[i] = 'medianppsf'
        elif headers[i] == 'median_list_price':
            headers[i] = 'medianlistprice'
        elif headers[i] == 'median_list_price_mom':
            headers[i] = 'medianlistpricemom'
        elif headers[i] == 'months_of_supply':
            headers[i] = 'monthsofsupply'
        elif headers[i] == 'price_drops':
            headers[i] = 'pricedrops'
        elif headers[i] == 'inventory':
            headers[i] = 'inventory'
        elif headers[i] == 'homes_sold':
            headers[i] = 'homessold'
        elif headers[i] == 'sold_above_list':
            headers[i] = 'soldabovelist'

    col_names = ['mediansaleprice','mediansalepricemom',
    'mediansalepriceyoy','mediandom',
    'mediandommom','mediandomyoy',
    'medianppsf','medianlistprice',
    'medianlistpricemom','monthsofsupply',
    'pricedrops','inventory',
    'homessold','soldabovelist']



    return headers, col_names