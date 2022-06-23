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
from utils.utils import string_to_int, nat_to_none, string_to_float
from models import zipcodemarketmap
from utils.production import calculate_percentiles_from_list, assign_legend_details, assign_color
import numpy as np
from dateutil import relativedelta
from dateutil.relativedelta import relativedelta


REALTOR_MIN_YEAR = 2016
REALTOR_MAX_YEAR = 2022
REDFIN_PROPERTY_TYPES = ['All Residential', 'Single Family Residential', 'Multi-Family (2-4 Unit)']
REDFIN_PROPERTY_TYPES_LOWERCASE = ['all', 'singlefamily', 'multifamily']
REDFIN_DATA_CATEGORIES = ['median_sale_price', 'median_ppsf', 'months_of_supply', 'median_dom', 'price_drops']


def import_realtor_historical_data(geo_level, default_geoid, geoid_field, geoname_field, collection_name):
    latest_update_date = datetime.datetime(1900, 1, 1)
    currpath = os.path.dirname(os.path.abspath(__file__))
    rootpath = os.path.dirname(os.path.abspath(currpath))

    geographies_df = mongoclient.query_geography(geo_level=geo_level, stateid=default_geoid)
    geo_list = list(geographies_df[geoid_field])
    geoid_field = ''
    file_dir = ''
    if geo_level == GeoLevels.USA:
        file_dir = '/files/us_national_market_tracker.tsv'
    # elif geo_level == GeoLevels.CBSA:
    #     file_dir = '/files/redfin_metro_market_tracker.tsv'
    # elif geo_level == GeoLevels.COUNTY:
    #     file_dir = '/files/county_market_tracker.tsv'
    elif geo_level == GeoLevels.ZIPCODE:
        file_dir = '/files/RDC_Inventory_Core_Metrics_Zip_History.csv'
        geoid_field = 'postal_code'

    geo_data_dict = {}

    with open(rootpath + file_dir, 'r') as csvfile:
        datareader = csv.reader(csvfile)
        headers = []

        for i, row in enumerate(datareader):
            if i < 1:
                headers = row
                continue

            if i % 100000 == 0:
                print("Reading {} total lines from redfin zip file".format(i+1))

            ### create dict
            row_dict = dict(zip(headers, row))

            geoid = row_dict[geoid_field]

            if geo_level == GeoLevels.USA:
                geoid = REDFIN_USA_TO_FIPS[geoid]
            elif geo_level == GeoLevels.CBSA:
                if geoid in REDFIN_MSA_TO_CBSA.keys():
                    geoid = REDFIN_MSA_TO_CBSA[geoid]
            elif geo_level == GeoLevels.COUNTY:
                if geoid in REDFIN_COUNTYID_TO_FIPS.keys():
                    geoid = REDFIN_COUNTYID_TO_FIPS[geoid]

            if geoid not in geo_list and geo_level != GeoLevels.ZIPCODE:
                continue

            month = row_dict['month_date_yyyymm'][4:]
            year = row_dict['month_date_yyyymm'][:4]
            date = datetime.datetime(int(year), int(month), 1)

            if latest_update_date < date:
                latest_update_date = date

            if int(year) < REALTOR_MIN_YEAR:
                continue


            try:
                median_listing_price = string_to_int(row_dict['median_listing_price'])
                median_listing_price_per_square_foot = string_to_int(row_dict['median_listing_price_per_square_foot'])
                active_listing_count = string_to_int(row_dict['active_listing_count'])
                total_listing_count = string_to_int(row_dict['total_listing_count'])

            except Exception as e:
                print("Parse error")
                print(e)
                sys.exit()

            if geoid not in geo_data_dict.keys():
                geo_data_dict[geoid] = {
                    'realtordata': {
                        'dates': [date],
                        'medianlistprice': [median_listing_price],
                        'medianlistpricepsqft': [median_listing_price_per_square_foot],
                        'activelistings': [active_listing_count],
                        'totallistings': [total_listing_count],
                    }
                }
            else:
                existing_geo_data = geo_data_dict[geoid]['realtordata']
                existing_geo_data['dates'].insert(0, date)
                existing_geo_data['medianlistprice'].insert(0, median_listing_price)
                existing_geo_data['medianlistpricepsqft'].insert(0, median_listing_price_per_square_foot)
                existing_geo_data['activelistings'].insert(0, active_listing_count)
                existing_geo_data['totallistings'].insert(0, total_listing_count)

    insert_list = []

    for k, zip_data in geo_data_dict.items():
        temp_df = pd.DataFrame.from_dict(zip_data['realtordata']).sort_values(by='dates')
        temp_df = temp_df.replace({np.nan: None})

        # check if at least 12 months exists
        if len(temp_df) < 12:
            continue

        if temp_df.iloc[len(temp_df)-1].dates.year != REALTOR_MAX_YEAR:
            # print("skipping zipcode: {}. Does not have any data for current year: {}".format(k, REDFIN_MAX_YEAR))
            continue

        temp_dict = {
            '{}'.format('zipcode'): k,
            'realtordata': {
                'dates': [],
                'medianlistprice': [],
                'medianlistpricepsqft': [],
                'activelistings': [],
                'totallistings': [],
            }
        }
        temp_df = temp_df.reset_index(drop=True)
        prev_date = None
        for i, row in temp_df.iterrows():
            if i == 0:
                prev_date = temp_df['dates'].iloc[0]
                continue

            diff = relativedelta(row['dates'], prev_date)
            month_diff = diff.months
            if month_diff != None and month_diff > 1:
                # print("!!! ERROR - diff not implemented for realtor data !!!")
                # continue
                fill_missing_dates(temp_dict, prev_date, month_diff-1)

            prev_date = row['dates']
            try:
                temp_dict['realtordata']['dates'].append(INDEX_TO_MONTH[row['dates'].month-1] + ' ' + str(row['dates'].year))
            except Exception as e:
                print(e)
                sys.exit()
            try:
                temp_dict['realtordata']['medianlistprice'].append(nat_to_none(row.medianlistprice))
                temp_dict['realtordata']['medianlistpricepsqft'].append(nat_to_none(row.medianlistpricepsqft))
                temp_dict['realtordata']['activelistings'].append(nat_to_none(row.activelistings))
                temp_dict['realtordata']['totallistings'].append(nat_to_none(row.totallistings))
            except Exception as e:
                print(e)
                sys.exit()

        insert_list.append(temp_dict)

    print("Updating existing historical profiles")
    new_insert_list = update_existing_historical_profile(insert_list, "zipcode", collection_name, geo_level)

    client = mongoclient.connect_to_client(prod_env=ProductionEnvironment.MARKET_PROFILES)
    dbname = 'MarketProfiles'
    db = client[dbname]
    collection = db[collection_name]

    collection_filter = {}

    print("Inserting records to database")
    success = mongoclient.batch_inserts_with_list(new_insert_list, collection, collection_filter, "zipcode")

    if not success:
        print("!!! geo historical batch insert failed !!!", len(new_insert_list))
        return success
    else:
        print("Realtor import finished")
        try:
            client = mongoclient.connect_to_client(prod_env=ProductionEnvironment.QA)
            dbname = 'LatestUpdates'
            db = client[dbname]

            insert = {
                      'category':'realtor',
                      'geolevel': geo_level.value,
                      'lastupdatedate': latest_update_date,
                      'year': latest_update_date.year,
                      'month': latest_update_date.month,
                      'datestring': INDEX_TO_MONTH[latest_update_date.month-1] + ' ' + str(latest_update_date.year)
                      }

            collection = db['lastupdates']
            collection.delete_one({'geolevel': geo_level.value})
            collection.insert_one(insert)
        except Exception as e:
            print("!!! ERROR storing latestupdatedate run to Mongo!!!", e)
            sys.exit()


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
                    'realtordata': updated_profile['realtordata']
                })
            continue

        existing_profile = existing_profile.iloc[0].to_dict()
        existing_profile['realtordata'] = updated_profile['realtordata']

        new_insert_list.append(existing_profile)

    return new_insert_list



def fill_missing_dates(temp_dict, prev_date, month_diff):

    add_date = prev_date + relativedelta(months=1)

    for month_index in range(month_diff):
        temp_dict['realtordata']['dates'].append(INDEX_TO_MONTH[add_date.month-1] + ' ' + str(add_date.year))
        temp_dict['realtordata']['medianlistprice'].append(None)
        temp_dict['realtordata']['medianlistpricepsqft'].append(None)
        temp_dict['realtordata']['activelistings'].append(None)
        temp_dict['realtordata']['totallistings'].append(None)
