import pandas as pd
import os
import csv
from database import mongoclient
from enums import GeoLevels
from enums import DefaultGeoIds
from lookups import MONTH_FORMAT
from lookups import REDFIN_MSA_TO_CBSA
from enums import ProductionEnvironment
import datetime
import sys
from lookups import INDEX_TO_MONTH
from utils.utils import drop_na_values_from_dict

ZILLOW_MIN_YEAR = 2018

def import_zillow_msa_rental_data(geo_level, default_geoid, geoid_field, geoname_field, collection_name):
    currpath = os.path.dirname(os.path.abspath(__file__))
    rootpath = os.path.dirname(os.path.abspath(currpath))


    zillow_geo_mapping = mongoclient.query_collection(database_name="Geographies",
                                                      collection_name="ZillowCbsaMapping",
                                                      collection_filter={},
                                                      prod_env=ProductionEnvironment.GEO_ONLY)

    zillow_geo_lookup = dict(zip(zillow_geo_mapping.zillowmsaid, zillow_geo_mapping.cbsacode))

    zillow_rental_dict = {}
    missing_zillow_ids = []

    file_dir = '/files/Metro_ZORI_AllHomesPlusMultifamily_Smoothed.csv'

    with open(rootpath + file_dir) as file:
        category_name = 'rentaltrends'
        df = pd.read_csv(file)
        df = df.drop(columns=['SizeRank'])
        df = df.melt(id_vars=['RegionID','RegionName'])
        df = df.rename(columns={'variable':'datestring', 'value': 'medianrent'})
        df['medianrent'] = df['medianrent'].fillna(0)

        for i, row in df.iterrows():
            zillow_id = row.RegionID
            year_string = row.datestring[:4]
            month_string = row.datestring[5:7]
            date_string = MONTH_FORMAT[month_string] + ' ' + year_string
            median_rent = int(row.medianrent)
            geoid = ''

            if zillow_id in zillow_geo_lookup.keys():
                geoid = str(zillow_geo_lookup[zillow_id]).zfill(5)

                if geo_level == GeoLevels.CBSA and geoid == DefaultGeoIds.USA.value:
                    continue

                if geo_level == GeoLevels.USA and geoid != DefaultGeoIds.USA.value:
                    continue
            else:
                if zillow_id not in missing_zillow_ids:
                    print('No match for zillow geo. zillowid: {}.'.format(zillow_id))
                    missing_zillow_ids.append(zillow_id)
                    continue

            if geoid in zillow_rental_dict.keys():
                zillow_rental_dict[geoid]['dates'].append(date_string)
                zillow_rental_dict[geoid]['median_rent'].append(median_rent)

            else:
                zillow_rental_dict[geoid] = {
                    'dates': [date_string],
                    'median_rent': [median_rent],
                }


    existing_historical_profiles = mongoclient.query_collection(database_name="MarketProfiles",
                                                               collection_name=collection_name,
                                                               collection_filter={},
                                                               prod_env=ProductionEnvironment.MARKET_PROFILES).drop(columns=["_id"])

    insert_list = []

    for i, row in existing_historical_profiles.iterrows():
        row_dict = row.to_dict()
        geoid = row_dict[geoid_field]
        if geoid in zillow_rental_dict.keys():
            row_dict['rentaltrends'] = zillow_rental_dict[geoid]
            insert_list.append(row_dict)


    client = mongoclient.connect_to_client(prod_env=ProductionEnvironment.MARKET_PROFILES)
    dbname = 'MarketProfiles'
    db = client[dbname]
    collection = db[collection_name]

    collection_filter = {}
    success = mongoclient.batch_inserts_with_list(insert_list, collection, collection_filter, geoid_field)

    if not success:
        print("!!! zipcodehistorical batch insert failed !!!", len(insert_list))
        return success



def import_zillow_zip_rental_data(collection_name):
    currpath = os.path.dirname(os.path.abspath(__file__))
    rootpath = os.path.dirname(os.path.abspath(currpath))

    file_dir = '/files/Zip_ZORI_AllHomesPlusMultifamily_Smoothed.csv'

    zipcode_rental_dict = {}

    with open(rootpath + file_dir) as file:
        df = pd.read_csv(file)
        df = df.drop(columns=['SizeRank','MsaName'])
        df = df.melt(id_vars=['RegionID','RegionName'])
        df = df.rename(columns={'variable':'datestring', 'value': 'medianrent'})
        df['medianrent'] = df['medianrent']
        df['date'] = pd.to_datetime(df['datestring'], format='%Y-%m')
        df = df.sort_values(by='date')

        for i, row in df.iterrows():
            zipcode = str(row.RegionName).zfill(5)

            year_string = row.datestring[:4]

            if int(year_string) < ZILLOW_MIN_YEAR:
                continue


            month_string = row.datestring[5:7]
            date_string = MONTH_FORMAT[month_string] + ' ' + year_string

            if row.medianrent != row.medianrent:
                median_rent = None
            else:
                median_rent = int(row.medianrent)

            if zipcode in zipcode_rental_dict.keys():
                zipcode_rental_dict[zipcode]['dates'].append(date_string)
                zipcode_rental_dict[zipcode]['median_rent'].append(median_rent)
            else:
                zipcode_rental_dict[zipcode] = {
                    'dates': [date_string],
                    'median_rent': [median_rent]
                }

    print('zipcode_rental_dict length: {}'.format(len(zipcode_rental_dict)))

    existing_zip_historical_profiles = mongoclient.query_collection(database_name="MarketProfiles",
                                                               collection_name=collection_name,
                                                               collection_filter={},
                                                               prod_env=ProductionEnvironment.MARKET_PROFILES).drop(columns=["_id"])

    insert_list = []
    total_key_deletes = 0
    for i, row in existing_zip_historical_profiles.iterrows():
        row_dict = row.to_dict()
        row_dict = drop_na_values_from_dict(row_dict)
        zipcode = row['zipcode']

        if zipcode in zipcode_rental_dict.keys():
            row_dict['rentaltrends'] = zipcode_rental_dict[zipcode]
            insert_list.append(row_dict)
            total_key_deletes += 1
            del zipcode_rental_dict[zipcode]

    print('Total keys deleted: {}'.format(total_key_deletes))
    print('zipcode_rental_dict length after assignment: {}'.format(len(zipcode_rental_dict)))

    ## Need to add zipcodes that are not in ScopeOut markets
    for k, v in zipcode_rental_dict.items():
        insert_list.append({
            'zipcode': zipcode,
            'rentaltrends': zipcode_rental_dict[k]
        })

    client = mongoclient.connect_to_client(prod_env=ProductionEnvironment.MARKET_PROFILES)
    dbname = 'MarketProfiles'
    db = client[dbname]
    collection = db[collection_name]

    collection_filter = {}

    success = mongoclient.batch_inserts_with_list(insert_list, collection, collection_filter, 'zipcode')

    if not success:
        print("!!! zipcodehistorical batch insert failed !!!", len(insert_list))
        return success

def fill_missing_dates(row, temp_dict, prev_month):
    num_months_between = row['date'].month - prev_month - 1

    for month in range(prev_month+1, num_months_between):
        temp_dict['date'].append(INDEX_TO_MONTH[month-1] + ' ' + str(row['date'].year))
        temp_dict['median_sale_price'].append(None)
