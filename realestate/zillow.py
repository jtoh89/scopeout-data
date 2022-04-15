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

ZILLOW_MIN_YEAR = 2018

def import_zillow_msa_rental_data(geo_level, default_geoid, geoid_field, geoname_field):
    currpath = os.path.dirname(os.path.abspath(__file__))
    rootpath = os.path.dirname(os.path.abspath(currpath))


    zillow_geo_mapping = mongoclient.query_collection(database_name="Geographies",
                                                      collection_name="ZillowCbsaMapping",
                                                      collection_filter={},
                                                      prod_env=ProductionEnvironment.GEO_ONLY)

    zillow_geo_lookup = dict(zip(zillow_geo_mapping.zillowmsaid, zillow_geo_mapping.cbsacode))

    zillow_dict = {}
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
            property_type = 'All Residential'
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

            if geoid in zillow_dict.keys():
                zillow_data = zillow_dict[geoid][category_name]

                if property_type in zillow_data.keys():
                    zillow_data[property_type]['dates'].append(date_string)
                    zillow_data[property_type]['median_rent'].append(median_rent)
                else:
                    zillow_data[property_type] = {
                        'dates': [date_string],
                        'median_rent': [median_rent],
                    }
            else:
                zillow_dict[geoid] = {
                    geoid_field: geoid,
                    'geolevel': geo_level.value,
                    category_name: {
                        property_type: {
                            'dates': [date_string],
                            'median_rent': [median_rent],
                        }
                    }
                }

        success = store_market_trends_zillow_data(zillow_dict,
                                                  category_name,
                                                  geoid_field=geoid_field,
                                                  geo_level=geo_level)



def store_market_trends_zillow_data(zillow_dict, category_name, geoid_field, geo_level, prod_env=ProductionEnvironment.MARKET_TRENDS):
    print("Storing MarketTrends zillow data into Mongo")
    client = mongoclient.connect_to_client(prod_env=prod_env)
    dbname = 'MarketTrends'

    db = client[dbname]
    collection = db['markettrends']
    collection_filter = {'geolevel': geo_level.value}

    existing_collections = collection.find(collection_filter, {'_id': False})
    existing_list = list(existing_collections)

    if len(existing_list) > 0:
        update_existing_market_trends(existing_list, zillow_dict, geoid_field, category_name)
    else:
        for k, results in zillow_dict.items():
            existing_list.append(results)

    success = mongoclient.batch_inserts_with_list(existing_list, collection, collection_filter, geoid_field)

    if success:
        print("Successfully stored batch into Mongo. Rows inserted: ", len(existing_list))
        return success


def update_existing_market_trends(existing_list, market_trends_dict, geoid_field, category_name):
    for existing_item in existing_list:
        geoid = existing_item[geoid_field]

        if geoid not in market_trends_dict.keys():
            # If geoid does not exist in market trends, then check if the existing data has realestatetrends.
            # If so, delete realestatetrends because it will result in a time gap. For example, 2012-2013, then jumping to 2015.
            # print('DID NOT FIND EXISTING GEOID IN MARKET TRENDS. GEOID: {}'.format(geoid))
            if category_name in existing_item.keys():
                print('!!! DELETING REALESTATETRENDS BECAUSE THERE IS A TIME GAP IN HISTORICAL DATA. GEOID: {}'.format(geoid))
                del existing_item[category_name]
            continue


        existing_item[category_name] = market_trends_dict[geoid][category_name]



def import_zillow_zip_rental_data():
    currpath = os.path.dirname(os.path.abspath(__file__))
    rootpath = os.path.dirname(os.path.abspath(currpath))

    zipcode_historical_profiles = mongoclient.query_collection(database_name="MarketTrends",
                                                              collection_name="zipcodehistoricalprofiles",
                                                              collection_filter={},
                                                              prod_env=ProductionEnvironment.MARKET_TRENDS).drop(columns=["_id"])


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
            property_type = 'All Residential'

            if row.medianrent != row.medianrent:
                median_rent = None
            else:
                median_rent = int(row.medianrent)

            if zipcode in zipcode_rental_dict.keys():
                zipcode_rental_dict[zipcode]['rentaltrends'][property_type]['dates'].append(date_string)
                zipcode_rental_dict[zipcode]['rentaltrends'][property_type]['median_rent'].append(median_rent)
            else:
                zipcode_rental_dict[zipcode] = {
                    'rentaltrends': {
                        '{}'.format(property_type): {
                            'dates': [date_string],
                            'median_rent': [median_rent]
                        }
                    }
                }

    zipcode_list = []

    print('zipcode_rental_dict length: {}'.format(len(zipcode_rental_dict)))

    total_key_deletes = 0
    for i, row in zipcode_historical_profiles.iterrows():
        zipcode = row['zipcode']
        zip_profile = row.to_dict()

        if zipcode in zipcode_rental_dict.keys():
            zip_profile['rentaltrends'] = zipcode_rental_dict[zipcode]['rentaltrends']
            zipcode_list.append(zip_profile)
            total_key_deletes += 1
            del zipcode_rental_dict[zipcode]

    print('Total keys deleted: {}'.format(total_key_deletes))
    print('zipcode_rental_dict length after assignment: {}'.format(len(zipcode_rental_dict)))

    for k, v in zipcode_rental_dict.items():
        zipcode_list.append({
            'zipcode': zipcode,
            'rentaltrends': zipcode_rental_dict[k]['rentaltrends']
        })

    client = mongoclient.connect_to_client(prod_env=ProductionEnvironment.MARKET_TRENDS)
    dbname = 'MarketTrends'
    db = client[dbname]
    collection = db['zipcodehistoricalprofiles']

    collection_filter = {}

    success = mongoclient.batch_inserts_with_list(zipcode_list, collection, collection_filter, 'zipcode')

    if not success:
        print("!!! zipcodehistorical batch insert failed !!!", len(zipcode_list))
        return success

def fill_missing_dates(row, temp_dict, prev_month):
    num_months_between = row['date'].month - prev_month - 1

    for month in range(prev_month+1, num_months_between):
        temp_dict['date'].append(INDEX_TO_MONTH[month-1] + ' ' + str(row['date'].year))
        temp_dict['median_sale_price'].append(None)
