import pandas as pd
import os
import csv
from database import mongoclient
from enums import GeoLevels
from enums import DefaultGeoIds
from lookups import MONTH_FORMAT
from lookups import REDFIN_MSA_TO_CBSA
from enums import ProductionEnvironment


def import_zillow_msa_rental_data(geo_level, default_geoid, geoid_field, geoname_field):
    currpath = os.path.dirname(os.path.abspath(__file__))
    rootpath = os.path.dirname(os.path.abspath(currpath))


    zillow_geo_mapping = mongoclient.query_collection(database_name="Geographies",
                                                      collection_name="Zillow_Cbsa_Mapping",
                                                      collection_filter={},
                                                      prod_env=ProductionEnvironment.GEO_ONLY)

    zillow_geo_lookup = dict(zip(zillow_geo_mapping.zillowmsaid, zillow_geo_mapping.cbsacode))

    zillow_dict = {}
    missing_zillow_ids = []

    file_dir = '/files/Metro_ZORI_AllHomesPlusMultifamily_SSA.csv'

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



        # if success:
        #     collection_add_finished_run = {
        #         'category': category_name,
        #         'geo_level': geo_level.value,
        #     }
        #     mongoclient.add_finished_run(collection_add_finished_run)



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

    success = mongoclient.store_market_trends(existing_list, collection, collection_filter, geoid_field)

    if success:
        print("Successfully stored batch into Mongo. Rows inserted: ", len(existing_list))
        return success


def update_existing_market_trends(existing_list, market_trends_dict, geoid_field, category_name):
    for existing_item in existing_list:
        geoid = existing_item[geoid_field]

        if geoid not in market_trends_dict.keys():
            # If geoid does not exist in market trends, then check if the existing data has realestatetrends.
            # If so, delete realestatetrends because it will result in a time gap. For example, 2012-2013, then jumping to 2015.
            print('DID NOT FIND EXISTING GEOID IN MARKET TRENDS. GEOID: {}'.format(geoid))
            if category_name in existing_item.keys():
                print('!!! DELETING REALESTATETRENDS BECAUSE THERE IS A TIME GAP IN HISTORICAL DATA. GEOID: {}'.format(geoid))
                del existing_item[category_name]
            continue


        existing_item[category_name] = market_trends_dict[geoid][category_name]



def import_zillow_zip_rental_data(geo_level, default_geoid, geoid_field, geoname_field):
    currpath = os.path.dirname(os.path.abspath(__file__))
    rootpath = os.path.dirname(os.path.abspath(currpath))


    zillow_geo_mapping = mongoclient.query_collection(database_name="Geographies",
                                                      collection_name="Zillow_Cbsa_Mapping",
                                                      collection_filter={},
                                                      prod_env=ProductionEnvironment.GEO_ONLY)

    zillow_geo_lookup = dict(zip(zillow_geo_mapping.zillowmsaid, zillow_geo_mapping.cbsacode))

    zillow_dict = {}
    missing_zillow_ids = []

    file_dir = ''
    if geo_level == GeoLevels.CBSA:
        file_dir = '/files/Metro_ZORI_AllHomesPlusMultifamily_SSA.csv'
    elif geo_level == GeoLevels.ZIPCODE:
        file_dir = '/files/Zip_ZORI_AllHomesPlusMultifamily_SSA.csv'

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

        # if len(missing_geos_df) > 0:
        #     missing_geos_df.to_csv('missing_redfin.csv')


        success = mongoclient.store_market_trends_data(zillow_dict,
                                                       category_name,
                                                       geoid_field=geoid_field,
                                                       geo_level=geo_level)

        collection_add_finished_run = {
            'category': category_name,
            'geo_level': geo_level.value,
        }

        if success:
            mongoclient.add_finished_run(collection_add_finished_run)


