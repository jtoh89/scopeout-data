import sys
from pymongo import MongoClient
import os
from dotenv import load_dotenv
from pandas import DataFrame
import pandas as pd
import json
from bson import json_util
from enums import GeoLevels
from enums import ProductionEnvironment
from enums import DefaultGeoIds

def connect_to_client(prod_env):
    load_dotenv()

    if prod_env == ProductionEnvironment.PRODUCTION:
        host = os.getenv("MONGO_HOST")
        database = os.getenv("MONGO_DATABASE")
        un = os.getenv("MONGO_USERNAME")
        pw = os.getenv("MONGO_PASSWORD")
    elif prod_env == ProductionEnvironment.GEO_ONLY:
        host = os.getenv("GEO_ONLY_MONGO_HOST")
        database = os.getenv("GEO_ONLY_MONGO_DATABASE")
        un = os.getenv("GEO_ONLY_MONGO_USERNAME")
        pw = os.getenv("GEO_ONLY_MONGO_PASSWORD")
    elif prod_env == ProductionEnvironment.QA:
        host = os.getenv("QA_MONGO_HOST")
        database = os.getenv("QA_MONGO_DATABASE")
        un = os.getenv("QA_MONGO_USERNAME")
        pw = os.getenv("QA_MONGO_PASSWORD")
    elif prod_env == ProductionEnvironment.CENSUS_DATA1:
        host = os.getenv("CENSUS_DATA1_HOST")
        database = os.getenv("CENSUS_DATA1_DATABASE")
        un = os.getenv("CENSUS_DATA1_USERNAME")
        pw = os.getenv("CENSUS_DATA1_PASSWORD")
    elif prod_env == ProductionEnvironment.CENSUS_DATA2:
        host = os.getenv("CENSUS_DATA2_HOST")
        database = os.getenv("CENSUS_DATA2_DATABASE")
        un = os.getenv("CENSUS_DATA2_USERNAME")
        pw = os.getenv("CENSUS_DATA2_PASSWORD")
    elif prod_env == ProductionEnvironment.MARKET_TRENDS:
        host = os.getenv("MARKET_TRENDS_MONGO_HOST")
        database = os.getenv("MARKET_TRENDS_MONGO_DATABASE")
        un = os.getenv("MARKET_TRENDS_MONGO_USERNAME")
        pw = os.getenv("MARKET_TRENDS_MONGO_PASSWORD")

    connection_string = 'mongodb+srv://{}:{}@{}/{}?retryWrites=true&w=majority' \
        .format(un, pw, host, database)


    client = MongoClient(connection_string)

    return client


def query_collection(database_name, collection_name, collection_filter, prod_env):
    client = connect_to_client(prod_env=prod_env)
    db = client[database_name]
    collection = db[collection_name]

    data = collection.find(collection_filter)
    data = list(data)
    df = DataFrame(data)

    return df


def query_geography(geo_level, stateid):
    '''
    Retrieves all geographies based on geo_level provided.
    :param geo_level:
    :return: all geographies
    '''
    client = connect_to_client(prod_env=ProductionEnvironment.GEO_ONLY)
    db = client['Geographies']
    collection = None

    if geo_level == GeoLevels.USA:
        collection = db['USA']
        collection_filter = {}
    elif geo_level == GeoLevels.CBSA:
        collection = db['Cbsa']
        collection_filter = {}
    elif geo_level == GeoLevels.STATE:
        collection = db['State']
        collection_filter = {
            'fipsstatecode': stateid,
        }
    elif geo_level == GeoLevels.COUNTY:
        collection = db['County']

        collection_filter = {
            'stateinfo.fipsstatecode': stateid,
        }

        if stateid == None:
            collection_filter = {}

    elif geo_level == GeoLevels.TRACT:
        collection = db['EsriTracts']
        collection_filter = {
            'fipsstatecode': stateid,
        }
    elif geo_level == GeoLevels.ZIPCODE:
        collection = db['ZipCountyCbsa']
        collection_filter = {}
    else:
        return pd.DataFrame()

    data = list(collection.find(collection_filter))
    df = DataFrame(data)
    df = df.drop(columns=['_id'])

    return df

def create_county_to_cbsa_lookup():
    '''
    Function creates lookup for counties to cbsa ids
    :return: None
    '''
    cbsa_data = query_collection(database_name="Geographies",
                                             collection_name="Cbsa",
                                             collection_filter={},
                                             prod_env=ProductionEnvironment.GEO_ONLY)

    counties_to_cbsa = []
    for i, cbsa in cbsa_data.iterrows():
        cbsaid = cbsa['cbsacode']
        cbsaname = cbsa['cbsatitle']
        for county in cbsa['counties']:
            stateid = county['stateinfo']['fipsstatecode']
            counties_to_cbsa.append({
                'countyfullcode': county['countyfullcode'],
                'cbsacode': cbsaid,
                'cbsaname': cbsaname,
                'stateid': stateid
            })

    insert_list_mongo(list_data=counties_to_cbsa,
                                  dbname='Geographies',
                                  collection_name='CountyToCbsa',
                                  prod_env=ProductionEnvironment.GEO_ONLY)

def store_neighborhood_data(state_id, neighborhood_profile_list):
    prod_env = ProductionEnvironment.PRODUCTION
    client = connect_to_client(prod_env=prod_env)
    dbname = 'scopeout'

    db = client[dbname]
    collection = db['neighborhoodprofiles']

    try:
        tempkey = 'store_neighborhood_data. state_id: {}'.format(state_id)
        store_temp_backup(key=tempkey,insert_list=neighborhood_profile_list)

        collection.delete_many({'stateid': state_id})
        collection.insert_many(neighborhood_profile_list)

        delete_temp_backup(key=tempkey)
    except:
        print("!!! ERROR storing data to Mongo!!!")
        return False

    print("Successfully stored store_neighborhood_data into Mongo. Rows inserted: ", len(neighborhood_profile_list))

    return True


def store_census_data(geo_level, state_id, filtered_dict, prod_env=ProductionEnvironment.PRODUCTION, county_batches=False):
    client = connect_to_client(prod_env=prod_env)

    if prod_env == ProductionEnvironment.CENSUS_DATA1:
        dbname = 'censusdata1'
        print("Storing census1 data into Mongo")
    elif prod_env == ProductionEnvironment.CENSUS_DATA2:
        dbname = 'censusdata2'
        print("Storing census2 data into Mongo")
    else:
        dbname = 'scopeout'

    db = client[dbname]
    collection = db['CensusData']

    if state_id:
        collection_filter = {
            'stateid': state_id,
            'geolevel': geo_level.value,
        }
    else:
        collection_filter = {
            'geolevel': geo_level.value,
        }

    existing_collections = collection.find(collection_filter)
    existing_list = list(existing_collections)

    for item_dict in existing_list:
        geoid = item_dict['geoid']
        existing_data = item_dict['data']

        if geoid not in filtered_dict.keys():
            print('DID NOT FIND GEO IN FILTERED DICT. GEOID: ', geoid)
            continue

        # append data to dict
        existing_data.update(filtered_dict[geoid]['data'])

    insert_list = existing_list

    if len(insert_list) < 1:
        for k, results in filtered_dict.items():
            insert_list.append(results)

    insert_batches = {}
    if county_batches == True:
        for row in insert_list:
            countyfullcode = row['countyfullcode']
            if countyfullcode not in insert_batches.keys():
                insert_batches[countyfullcode] = [row]
            else:
                insert_batches[countyfullcode].append(row)
    else:
        insert_batches = {'singlebatch': insert_list}

    total_inserts = 0
    for k, data_list in insert_batches.items():
        if k != "singlebatch":
            collection_filter['countyfullcode'] = k
        try:
            tempkey = 'store_census_data. geo_level: {}. state_id: {}'.format(geo_level, state_id)

            use_single_inserts = store_temp_backup(key=tempkey,insert_list=data_list)

            if use_single_inserts:
                perform_small_batch_inserts_census_tracts(data_list, tempkey, collection, collection_filter, geo_level)
            else:
                collection.delete_many(collection_filter)
                try:
                    collection.insert_many(data_list)
                except Exception as e:
                    print("!!! Could not perform insert many into Census Data. Try again with single insert. Err: ", e)
                    perform_small_batch_inserts_census_tracts(data_list, tempkey, collection, collection_filter, geo_level)

            delete_temp_backup(key=tempkey)
            total_inserts += len(data_list)

        except Exception as e:
            print("!!! ERROR storing data to Mongo:!!! DETAILS: ", e)
            return False

    print("Successfully stored batch into Mongo. Rows inserted: ", total_inserts)

    return True

def store_market_trends(data_list, collection, collection_filter, geoid_field):
    '''
    Function will insert records into mongo 99 records at a time.

    :param data_list:
    :param collection:
    :param collection_filter:
    :param geoid_field:
    :return:
    '''
    try:
        tempkey = 'store_market_trends_data'

        insert_ids = []

        insert_list = []
        for i, data in enumerate(data_list, 1):
            insert_ids.append(data[geoid_field])
            insert_list.append(data)
            if i % 99 == 0:
                market_trends_db_insert(insert_list, insert_ids, collection, collection_filter, tempkey, geoid_field)

                insert_ids = []
                insert_list = []

        # insert any remaining inserts
        if len(insert_list) > 0:
            market_trends_db_insert(insert_list, insert_ids, collection, collection_filter, tempkey, geoid_field)

    except Exception as e:
        print("!!! ERROR storing data to Mongo:!!! DETAILS: ", e)
        return False

    return True


def market_trends_db_insert(insert_list, insert_ids, collection, collection_filter, tempkey, geoid_field):
    collection_filter[geoid_field] = {'$in': insert_ids}
    insert_failed = store_temp_backup(key=tempkey,insert_list=insert_list)

    if not insert_failed:
        collection.delete_many(collection_filter)

    try:
        collection.insert_many(insert_list)
    except Exception as e:
        print("!!! Could not perform insert many into Census Data. Try again with single insert. Err: ", e)


    delete_temp_backup(key=tempkey)


def perform_small_batch_inserts_census_tracts(data_list, tempkey, collection, collection_filter, geo_level):
    if geo_level != GeoLevels.TRACT:
        print('ERROR!!! SINGLE INSERTS IMPLEMENTED ONLY FOR TRACTS')
        sys.exit()

    print("Finished single inserts. Num of records: ", len(data_list))

    remove_list = []
    insert_list = []
    for i, row in enumerate(data_list, 1):
        tractid = row['geoid']
        if i % 99 == 0:
            store_temp_backup(key=tempkey,insert_list=insert_list)

            collection.delete_many({
                'geolevel':'tract',
                'geoid': {'$in': remove_list}})
            collection.insert_many(insert_list)
            delete_temp_backup(key=tempkey)

            print("Finished small batch insert. Current index: ", i)

            insert_list.clear()
            remove_list.clear()
        else:
            insert_list.append(row)
            remove_list.append(tractid)

    print("Finished single inserts. Num of records: ", len(data_list) )

def add_finished_run(collection_add_finished_run):
    client = connect_to_client(prod_env=ProductionEnvironment.QA)
    db = client['CensusDataInfo']
    # Add entry to finished runs. So if process stops, we can start from where we left off without running through
    # each state and category again.
    try:
        collection = db['FinishedRuns']

        collection.insert_one(collection_add_finished_run)
    except:
        print("!!! ERROR storing finished run to Mongo!!!")
        sys.exit()

    print("Successfully stored finished run into Mongo")

def get_finished_runs(collection_find_finished_runs):
    client = connect_to_client(prod_env=ProductionEnvironment.QA)
    db = client['CensusDataInfo']
    collection = db['FinishedRuns']

    data = list(collection.find(collection_find_finished_runs))

    if len(data) > 0:
        df = DataFrame(data)
        df = df.drop(columns=['_id'])
    else:
        df = pd.DataFrame(columns=['state_id'])

    return df

def update_finished_run(collection_add_finished_run, geo_level, category):
    client = connect_to_client(prod_env=ProductionEnvironment.QA)
    db = client['CensusDataInfo']



    # Add entry to finished runs. So if process stops, we can start from where we left off without running through
    # each state and category again.
    try:
        collection = db['FinishedRuns']
        collection.delete_one({'category': category, 'geo_level': geo_level.value})
        collection.insert_one(collection_add_finished_run)
    except:
        print("!!! ERROR storing finished run to Mongo!!!")
        sys.exit()

    print("Successfully stored finished run into Mongo")

def store_missing_geo(missing_geo, geo_level, state_id, category):
    print("!!! STORING MISSING GEO !!! COUNT: ".format(len(missing_geo)))
    client = connect_to_client(prod_env=ProductionEnvironment.QA)
    db = client['CensusDataInfo']

    collection = db['CensusDataMissingInEsri']

    delete_filter = {
        'stateid': state_id,
        'geolevel': geo_level.value,
        'category': category,
    }

    deleted = collection.delete_many(delete_filter)
    print("Cleared out CensusDataMissingInEsri ", deleted.deleted_count)

    try:
        collection.insert_many(missing_geo)
    except:
        print("!!! ERROR storing data to Mongo!!!")

def get_mongo_info_from_environment(prod_env):
    if prod_env == ProductionEnvironment.CENSUS_DATA1:
        return {
            'dbname': 'censusdata1',
            'collectionname': 'CensusData'
        }

    elif prod_env == ProductionEnvironment.CENSUS_DATA2:
        return {
            'dbname': 'censusdata2',
            'collectionname': 'CensusData'
        }

def store_temp_backup(key, insert_list):
    client = connect_to_client(prod_env=ProductionEnvironment.QA)
    db = client['CensusDataInfo']

    try:
        collection = db['TempBackUp']
        collection_add = {
            'key': key,
            'data': insert_list
        }
        collection.insert_one(collection_add)

        return False
    except Exception as e:
        print("!!! ERROR could not store backup to Mongo!!! Details: ", e)

        error_string = str(e)[:23]
        if error_string == 'BSON document too large':
            return True
        else:
            sys.exit()

def delete_temp_backup(key):
    client = connect_to_client(prod_env=ProductionEnvironment.QA)
    db = client['CensusDataInfo']

    try:
        collection = db['TempBackUp']
        collection_delete = {
            'key': key,
        }
        collection.delete_one(collection_delete)
    except:
        print("!!! ERROR could not delete backup Mongo!!!")
        sys.exit()


def insert_list_mongo(list_data, dbname, collection_name, prod_env):
    client = connect_to_client(prod_env=prod_env)
    db = client[dbname]

    try:
        collection = db[collection_name]
        collection.insert_many(list_data)
    except:
        print("!!! ERROR could not store insert_list_mongo to Mongo!!!")
        sys.exit()

def test_mongo(data_dict):
    client = connect_to_client(prod_env=ProductionEnvironment.PRODUCTION)
    db = client['scopeout']

    collection = db['test']

    print("Storing census2 data into Mongo")
    collection.insert_one(data_dict)

    print("Successfully stored batch into Mongo")

    return True

