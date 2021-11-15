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
        return pd.DataFrame.from_dict({
            'name': ['United States'],
            'geoid': [DefaultGeoIds.USA.value],
            'stateid': [DefaultGeoIds.USA.value]
        })

    if geo_level == GeoLevels.CBSA:
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
    elif geo_level == GeoLevels.TRACT:
        collection = db['EsriTracts']
        collection_filter = {
            'fipsstatecode': stateid,
        }

    data = list(collection.find(collection_filter))
    df = DataFrame(data)
    df = df.drop(columns=['_id'])

    return df


def store_census_data(geo_level, state_id, filtered_dict, prod_env=ProductionEnvironment.PRODUCTION):
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

        existing_data.update(filtered_dict[geoid]['data'])

    insert_list = existing_list

    if len(insert_list) < 1:
        for k, results in filtered_dict.items():
            insert_list.append(results)

    try:
        tempkey = 'store_census_data. geo_level: {}. state_id: {}'.format(geo_level, state_id)
        store_temp_backup(key=tempkey,insert_list=insert_list)

        collection.delete_many(collection_filter)
        collection.insert_many(insert_list)

        delete_temp_backup(key=tempkey)
    except:
        print("!!! ERROR storing data to Mongo!!!")
        return False

    print("Successfully stored batch into Mongo. Rows inserted: ", len(insert_list))

    return True


def add_finished_run(geo_level, state_id, scopeout_year, category):
    client = connect_to_client(prod_env=ProductionEnvironment.QA)
    db = client['CensusDataInfo']
    # Add entry to finished runs. So if process stops, we can start from where we left off without running through
    # each state and category again.
    try:
        collection = db['FinishedRuns']
        collection_add = {
            'scopeout_year': scopeout_year,
            'state_id': state_id,
            'geo_level': geo_level.value,
            'category': category,
        }
        collection.insert_one(collection_add)
    except:
        print("!!! ERROR storing finished run to Mongo!!!")
        sys.exit()

    print("Successfully stored finished run into Mongo")

def get_finished_runs(geo_level, scopeout_year):
    client = connect_to_client(prod_env=ProductionEnvironment.QA)
    db = client['CensusDataInfo']
    collection = db['FinishedRuns']

    collection_find = {
        'scopeout_year': scopeout_year,
        'geo_level': geo_level.value,
    }

    data = list(collection.find(collection_find))

    if len(data) > 0:
        df = DataFrame(data)
        df = df.drop(columns=['_id'])
    else:
        df = pd.DataFrame(columns=['state_id'])

    return df

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
    except:
        print("!!! ERROR could not store backup to Mongo!!!")
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

def store_county_cbsa_lookup(counties_to_cbsa):
    client = connect_to_client(prod_env=ProductionEnvironment.QA)
    db = client['CensusDataInfo']

    try:
        collection = db['CountyToCbsa']
        collection.insert_many(counties_to_cbsa)
    except:
        print("!!! ERROR could not store backup to Mongo!!!")
        sys.exit()

def test_mongo(data_dict):
    client = connect_to_client(prod_env=ProductionEnvironment.PRODUCTION)
    db = client['scopeout']

    collection = db['test']

    print("Storing census2 data into Mongo")
    collection.insert_one(data_dict)

    print("Successfully stored batch into Mongo")

    return True

