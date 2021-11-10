import sys
from pymongo import MongoClient
import os
from dotenv import load_dotenv
from pandas import DataFrame
import pandas as pd
import json
from bson import json_util
from enums import GeoLevels

def connect_to_client(prod_env):
    load_dotenv()

    if prod_env == "prod":
        host = os.getenv("MONGO_HOST")
        database = os.getenv("MONGO_DATABASE")
        un = os.getenv("MONGO_USERNAME")
        pw = os.getenv("MONGO_PASSWORD")
    elif prod_env == "geoonly":
        host = os.getenv("GEO_ONLY_MONGO_HOST")
        database = os.getenv("GEO_ONLY_MONGO_DATABASE")
        un = os.getenv("GEO_ONLY_MONGO_USERNAME")
        pw = os.getenv("GEO_ONLY_MONGO_PASSWORD")
    elif prod_env == "qa":
        host = os.getenv("QA_MONGO_HOST")
        database = os.getenv("QA_MONGO_DATABASE")
        un = os.getenv("QA_MONGO_USERNAME")
        pw = os.getenv("QA_MONGO_PASSWORD")

    connection_string = 'mongodb+srv://{}:{}@{}/{}?retryWrites=true&w=majority'\
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


def query_geography(geo_level):
    '''
    Retrieves all geographies based on geo_level provided.
    :param geo_level:
    :return: all geographies
    '''
    client = connect_to_client(prod_env="geoonly")
    db = client['ScopeOutGeographies']
    collection = None

    if geo_level == GeoLevels.CBSA:
        collection = db['Cbsa']
    elif geo_level == GeoLevels.STATE:
        collection = db['State']
    elif geo_level == GeoLevels.COUNTY:
        collection = db['County']
    elif geo_level == GeoLevels.TRACT:
        collection = db['EsriTracts']

    data = list(collection.find())
    df = DataFrame(data)
    df = df.drop(columns=['_id'])

    return df


def store_census_data(data_dict):
    client = connect_to_client(prod_env="prod")
    db = client['scopeout']

    collection = db['CensusData']

    print("Storing census data into Mongo")
    for k, results in data_dict.items():
        collection_filter = {
            'geoid': results['geoid'],
            'geolevel': results['geolevel'],
        }

        existing_data = collection.find_one(collection_filter)

        try:
            if existing_data:
                # iterate through current data. Update existing data.
                for year, variable_data in results['data'].items():
                    category = list(variable_data.keys())[0]
                    values = list(variable_data.values())[0]

                    if year in existing_data['data'].keys():
                        existing_data['data'][year][category] = values
                    else:
                        existing_data['data'][year] = {'{}'.format(category): values}

                    collection_filter = {
                        'geoid': results['geoid'],
                        'geolevel': results['geolevel'],
                    }

                collection.update(collection_filter, {'$set': existing_data})
            else:
                collection.insert_one(results)
        except:
            print("!!! ERROR storing data to Mongo!!!")
            return False

    print("Successfully stored batch into Mongo")

    return True


def add_finished_run(geo_level, state_id, scopeout_year, category):
    client = connect_to_client(prod_env="prod")
    db = client['scopeout']
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
    client = connect_to_client(prod_env="prod")
    db = client['scopeout']
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

def store_missing_geo(geo_id, geo_level, category):
    print("!!! FOUND MISSING GEO !!! - geo_id: {}. geo_level: {}. category: {} ".format(geo_id, geo_level, category))

    client = connect_to_client(prod_env="prod")
    db = client['scopeout']

    collection = db['CensusDataMissingInEsri']

    data = {
        'geoid': geo_id,
        'geolevel': geo_level.value,
        'category': category,
    }

    existing_data = collection.find_one(data)

    # print("Storing census data into Mongo")

    try:
        if existing_data:
            print("Already stored missing geo")
        else:
            collection.insert_one(data)
    except:
        print("!!! ERROR storing data to Mongo!!!")



def test_mongo(data_dict):
    client = connect_to_client(prod_env="prod")
    db = client['scopeout']

    collection = db['test']

    print("Storing census data into Mongo")
    collection.insert_one(data_dict)

    print("Successfully stored batch into Mongo")

    return True

