from pymongo import MongoClient
import os
from dotenv import load_dotenv
from pandas import DataFrame
import json
from bson import json_util

def connect_to_client():
    load_dotenv()
    host = os.getenv("MONGO_HOST")
    database = os.getenv("MONGO_DATABASE")
    un = os.getenv("MONGO_USERNAME")
    pw = os.getenv("MONGO_PASSWORD")

    connection_string = 'mongodb+srv://{}:{}@{}/{}?retryWrites=true&w=majority'\
        .format(un, pw, host, database)


    client = MongoClient(connection_string)

    return client


def query_collection(collection_name, collection_filter):
    client = connect_to_client()
    db = client['scopeout']
    collection = db[collection_name]

    data = collection.find(collection_filter)
    data = list(data)
    df = DataFrame(data)

    return df


def query_geography(geo_level):
    '''
    :param geo_level:
    :return: all geographies
    '''
    client = connect_to_client()
    db = client['scopeout']
    collection = None

    if geo_level == 'cbsa':
        collection = db['Cbsa']
    elif geo_level == 'state':
        collection = db['State']
    elif geo_level == 'county':
        collection = db['County']
    elif geo_level == 'tracts':
        collection = db['EsriTracts']

    data = list(collection.find())
    df = DataFrame(data)
    df = df.drop(columns=['_id'])

    return df


def store_census_data(data_dict):
    client = connect_to_client()
    db = client['scopeout']

    collection = db['CensusData']

    for k, results in data_dict.items():
        collection_filter = {
            'geoid': results['geoid'],
            'geolevel': results['geolevel'],
        }

        existing_data = collection.find_one(collection_filter)

        print("Storing census data into Mongo")

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

        print("Successfully stored batch into Mongo")


def store_missing_geo(geo_id, geo_level, category):
    print("!!! FOUND MISSING GEO !!! - geo_id: {}. geo_level: {}. category: {} ".format(geo_id, geo_level, category))

    client = connect_to_client()
    db = client['scopeout']

    collection = db['CensusDataMissingInEsri']

    data = {
        'geoid': geo_id,
        'geolevel': geo_level,
        'category': category,
    }

    existing_data = collection.find_one(data)

    print("Storing census data into Mongo")

    try:
        if existing_data:
            print("Already stored missing geo")
        else:
            collection.insert_one(data)
    except:
        print("!!! ERROR storing data to Mongo!!!")

    print("Successfully stored batch into Mongo")

