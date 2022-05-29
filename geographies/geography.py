import os
import sys
from database import mongoclient
import time
import requests as r
import pandas as pd
from enums import ProductionEnvironment, GeoLevels
from database import mongoclient
import json
import geojson
from models.geojson import GeoJson, GeoJsonFeature, GeoJsonGeometry
from geographies import esrigeographies
from utils.production import create_url_slug
from lookups import STATES
from census.censusdata import census_api

def dump_zipcode_geojson_by_scopeout_markets():
    currpath = os.path.dirname(os.path.abspath(__file__))
    rootpath = os.path.dirname(os.path.abspath(currpath))

    scopeout_markets = mongoclient.query_collection(database_name="ScopeOut",
                                                         collection_name="ScopeOutMarkets",
                                                         collection_filter={},
                                                         prod_env=ProductionEnvironment.GEO_ONLY)

    for i, cbsa in scopeout_markets.iterrows():
        zipcode_list = []
        cbsacode = cbsa['cbsacode']

        store_zip_geojson_for_cbsacode(cbsacode=cbsacode)

def store_zip_geojson_for_cbsacode(cbsacode):
    cbsa_market = mongoclient.query_collection(database_name="Geographies",
                                                    collection_name="Cbsa",
                                                    collection_filter={"cbsacode": cbsacode},
                                                    prod_env=ProductionEnvironment.GEO_ONLY)

    zipcodes_for_cbsa = mongoclient.query_collection(database_name="ScopeOut",
                                                     collection_name="EsriZipcodesBySOMarkets",
                                                     collection_filter={"cbsacode": cbsacode},
                                                     prod_env=ProductionEnvironment.GEO_ONLY).iloc[0].zipcodes

    counties_in_cbsa = cbsa_market.iloc[0].counties
    cbsa_name = cbsa_market.iloc[0].cbsaname
    states_in_cbsa = []

    for county_info in counties_in_cbsa:
        # state_info = county_info['stateinfo']
        # states_in_cbsa.append(state_info["stateabbreviation"])
        states_in_cbsa.append(county_info["stateid"])

    unique_states = list(set(states_in_cbsa))

    geojson_model = GeoJson()
    geojson_model.name = cbsa_name

    directory = 'zipcodegeojson'
    for foldername in os.listdir(directory):
        if foldername not in unique_states:
            continue

        for geojsonfile in os.listdir(os.path.join(directory, foldername)):
            cwd = os.getcwd()
            with open(cwd + "/" + directory + "/" + foldername + "/" + geojsonfile, 'r') as gfile:
                gj = geojson.load(gfile)
                zipcode_data = gj['features'][1]

                zipcode = zipcode_data['properties']['postal-code']

                if zipcode not in zipcodes_for_cbsa:
                    continue

                geojson_feature = GeoJsonFeature()
                geojson_feature.id = zipcode

                geo_json_geometry = GeoJsonGeometry()
                geo_json_geometry.coordinates = zipcode_data['geometry']['coordinates']
                geo_json_geometry.type = zipcode_data['geometry']['type']

                geojson_feature.geometry = geo_json_geometry.__dict__
                geojson_model.features.append(geojson_feature.__dict__)


    cbsa_ziplist = {
        "cbsacode": cbsacode,
        "geojson": geojson_model.__dict__
    }

    mongoclient.insert_list_mongo(list_data=[cbsa_ziplist],
                                  dbname='ScopeOut',
                                  collection_name='GeojsonZipcodesBySOMarkets',
                                  prod_env=ProductionEnvironment.GEO_ONLY,
                                  collection_update_existing={"cbsacode": cbsacode})

    collection_add_finished_run = {
        'hosttype': 'Geography',
        'dbname': 'ScopeOut',
        'tablename': 'GeojsonZipcodesBySOMarkets',
        'cbsacode': cbsacode,
    }

    print('Successfully stored zipcode geojson for cbsacode: {}'.format(cbsacode))
    mongoclient.add_finished_run(collection_add_finished_run)

def DEPRECATED_dump_all_geographies():
    """
    Stores data to State, County, Cbsa tables
    """
    currpath = os.path.dirname(os.path.abspath(__file__))
    rootpath = os.path.dirname(os.path.abspath(currpath))

    state_dict = {}
    county_dict = {}

    for geo_level in ['state', 'county']:
        if geo_level == 'state' or geo_level == 'county':
            file_dir = '/files/statecountyfips.csv'
            geo_df = pd.read_csv(rootpath + file_dir)

            geo_df = geo_df.rename(columns={'State Code (FIPS)':'stateid',
                                            'County Code (FIPS)': 'countyid',
                                            'Area Name (including legal/statistical area description)':'areaname',
                                            'Abbreviation':'abbreviation'})

            geo_df['stateid'] = geo_df['stateid'].apply(lambda x: str(x).zfill(2))
            geo_df['countyid'] = geo_df['countyid'].apply(lambda x: str(x).zfill(3))

            geo_df = geo_df[geo_df['stateid'] != '72']

            for i, row in geo_df.iterrows():
                if geo_level == 'state' and row['Summary Level'] == 40:
                    state_dict[row['stateid']] = {
                        'fipsstatecode': row['stateid'],
                        'statename': row['areaname'],
                        'stateabbreviation': row['abbreviation'],
                    }
                elif geo_level == 'county' and row['Summary Level'] == 50:
                    countyfullcode = row['stateid'] + row['countyid']

                    county_dict[countyfullcode] = {
                        'countyfullcode': countyfullcode,
                        'fipscountycode': row['countyid'],
                        'countyname': row['areaname'],
                        'stateinfo': state_dict[row['stateid']],
                    }


    state_list = []
    county_list = []
    for _, v in state_dict.items():
        state_list.append(v)

    mongoclient.insert_list_mongo(list_data=state_list,
                                  dbname='Geographies',
                                  collection_name='State',
                                  prod_env=ProductionEnvironment.GEO_ONLY,
                                  collection_update_existing={})

    for _, v in county_dict.items():
        county_list.append(v)

    mongoclient.insert_list_mongo(list_data=county_list,
                                  dbname='Geographies',
                                  collection_name='County',
                                  prod_env=ProductionEnvironment.GEO_ONLY,
                                  collection_update_existing={})


    file_dir = '/files/cbsafips.csv'
    cbsa_df = pd.read_csv(rootpath + file_dir)

    cbsa_dict = {}

    for i, row in cbsa_df.iterrows():

        if row['cbsacode'] == row['cbsacode']:
            cbsacode = str(int(row['cbsacode']))
            stateid = str(int(row['fipsstatecode'])).zfill(2)

            if stateid == '72':
                continue


            fipscountycode = str(int(row['fipscountycode'])).zfill(3)
            countyfullcode = stateid + fipscountycode

            if countyfullcode in county_dict.keys():
                county_data = county_dict[countyfullcode]
            else:
                print('Skipping county: ', row['countycountyequivalent'])
                continue

            if cbsacode in cbsa_dict.keys():
                cbsa_dict[cbsacode]['counties'].append(county_data)
            else:
                coords = esrigeographies.esri_get_centroids(geoid=cbsacode, geo_level=GeoLevels.CBSA)

                cbsa_dict[cbsacode] = {
                    'cbsacode': cbsacode,
                    'cbsaname': row['cbsatitle'].replace('--','-'),
                    'counties': [county_data],
                    'lon_x': coords['lon_x'],
                    'lat_y': coords['lat_y']
                }

    cbsa_list = []

    for _, v in cbsa_dict.items():
        cbsa_list.append(v)

    mongoclient.insert_list_mongo(list_data=cbsa_list,
                                  dbname='Geographies',
                                  collection_name='Cbsa',
                                  prod_env=ProductionEnvironment.GEO_ONLY,
                                  collection_update_existing={})



    print('done')

def dump_county_by_cbsa_lookup():
    '''
    Stores data to CountyByCbsa table. Function creates lookup for counties to cbsa ids
    '''
    cbsa_data = mongoclient.query_collection(database_name="Geographies",
                                 collection_name="Cbsa",
                                 collection_filter={},
                                 prod_env=ProductionEnvironment.GEO_ONLY)

    scopeout_markets = mongoclient.query_collection(database_name="ScopeOut",
                                             collection_name="ScopeOutMarkets",
                                             collection_filter={},
                                             prod_env=ProductionEnvironment.GEO_ONLY)
    scopeout_markets = list(scopeout_markets['cbsacode'])

    counties_to_cbsa = []
    for i, cbsa in cbsa_data.iterrows():
        cbsaid = cbsa['cbsacode']
        cbsaname = cbsa['cbsaname']

        in_scopeout_market = False
        urlslug = 'default'
        if cbsaid in scopeout_markets:
            in_scopeout_market = True
            urlslug = create_url_slug(cbsaid, cbsaname)


        for county in cbsa['counties']:
            # stateid = county['stateinfo']['fipsstatecode']
            stateid = county['stateid']
            counties_to_cbsa.append({
                'countyfullcode': county['countyfullcode'],
                'cbsacode': cbsaid,
                'cbsaname': cbsaname,
                'stateid': stateid,
                'inscopeoutmarket': in_scopeout_market,
                'urlslug': urlslug,
            })

    mongoclient.insert_list_mongo(list_data=counties_to_cbsa,
                      dbname='Geographies',
                      collection_name='CountyByCbsa',
                      prod_env=ProductionEnvironment.GEO_ONLY,
                      collection_update_existing={})

def dump_zillow_cbsa_mapping():
    '''
    Function dumps mappings for zillow metro ids to cbsa ids
    :return:
    '''
    currpath = os.path.dirname(os.path.abspath(__file__))
    rootpath = os.path.dirname(os.path.abspath(currpath))

    final_df = pd.DataFrame()

    # get this csv from google sheet
    file_dir = '/files/Zillow CBSA ID Map.csv'
    df = pd.read_csv(rootpath + file_dir)
    df = df[df['cbsamissinginzillow'] != 'Yes']
    df[['zillowmsaid','cbsacode']] = df[['zillowmsaid','cbsacode']].fillna(0).astype(int)

    zillow_cbsa_mapping_list = []

    length_zillow = len(df['zillowmsaid'])
    length_zillow_unique = len(df['zillowmsaid'].drop_duplicates())
    length_cbsa = len(df['cbsacode'])
    length_cbsa_unique = len(df['cbsacode'].drop_duplicates())

    if length_zillow != length_zillow_unique and length_cbsa != length_cbsa_unique:
        print('Why is there a mismatch in number ')
        sys.exit()

    for i, row in df.iterrows():
        zillow_cbsa_mapping_list.append({
            'zillowmsaid': row.zillowmsaid,
            'zillowmsaname': row.zillowmetroname,
            'cbsacode': row.cbsacode,
            'cbsaname': row.cbsaname
        })


    mongoclient.insert_list_mongo(list_data=zillow_cbsa_mapping_list,
                                  dbname='Geographies',
                                  collection_name='ZillowCbsaMapping',
                                  prod_env=ProductionEnvironment.GEO_ONLY)

def store_tract_lookups():
    for stateid in STATES:

        query_url = "https://api.census.gov/data/2020/acs/acs5?get=NAME&for=tract:*&in=state:{}%20county:*".format(stateid)
        df = census_api(query_url)

        insert_list = []
        for i, row in df.iterrows():
            stateid = row['state']
            countyid = row['county']
            tractcode = stateid + countyid + row['tract']

            insert_list.append({
                "tractcode": tractcode,
                "countyfullcode": stateid + countyid,
                "fipsstatecode": stateid
            })

        mongoclient.insert_list_mongo(list_data=insert_list,
                                      dbname='Geographies',
                                      collection_name='TractLookup',
                                      prod_env=ProductionEnvironment.GEO_ONLY)

def dump_state_geography():
    """
    Stores data to State tables
    """
    currpath = os.path.dirname(os.path.abspath(__file__))
    rootpath = os.path.dirname(os.path.abspath(currpath))

    file_dir = '/files/statecountyfips.csv'
    geo_df = pd.read_csv(rootpath + file_dir)

    geo_df = geo_df.rename(columns={'State Code (FIPS)': 'stateid',
                                    'County Code (FIPS)': 'countyid',
                                    'Area Name (including legal/statistical area description)': 'areaname',
                                    'Abbreviation': 'abbreviation'})

    geo_df['stateid'] = geo_df['stateid'].apply(lambda x: str(x).zfill(2))

    geo_df = geo_df[geo_df['stateid'] != '72']

    state_list = []
    for i, row in geo_df.iterrows():
        if row['Summary Level'] == 40:
            state_list.append({
                'fipsstatecode': row['stateid'],
                'statename': row['areaname'],
                'stateabbreviation': row['abbreviation'],
            })

    mongoclient.insert_list_mongo(list_data=state_list,
                                  dbname='Geographies',
                                  collection_name='State',
                                  prod_env=ProductionEnvironment.GEO_ONLY,
                                  collection_update_existing={})

    print('done')

def dump_county_geography():
    """
    Stores data to County tables
    """


    states = mongoclient.query_collection(database_name="Geographies",
                                               collection_name="State",
                                               collection_filter={},
                                               prod_env=ProductionEnvironment.GEO_ONLY)


    url = "https://api.census.gov/data/2020/acs/acs5/subject?get=NAME&for=county:*&in=state:*"

    counties_df = census_api(url)

    county_list = []
    for i, row in counties_df.iterrows():
        if row['state'] == "72":
            continue

        state_info = states[states['fipsstatecode'] == row['state']].iloc[0].to_dict()

        county_name = row['NAME'].split(",")[0]

        county_list.append({
            'countyfullcode': row['state'] + row['county'],
            'fipscountycode': row['county'],
            'countyname': county_name,
            'stateid': row['state'],
            'stateabbreviation': state_info['stateabbreviation'],
        })

    mongoclient.insert_list_mongo(list_data=county_list,
                                  dbname='Geographies',
                                  collection_name='County',
                                  prod_env=ProductionEnvironment.GEO_ONLY,
                                  collection_update_existing={})


    print('done')

def dump_cbsa_geography():
    """
    Stores data to Cbsa tables
    """
    counties_df = mongoclient.query_collection(database_name="Geographies",
                                                 collection_name="County",
                                                 collection_filter={},
                                                 prod_env=ProductionEnvironment.GEO_ONLY)

    currpath = os.path.dirname(os.path.abspath(__file__))
    rootpath = os.path.dirname(os.path.abspath(currpath))

    file_dir = '/files/cbsafips.csv'
    cbsa_df = pd.read_csv(rootpath + file_dir)

    cbsa_dict = {}

    for i, row in cbsa_df.iterrows():
        if row['cbsacode'] == row['cbsacode']:
            cbsacode = str(int(row['cbsacode']))
            stateid = str(int(row['fipsstatecode'])).zfill(2)

            if stateid == '72':
                continue

            fipscountycode = str(int(row['fipscountycode'])).zfill(3)
            countyfullcode = stateid + fipscountycode

            if countyfullcode in list(counties_df['countyfullcode']):
                county_data = counties_df[counties_df['countyfullcode'] == countyfullcode].iloc[0].to_dict()
                del county_data['_id']
            else:
                print('Skipping county: ', row['countycountyequivalent'])
                continue

            if cbsacode in cbsa_dict.keys():
                cbsa_dict[cbsacode]['counties'].append(county_data)
            else:
                coords = esrigeographies.esri_get_centroids(geoid=cbsacode, geo_level=GeoLevels.CBSA)

                cbsa_dict[cbsacode] = {
                    'cbsacode': cbsacode,
                    'cbsaname': row['cbsatitle'].replace('--','-'),
                    'counties': [county_data],
                    'lon_x': coords['lon_x'],
                    'lat_y': coords['lat_y']
                }

    cbsa_list = []

    for _, v in cbsa_dict.items():
        cbsa_list.append(v)

    mongoclient.insert_list_mongo(list_data=cbsa_list,
                                  dbname='Geographies',
                                  collection_name='Cbsa',
                                  prod_env=ProductionEnvironment.GEO_ONLY,
                                  collection_update_existing={})



    print('done')

