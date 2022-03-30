import os
from dotenv import load_dotenv
import sys
from database import mongoclient
import pandas as pd
from enums import ProductionEnvironment, GeoLevels, GeoIdField, GeoNameField
from utils.utils import list_length_okay
import requests as r
import geojson
from shapely.geometry import shape, Point, Polygon, mapping, MultiPolygon
from census.censusdata import STATES1, STATES2


CBSA_LIST = ["31080"]

def esri_auth():
    load_dotenv()

    arcgis_clientid = os.getenv("ESRI_OAUTH_CLIENT_ID")
    arcgis_clientsecret = os.getenv("ESRI_OAUTH_CLIENT_SECRET")
    url = "https://www.arcgis.com/sharing/rest/oauth2/token"

    params = {
        "client_id":arcgis_clientid,
        "client_secret":arcgis_clientsecret,
        "grant_type":"client_credentials"
    }

    response = r.get(url=url, params=params)
    response = response.json()

    return response['access_token']



def dump_tract_by_county(batch_size=100):
    """
    Iterates through list of CBSA Ids. Queries Esri Standard Geography for polygon rings and stores into mongo
    :return:
    """
    auth_token = esri_auth()

    existing_tract_counties = mongoclient.query_collection(database_name="Geographies",
                                            collection_name="EsriTractLookup",
                                            collection_filter={},
                                            prod_env=ProductionEnvironment.GEO_ONLY)

    if len(existing_tract_counties) == 0:
        existing_tract_counties = []
    else:
        existing_tract_counties = list(existing_tract_counties['countyfullcode'].drop_duplicates())

    counties = mongoclient.query_collection(database_name="Geographies",
                                            collection_name="County",
                                            collection_filter={},
                                            prod_env=ProductionEnvironment.GEO_ONLY)

    print("{} counties left to process".format(len(counties) - len(existing_tract_counties)))

    count = 0
    for countyid in counties['countyfullcode']:
        if countyid in existing_tract_counties:
            continue
        if count > batch_size:
            print("Finished batch.")
            break
        if count != 0 and count % 10 == 0:
            print("{} / {} counties processed so far".format(count, batch_size))

        county_tract_list = get_tract_info(geoid=countyid, auth_token=auth_token, geo_layer=GeoLevels.COUNTY, geo_level=GeoLevels.TRACT)

        if len(county_tract_list) < 1:
            print("No tracts found. Skipping countyid: {}".format(countyid))
            continue

        mongoclient.insert_list_mongo(list_data=county_tract_list,
                                      dbname='Geographies',
                                      collection_name='EsriTractLookup',
                                      prod_env=ProductionEnvironment.GEO_ONLY,
                                      collection_update_existing={"countyfullcode": countyid})

        count += 1

def dump_tract_geojson_for_scopeout_markets():
    """
    Iterates through list of CBSA Ids. Queries Esri Standard Geography for polygon rings and stores into mongo
    :return:
    """
    auth_token = esri_auth()

    scopeout_markets = mongoclient.query_collection(database_name="ScopeOut",
                                                    collection_name="ScopeOutMarkets",
                                                    collection_filter={},
                                                    prod_env=ProductionEnvironment.GEO_ONLY)


    collection_find_finished_runs = {
        'tablename': 'EsriTractsBySOMarkets'
    }
    finished_runs = mongoclient.get_finished_runs(collection_find_finished_runs)

    if len(finished_runs) > 0:
        finished_runs = list(finished_runs["cbsacode"])


    for cbsacode in list(scopeout_markets['cbsacode']):
        if cbsacode in finished_runs:
            print("Skipping cbsacode: ", cbsacode)
            continue

        cbsa_tract_list = get_geojson_for_tract(cbsacode=cbsacode, auth_token=auth_token, geo_layer=GeoLevels.COUNTY, geo_level=GeoLevels.TRACT)

        try:
            mongoclient.insert_list_mongo(list_data=cbsa_tract_list,
                                          dbname='ScopeOut',
                                          collection_name='EsriTractsBySOMarkets',
                                          prod_env=ProductionEnvironment.GEO_ONLY,
                                          collection_update_existing={"cbsacode": cbsacode})

            collection_add_finished_run = {
                'dbname': 'ScopeOut',
                'tablename': 'EsriTractsBySOMarkets',
                'cbsacode': cbsacode,
            }
            mongoclient.add_finished_run(collection_add_finished_run)
        except:
            print("Geojson insert failed")

def get_geojson_for_tract(cbsacode, auth_token, geo_layer, geo_level):
    if geo_layer != GeoLevels.COUNTY:
        print("!!! ERROR - get_geojson_for_tract only supports queries using county")
        sys.exit()

    geo_features = esri_standard_geography_api(geoid=cbsacode, auth_token=auth_token, main_geo_layer=GeoLevels.CBSA, return_geo_level=geo_level)

    tract_spatial_list = []

    for geo_dict in geo_features:
        tract_id = geo_dict['attributes']['AreaID']
        county_full_code = tract_id[0:5]
        state_id = tract_id[0:2]

        tract_spatial_list.append({
            "tractcode": tract_id,
            "{}".format(GeoIdField.CBSA.value): cbsacode,
            "{}".format(GeoIdField.COUNTY.value): county_full_code,
            "{}".format(GeoIdField.STATE.value): state_id,
            "rings": geo_dict['geometry']['rings']
        })

    return tract_spatial_list

def get_tract_info(geoid, auth_token, geo_layer, geo_level):
    geo_features = esri_standard_geography_api(geoid=geoid,
                                               auth_token=auth_token,
                                               main_geo_layer=geo_layer,
                                               return_geo_level=geo_level,
                                               return_geography=False)

    tract_spatial_list = []

    for geo_dict in geo_features:
        tract_id = geo_dict['attributes']['AreaID']
        county_full_code = tract_id[0:5]
        state_id = tract_id[0:2]

        if geo_layer == "US.Counties":
            tract_spatial_list.append({
                "tractcode": tract_id,
                "{}".format(GeoIdField.COUNTY.value): county_full_code,
                "{}".format(GeoIdField.STATE.value): state_id,
            })
        else:
            tract_spatial_list.append({
                "tractcode": tract_id,
                "{}".format(GeoIdField.CBSA.value): geoid,
                "{}".format(GeoIdField.COUNTY.value): county_full_code,
                "{}".format(GeoIdField.STATE.value): state_id,
            })


    return tract_spatial_list

def dump_zipcodes_spatial_by_cbsa():
    for cbsacode in CBSA_LIST:
        cbsa_ziplist = get_zip_list_for_cbsacode(cbsacode=cbsacode)
        mongoclient.insert_list_mongo(list_data=[cbsa_ziplist],
                                      dbname='ScopeOut',
                                      collection_name='GeoJsonZipcodesBySOMarkets',
                                      prod_env=ProductionEnvironment.GEO_ONLY,
                                      collection_update_existing={"cbsacode": cbsacode})




def get_zip_list_for_cbsacode(cbsacode):
    zipcode_list = []

    scopeout_markets = mongoclient.query_collection(database_name="Geographies",
                                                    collection_name="Cbsa",
                                                    collection_filter={"cbsacode": cbsacode},
                                                    prod_env=ProductionEnvironment.GEO_ONLY)

    zipcodes_for_cbsa = mongoclient.query_collection(database_name="ScopeOut",
                                                    collection_name="EsriZipcodesBySOMarkets",
                                                    collection_filter={"cbsacode": cbsacode},
                                                    prod_env=ProductionEnvironment.GEO_ONLY).iloc[0].zipcodes

    counties_in_cbsa = scopeout_markets.iloc[0].counties

    states_in_cbsa = []

    for county_info in counties_in_cbsa:
        state_info = county_info['stateinfo']
        states_in_cbsa.append(state_info["stateabbreviation"])

    unique_states = list(set(states_in_cbsa))

    directory = 'zipcodedata'

    for foldername in os.listdir(directory):
        if foldername not in unique_states:
            continue

        for geojsonfile in os.listdir(os.path.join(directory, foldername)):
            # f = os.path.join(directory, geojsonfile)
            cwd = os.getcwd()
            with open(cwd + "/" + directory + "/" + foldername + "/" + geojsonfile, 'r') as gfile:
                gj = geojson.load(gfile)
                zipcode_data = gj['features'][1]

                geometry = []
                zipcode = zipcode_data['properties']['postal-code']

                if zipcode not in zipcodes_for_cbsa:
                    continue




        cbsa_ziplist = {
            "cbsacode": "31080",
            "urlslug":"los-angeles-long-beach-anaheim-real-estate-market-trends",
            "zipprofiles": zipcode_list
        }

        mongoclient.insert_list_mongo(list_data=[cbsa_ziplist],
                             dbname='ScopeOutMaps',
                             collection_name='z',
                             prod_env=ProductionEnvironment.GEO_ONLY,
                             collection_update_existing={"cbsacode": "31080"})


    cbsa_ziplist = {
        "cbsacode": cbsacode,
        "zipcodes": zipcode_list
    }

    return cbsa_ziplist


def esri_standard_geography_api(geoid, auth_token, main_geo_layer, return_geo_level, return_geography=True):
    if main_geo_layer == GeoLevels.CBSA:
        maingeolayer = "US.CBSA"
    elif main_geo_layer == GeoLevels.ZIPCODE:
        maingeolayer = "US.ZIP5"
    elif main_geo_layer == GeoLevels.COUNTY:
        maingeolayer = "US.Counties"
    elif main_geo_layer == GeoLevels.TRACT:
        maingeolayer = "US.Tracts"
    else:
        print("!!! ERROR - main_geo_layer not defined")
        sys.exit()


    if return_geo_level == GeoLevels.ZIPCODE:
        returngeolayer = "US.ZIP5"
    elif return_geo_level == GeoLevels.COUNTY:
        returngeolayer = "US.Counties"
    elif return_geo_level == GeoLevels.TRACT:
        returngeolayer = "US.Tracts"
    else:
        print("!!! ERROR - return_geo_level not defined")
        sys.exit()

    url = "https://geoenrich.arcgis.com/arcgis/rest/services/World/geoenrichmentserver/StandardGeographyQuery/execute"

    params = {
        "sourceCountry": "US",
        "geographylayers": [maingeolayer],
        "geographyids": [geoid],
        "returnGeometry": return_geography,
        "returnSubGeographyLayer": True,
        "subGeographyLayer": returngeolayer,
        "generalizationLevel": 6,
        "f": "pjson",
        "token": auth_token,
    }

    response = r.get(url=url, params=params)
    response = response.json()

    geo_features = response['results'][0]['value']['features']

    if not list_length_okay(geo_features, 4999):
        print("!!! Error: 5000 esri query limit hit!!!")
        sys.exit()

    return geo_features


def dump_zipcodes_by_scopeout_markets(batch_size=50):
    """
    Stores data to EsriZipcodesByCbsa
    """
    auth_token = esri_auth()

    scopeout_markets = list(mongoclient.query_collection(database_name="ScopeOut",
                                                collection_name="ScopeOutMarkets",
                                                collection_filter={},
                                                prod_env=ProductionEnvironment.GEO_ONLY)['cbsacode'])


    existing_data = list(mongoclient.query_collection(database_name="ScopeOut",
                                                    collection_name="EsriZipcodesBySOMarkets",
                                                    collection_filter={},
                                                    prod_env=ProductionEnvironment.GEO_ONLY)['cbsacode'])
    count = 0
    for cbsacode in scopeout_markets:
        if cbsacode in existing_data:
            continue

        if count > batch_size:
            print("Finished batch")
            break

        cbsa_zipcode_lookup = []
        zipcode_list = []

        geo_features = esri_standard_geography_api(geoid=cbsacode, auth_token=auth_token, main_geo_layer=GeoLevels.CBSA, return_geo_level=GeoLevels.ZIPCODE)

        for geo_dict in geo_features:
            zipcode = geo_dict['attributes']['AreaID']
            zipcode_list.append(zipcode)

        cbsa_zipcode_lookup.append({
            "cbsacode": cbsacode,
            "zipcodes": zipcode_list
        })

        print("Writing zipcodes for cbsa: ", cbsacode)
        mongoclient.insert_list_mongo(list_data=cbsa_zipcode_lookup,
                                      dbname='ScopeOut',
                                      collection_name='EsriZipcodesBySOMarkets',
                                      prod_env=ProductionEnvironment.GEO_ONLY,
                                      collection_update_existing={"cbsacode": cbsacode})
        count += 1


def create_polygon_lat_lng_values(polygon_x_y):
    polygon_lat_lng = []

    for latlng in polygon_x_y:
        polygon_lat_lng.append({
            "lng": latlng[0],
            "lat": latlng[1],
        })

    return polygon_lat_lng

