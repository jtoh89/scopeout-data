import os
from dotenv import load_dotenv
import sys
from database import mongoclient
import pandas as pd
from enums import ProductionEnvironment, GeoLevels
from utils.utils import list_length_okay
import requests as r



def esri_auth():
    load_dotenv()

    arcgis_clientid = os.getenv("ESRI_OAUTH_CLIENT_ID_JAYLEEONG0913")
    arcgis_clientsecret = os.getenv("ESRI_OAUTH_CLIENT_SECRET_JAYLEEONG0913")
    url = "https://www.arcgis.com/sharing/rest/oauth2/token"

    params = {
        "client_id":arcgis_clientid,
        "client_secret":arcgis_clientsecret,
        "grant_type":"client_credentials"
    }

    response = r.get(url=url, params=params)
    response = response.json()

    return response['access_token']


def dump_geographies_by_cbsa():
    auth_token = esri_auth()

    cbsa_list = ["31080"]

    for cbsacode in cbsa_list:
        cbsa_ziplist = get_zip_list_for_cbsacode(cbsacode=cbsacode, auth_token=auth_token)
        mongoclient.insert_list_mongo(list_data=[cbsa_ziplist],
                                      dbname='Geographies',
                                      collection_name='EsriZipcodes',
                                      prod_env=ProductionEnvironment.GEO_ONLY,
                                      collection_update_existing={"cbsacode": cbsacode})


def get_zip_list_for_cbsacode(cbsacode, auth_token):
    geo_features = esri_standard_geography_api(geoid=cbsacode, auth_token=auth_token, geo_level=GeoLevels.ZIPCODE)

    zipcode_list = []

    for geo_dict in geo_features:
        zipcode_list.append(
            {
                "zipcode":geo_dict['attributes']['AreaID'],
                "geometry": geo_dict['geometry']['rings']
            }
        )

    cbsa_ziplist = {
        "cbsacode": cbsacode,
        "zipcodes": zipcode_list
    }

    return cbsa_ziplist


def esri_standard_geography_api(geoid, auth_token, geo_level):
    geolayer = ""
    subgeolayer = ""
    if geo_level == GeoLevels.ZIPCODE:
        geolayer = "US.CBSA"
        subgeolayer = "US.ZIP5"

    url = "https://geoenrich.arcgis.com/arcgis/rest/services/World/geoenrichmentserver/StandardGeographyQuery/execute"

    params = {
        "sourceCountry": "US",
        "geographylayers": [geolayer],
        "geographyids": [geoid],
        "returnGeometry": True,
        "returnSubGeographyLayer": True,
        "subGeographyLayer": subgeolayer,
        "generalizationLevel": 6,
        "f": "pjson",
        "token": auth_token,
    }

    response = r.get(url=url, params=params)
    response = response.json()

    geo_features = response['results'][0]['value']['features']

    if not list_length_okay(geo_features, 5000):
        print("!!! Error: 5000 esri query limit hit!!!")
        sys.exit()

    return geo_features
