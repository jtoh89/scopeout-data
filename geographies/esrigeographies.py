import os
from dotenv import load_dotenv
import sys
from database import mongoclient
import pandas as pd
from enums import ProductionEnvironment, GeoLevels, GeoIdField
from utils.utils import list_length_okay
import requests as r
from shapely.geometry import shape, Point, Polygon, mapping, MultiPolygon
from shapely.ops import split
import geopandas
from census.censusdata import STATES1, STATES2


CBSA_LIST = ["31080"]

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

        county_tract_list = get_tract_info(geoid=countyid, auth_token=auth_token, geo_layer="US.Counties", geo_level=GeoLevels.TRACT)

        if len(county_tract_list) < 1:
            print("No tracts found. Skipping countyid: {}".format(countyid))
            continue

        mongoclient.insert_list_mongo(list_data=county_tract_list,
                                      dbname='Geographies',
                                      collection_name='EsriTractLookup',
                                      prod_env=ProductionEnvironment.GEO_ONLY,
                                      collection_update_existing={"countyfullcode": countyid})

        count += 1

def dump_tract_spatial_data_by_cbsa():
    """
    Iterates through list of CBSA Ids. Queries Esri Standard Geography for polygon rings and stores into mongo
    :return:
    """
    auth_token = esri_auth()

    for cbsacode in CBSA_LIST:
        cbsa_tract_list = get_spatial_data_for_tract(geoid=cbsacode, auth_token=auth_token, geo_layer="US.Cbsa", geo_level=GeoLevels.TRACT)
        mongoclient.insert_list_mongo(list_data=cbsa_tract_list,
                                      dbname='Geographies',
                                      collection_name='EsriTractsByCbsa',
                                      prod_env=ProductionEnvironment.GEO_ONLY,
                                      collection_update_existing={"cbsacode": cbsacode})

def get_spatial_data_for_tract(geoid, auth_token, geo_layer, geo_level):
    geo_features = esri_standard_geography_api(geoid=geoid, auth_token=auth_token, geo_layer=geo_layer, geo_level=geo_level)

    tract_spatial_list = []

    for geo_dict in geo_features:
        tract_id = geo_dict['attributes']['AreaID']
        county_full_code = tract_id[0:5]
        state_id = tract_id[0:2]

        if len(geo_dict['geometry']['rings']) > 1:
            multi_polygon_list = []
            for i, lat_lng_array in enumerate(geo_dict['geometry']['rings']):
                polygon_lat_lng = create_polygon_lat_lng_values(lat_lng_array)
                multi_polygon_list.append(polygon_lat_lng)

            if geo_layer == "US.Counties":
                tract_spatial_list.append({
                    "tractcode": tract_id,
                    "{}".format(GeoIdField.COUNTY.value): county_full_code,
                    "{}".format(GeoIdField.STATE.value): state_id,
                    "geometry": multi_polygon_list
                })
            else:
                tract_spatial_list.append({
                    "tractcode": tract_id,
                    "{}".format(GeoIdField.CBSA.value): geoid,
                    "{}".format(GeoIdField.COUNTY.value): county_full_code,
                    "{}".format(GeoIdField.STATE.value): state_id,
                    "geometry": multi_polygon_list
                })
        else:
            polygon_lat_lng = create_polygon_lat_lng_values(geo_dict['geometry']['rings'][0])

            if geo_layer == "US.Counties":
                tract_spatial_list.append({
                    "tractcode": tract_id,
                    "{}".format(GeoIdField.COUNTY.value): county_full_code,
                    "{}".format(GeoIdField.STATE.value): state_id,
                    "geometry": [polygon_lat_lng]
                })
            else:
                tract_spatial_list.append({
                    "tractcode": tract_id,
                    "{}".format(GeoIdField.CBSA.value): geoid,
                    "{}".format(GeoIdField.COUNTY.value): county_full_code,
                    "{}".format(GeoIdField.STATE.value): state_id,
                    "geometry": [polygon_lat_lng]
                })

    return tract_spatial_list

def get_tract_info(geoid, auth_token, geo_layer, geo_level):
    geo_features = esri_standard_geography_api(geoid=geoid,
                                               auth_token=auth_token,
                                               geo_layer=geo_layer,
                                               geo_level=geo_level,
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
    auth_token = esri_auth()

    for cbsacode in CBSA_LIST:
        cbsa_ziplist = get_zip_list_for_cbsacode(cbsacode=cbsacode, auth_token=auth_token)
        mongoclient.insert_list_mongo(list_data=[cbsa_ziplist],
                                      dbname='Geographies',
                                      collection_name='EsriZipcodes',
                                      prod_env=ProductionEnvironment.GEO_ONLY,
                                      collection_update_existing={"cbsacode": cbsacode})




def get_zip_list_for_cbsacode(cbsacode, auth_token):
    geo_features = esri_standard_geography_api(geoid=cbsacode, auth_token=auth_token, geo_layer="US.CBSA", geo_level=GeoLevels.ZIPCODE)

    zipcode_list = []

    for geo_dict in geo_features:
        zipcode_test = geo_dict['attributes']['AreaID']
        # if zipcode_test != "92683":
        #     continue

        geometry = []
        if len(geo_dict['geometry']['rings']) > 1:
            parentPolygon = ""
            diff_poly = ""
            subPolygonList = []
            for i, lat_lng_array in enumerate(geo_dict['geometry']['rings']):
                geometry.append(lat_lng_array)
                continue

                if parentPolygon == "":
                    parentPolygon = Polygon(lat_lng_array)
                    # s = geopandas.GeoSeries([parentPolygon])
                    continue

                subPolygon = Polygon(lat_lng_array)

                if (parentPolygon.intersects(subPolygon) is True):
                    subPolygonList.append(subPolygon)
                    # s2 = geopandas.GeoSeries([subPolygon])

                    # if i == 1:
                    #     subPolygonList.append(subPolygon)
                    #     diff_poly = s.difference(s2, align=True)
                    # elif i == 2:
                    #     subPolygonList.append(subPolygon)
                    #     diff_poly = diff_poly.difference(s2, align=True)

                # else:
                #     outmulti.append(parentPolygon)



        else:
            geometry = geo_dict['geometry']['rings']


            # difference_poly = parentPolygon.difference(MultiPolygon(subPolygonList))
            # poly_mapped = mapping(difference_poly)
            # geo_list = []
            # for i, polyTupleList in enumerate(poly_mapped['coordinates']):
            #     for polytuple in polyTupleList:
            #         geo_list.append(polytuple)
            # geometry.append(geo_list)


            # poly_mapped = mapping(diff_poly[0])
            #
            # geo_list = []
            # for i, polyTupleList in enumerate(poly_mapped['coordinates']):
            #     for polytuple in polyTupleList:
            #         geo_list.append(polytuple)
            # geometry.append(geo_list)

        zipcode_list.append(
            {
                "zipcode":geo_dict['attributes']['AreaID'],
                # "geometry": geo_dict['geometry']['rings']
                "geometry": geometry
            }
        )

    cbsa_ziplist = {
        "cbsacode": cbsacode,
        "zipcodes": zipcode_list
    }

    return cbsa_ziplist


def esri_standard_geography_api(geoid, auth_token, geo_layer, geo_level, return_geography=True):
    # geolayer = ""
    subgeolayer = ""
    if geo_level == GeoLevels.ZIPCODE:
        subgeolayer = "US.ZIP5"
    elif geo_level == GeoLevels.COUNTY:
        subgeolayer = "US.Counties"
    elif geo_level == GeoLevels.TRACT:
        subgeolayer = "US.Tracts"

    url = "https://geoenrich.arcgis.com/arcgis/rest/services/World/geoenrichmentserver/StandardGeographyQuery/execute"

    params = {
        "sourceCountry": "US",
        "geographylayers": [geo_layer],
        "geographyids": [geoid],
        "returnGeometry": return_geography,
        "returnSubGeographyLayer": True,
        "subGeographyLayer": subgeolayer,
        "generalizationLevel": 0,
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


def dump_zipcodes_by_scopeout_markets(batch_size=50):
    """
    Stores data to EsriZipcodesByCbsa
    """
    auth_token = esri_auth()

    scopeout_markets = list(mongoclient.query_collection(database_name="ScopeOut",
                                                collection_name="ScopeOutMarkets",
                                                collection_filter={},
                                                prod_env=ProductionEnvironment.GEO_ONLY)['cbsacode'])


    existing_data = list(mongoclient.query_collection(database_name="Geographies",
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

        geo_features = esri_standard_geography_api(geoid=cbsacode, auth_token=auth_token, geo_layer="US.CBSA", geo_level=GeoLevels.ZIPCODE)

        for geo_dict in geo_features:
            zipcode = geo_dict['attributes']['AreaID']
            zipcode_list.append(zipcode)

        cbsa_zipcode_lookup.append({
            "cbsacode": cbsacode,
            "zipcodes": zipcode_list
        })

        print("Writing zipcodes for cbsa: ", cbsacode)
        mongoclient.insert_list_mongo(list_data=cbsa_zipcode_lookup,
                                      dbname='Geographies',
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