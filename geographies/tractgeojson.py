import sys
import geopandas
import os
from lookups import STATES, SCOPEOUT_MARKET_LIST
from database import mongoclient
from enums import ProductionEnvironment
from models.geojson import GeoJson, GeoJsonFeature, GeoJsonGeometry


def store_tract_geojson_for_cbsacode():
    currpath = os.path.dirname(os.path.abspath(__file__))
    rootpath = os.path.dirname(os.path.abspath(currpath))

    for stateid in STATES:
        cbsa_counties = mongoclient.query_collection(database_name="Geographies",
                                                        collection_name="CountyByCbsa",
                                                        collection_filter={'stateid': stateid},
                                                        prod_env=ProductionEnvironment.GEO_ONLY)

        file_dir = '/tractsgeojson/tl_2020_{}_tract/tl_2020_{}_tract.shp'
        file_dir = file_dir.format(stateid, stateid)
        df = geopandas.read_file(rootpath + file_dir)
        df = df.to_crs("EPSG:4326")

        tract_geojson_list = []
        tract_geojson_dict = {}
        for i, row in df.iterrows():
            countyfullcode = row['GEOID'][:5]

            if countyfullcode not in list(cbsa_counties['countyfullcode']):
                continue

            countyinfo = cbsa_counties[cbsa_counties['countyfullcode'] == countyfullcode].iloc[0].to_dict()
            cbsacode = countyinfo['cbsacode']

            if cbsacode not in SCOPEOUT_MARKET_LIST:
                continue

            tractinfo = {
                'geoid': row['GEOID'],
                "geometry": row['geometry']
            }

            if cbsacode in tract_geojson_dict.keys():
                tract_geojson_dict[cbsacode]['tractinfo'].append(tractinfo)
            else:
                tract_geojson_dict[cbsacode] = {
                    "cbsaname": countyinfo['cbsaname'],
                    "tractinfo": [tractinfo],
                }

        if len(tract_geojson_dict) == 0:
            continue

        cbsa_list = list(tract_geojson_dict.keys())

        for cbsacode in cbsa_list:
            if cbsa_exists_for_state(cbsacode, stateid):
                print("Skipping stateid:{} cbsacode: {}. Already processed".format(stateid, cbsacode))
                continue

            iterate_dict = tract_geojson_dict[cbsacode]

            geojson_model = GeoJson()
            geojson_model.name = iterate_dict['cbsaname']

            for tract_data in iterate_dict['tractinfo']:
                geojson = geopandas.GeoSeries([tract_data['geometry']]).__geo_interface__

                geojson_feature = GeoJsonFeature()
                geojson_feature.id = tract_data['geoid']

                if len(geojson['features']) > 1:
                    print('!!! Why is there more than 1 feature !!!')
                    sys.exit()

                geo_json_geometry = GeoJsonGeometry()
                geo_json_geometry.coordinates = geojson['features'][0]['geometry']['coordinates']
                geo_json_geometry.type = geojson['features'][0]['geometry']['type']

                geojson_feature.geometry = geo_json_geometry.__dict__
                geojson_model.features.append(geojson_feature.__dict__)

            cbsa_ziplist = {
                "cbsacode": cbsacode,
                "geojson": geojson_model.__dict__
            }

            mongoclient.insert_list_mongo(list_data=[cbsa_ziplist],
                                          dbname='ScopeOut',
                                          collection_name='GeojsonTractsBySOMarkets',
                                          prod_env=ProductionEnvironment.GEO_ONLY,
                                          collection_update_existing={"cbsacode": cbsacode})

            collection_add_finished_run = {
                'hosttype': 'Geography',
                'dbname': 'ScopeOut',
                'tablename': 'GeojsonTractsBySOMarkets',
                'cbsacode': cbsacode,
                'stateid': stateid
            }

            print('Successfully stored tract geojson for cbsacode: {}'.format(cbsacode))
            mongoclient.add_finished_run(collection_add_finished_run)


def cbsa_exists_for_state(cbsacode, stateid):
    existing_cbsa_geojson = mongoclient.query_collection(database_name="CensusDataInfo",
                                                     collection_name="FinishedRuns",
                                                     collection_filter={"tablename":"GeojsonTractsBySOMarkets", "cbsacode": cbsacode},
                                                     prod_env=ProductionEnvironment.QA)

    if len(existing_cbsa_geojson) == 0:
        return False

    existing_states = list(existing_cbsa_geojson['stateid'])

    if stateid in existing_states:
        return True
