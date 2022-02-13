from database import mongoclient
from models import marketmap
from enums import ProductionEnvironment
from utils.utils import list_length_okay, create_url_slug

def generate_market_maps():
    top_200_msas = mongoclient.query_collection(database_name="Geographies",
                                              collection_name="Top200Msa",
                                              collection_filter={},

                                              prod_env=ProductionEnvironment.GEO_ONLY)

    cbsa_zip_df = mongoclient.query_collection(database_name="Geographies",
                                                collection_name="EsriZipcodes",
                                                collection_filter={},
                                                prod_env=ProductionEnvironment.GEO_ONLY)

    insert_list = []
    for i, cbsa_data in cbsa_zip_df.iterrows():
        marketname = top_200_msas[top_200_msas['cbsacode'] == cbsa_data.cbsacode]["cbsaname"].iloc[0]
        marketmap_data = marketmap.MarketMap()
        marketmap_data.cbsacode = cbsa_data.cbsacode
        marketmap_data.urlslug = create_url_slug(marketname=marketname, cbsacode=cbsa_data.cbsacode)
        # marketmap_data.urlslug += '-test'

        for zipcode_data in cbsa_data.zipcodes:
            geometry = []

            if zipcode_data['zipcode'] != "92683" :
                continue
            else:
                print('')

            if not list_length_okay(zipcode_data['geometry'], 1):
                print('zipcode geo length >1: ', zipcode_data['zipcode'])
                for zipcode_subgeo in zipcode_data['geometry']:
                    # geometry.append({
                    #     "lng": zipcode_subgeo[0],
                    #     "lat": zipcode_subgeo[1],
                    # })

                    add_list = []
                    for zipcode_subgeo_latlng in zipcode_subgeo:
                        add_list.append({
                            "lng": zipcode_subgeo_latlng[0],
                            "lat": zipcode_subgeo_latlng[1],
                        })

                    geometry.append(add_list)
            else:
                add_list = []
                for latlng in zipcode_data['geometry'][0]:
                    add_list.append({
                        "lng": latlng[0],
                        "lat": latlng[1],
                    })

                geometry = [add_list]

            marketmap_data.zipprofiles.append({
                'zipcode': zipcode_data['zipcode'],
                'geometry': geometry,
                'data': {
                    'medianhouseholdincome': 80000
                }
            })


        insert_list.append(marketmap_data.__dict__)


    mongoclient.insert_list_mongo(list_data=insert_list,
                                  dbname='ScopeOutMaps',
                                  collection_name='MarketMaps',
                                  prod_env=ProductionEnvironment.MARKET_MAPS,
                                  collection_update_existing={})