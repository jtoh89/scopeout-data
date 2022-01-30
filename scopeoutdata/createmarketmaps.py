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
    for i, data in cbsa_zip_df.iterrows():
        marketname = top_200_msas[top_200_msas['cbsacode'] == data.cbsacode]["cbsaname"].iloc[0]
        marketmap_data = marketmap.MarketMap()
        marketmap_data.cbsacode = data.cbsacode
        marketmap_data.urlslug = create_url_slug( marketname=marketname, cbsacode=data.cbsacode)


        for zipcode_data in data.zipcodes:
            geometry = []

            if not list_length_okay(zipcode_data['geometry'], 1):
                print('')
            else:
                for latlng in zipcode_data['geometry'][0]:
                    geometry.append({
                        "lng": latlng[0],
                        "lat": latlng[1],
                    })

            marketmap_data.zipprofiles.append({
                'zipcode': data.zipcodes[0]['zipcode'],
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