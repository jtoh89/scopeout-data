from database import mongoclient
from models import marketmap
from models import tractmarketmaps
from enums import ProductionEnvironment
from utils.utils import list_length_okay, create_url_slug

TEST_CBSAID = "31080"

COLOR_LEVEL_0 = "#999999"
COLOR_LEVEL_1 = "#ff0000"
COLOR_LEVEL_2 = "#ff7f01"
COLOR_LEVEL_3 = "#ffff01"
COLOR_LEVEL_4 = "#00ff01"
COLOR_LEVEL_5 = "#004c00"

def generate_tract_maps():
    top_200_msas = mongoclient.query_collection(database_name="Geographies",
                                              collection_name="Top200Msa",
                                              collection_filter={},
                                              prod_env=ProductionEnvironment.GEO_ONLY)

    for cbsacode in [TEST_CBSAID]:

        cbsa_tracts_geo_df = mongoclient.query_collection(database_name="Geographies",
                                                    collection_name="EsriTracts",
                                                    collection_filter={"cbsacode": cbsacode},
                                                    prod_env=ProductionEnvironment.GEO_ONLY)

        tracts_data_df = mongoclient.query_collection(database_name="scopeout",
                                                    collection_name="neighborhoodprofiles",
                                                    collection_filter={"cbsacode": cbsacode},
                                                    prod_env=ProductionEnvironment.PRODUCTION)
        tracts_data2_df = mongoclient.query_collection(database_name="scopeout",
                                                    collection_name="neighborhoodprofiles",
                                                    collection_filter={"cbsacode": cbsacode},
                                                    prod_env=ProductionEnvironment.PRODUCTION2)
        tracts_data_df.append(tracts_data2_df)

        marketname = top_200_msas[top_200_msas['cbsacode'] == cbsacode]["cbsaname"].iloc[0]
        tract_map = tractmarketmaps.TractMarketMap()
        tract_map.cbsacode = cbsacode
        tract_map.urlslug = create_url_slug(marketname=marketname, cbsacode=cbsacode)

        for i, tract in cbsa_tracts_geo_df.iterrows():
            tract_map_data = tractmarketmaps.TractMarketMapData()
            tract_map_data.geoid = tract.tractcode
            tract_map_data.geometry = tract.geometry

            tract_data = tracts_data_df[tracts_data_df['geoid'] == tract.tractcode]

            if len(tract_data) == 0:
                print('!!! WARNING - Could not find matching tract for tractid: ', tract.tractcode)
                tract_map.tractprofiles.append(tract_map_data.__dict__)
                continue

            tract_data = tract_data.iloc[0]

            medianhouseholdincome = tract_data.economy['medianhouseholdincome']['data1'][0]
            unemploymentrate = tract_data.economy['unemploymentrate']['data'][0]

            tract_map_data.medianhouseholdincome = medianhouseholdincome
            tract_map_data.medianhouseholdincomecolor = assign_color(medianhouseholdincome, 'medianhouseholdincome')
            tract_map_data.unemploymentrate = unemploymentrate
            tract_map_data.unemploymentratecolor = assign_color(unemploymentrate, 'unemploymentrate')

            tract_map.tractprofiles.append(tract_map_data.__dict__)

        mongoclient.insert_list_mongo(list_data=[tract_map.__dict__],
                                      dbname='ScopeOutMaps',
                                      collection_name='TractsMarketMaps',
                                      prod_env=ProductionEnvironment.MARKET_MAPS,
                                      collection_update_existing={})

def assign_color(value, datatype):
    if value != value:
        return COLOR_LEVEL_0

    if datatype == "medianhouseholdincome":
        if value < 25000:
            return COLOR_LEVEL_1
        elif value < 50000:
            return COLOR_LEVEL_2
        elif value < 75000:
            return COLOR_LEVEL_3
        elif value < 100000:
            return COLOR_LEVEL_4
        else:
            return COLOR_LEVEL_5
    elif datatype == "unemploymentrate":
        if value < 1.5:
            return COLOR_LEVEL_5
        if value < 3:
            return COLOR_LEVEL_4
        if value < 6:
            return COLOR_LEVEL_3
        if value < 10:
            return COLOR_LEVEL_2
        else:
            return COLOR_LEVEL_1


def generate_zipcode_maps():
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

        for zipcode_data in cbsa_data.zipcodes:
            geometry = []

            # if zipcode_data['zipcode'] not in ["92705", "90275", "92683"]:
            #     continue

            if not list_length_okay(zipcode_data['geometry'], 1):
                print('zipcode geo length >1: ', zipcode_data['zipcode'])
                for zipcode_subgeo in zipcode_data['geometry']:
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