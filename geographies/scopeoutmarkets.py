import pandas as pd
from database import mongoclient
from enums import ProductionEnvironment, GeoLevels
from utils.production import create_url_slug

def store_all_markets(top_n):
    all_markets = mongoclient.query_collection(database_name="CensusData1",
                                                 collection_name="CensusData",
                                                 collection_filter={'geolevel': GeoLevels.CBSA.value},
                                                 prod_env=ProductionEnvironment.CENSUS_DATA1)

    cbsacode_list = []
    population_list = []

    for i, row in all_markets.iterrows():
        data = row['data']
        population = data['Population Growth']['Total Population'][-1]

        cbsacode_list.append(row['geoid'])
        population_list.append(population)

    population_df = pd.DataFrame(list(zip(cbsacode_list, population_list)), columns=['cbsacode', 'population'])

    population_df = population_df.sort_values(by=['population'], ascending=False)

    top_population_list = population_df.head(top_n)

    all_cbsa = mongoclient.query_collection(database_name="Geographies",
                                                 collection_name="Cbsa",
                                                 collection_filter={},
                                                 prod_env=ProductionEnvironment.GEO_ONLY)

    all_markets = mongoclient.query_collection(database_name="CensusData1",
                                                 collection_name="CensusData",
                                                 collection_filter={'geolevel': GeoLevels.CBSA.value},
                                                 prod_env=ProductionEnvironment.CENSUS_DATA1)
    # all_markets = all_markets.to_dict('records')
    insert_list = []
    for cbsacode in top_population_list['cbsacode']:
        cbsa_info = all_markets[all_markets['geoid'] == cbsacode]

        if len(cbsa_info) > 1 or len(cbsa_info) < 1:
            print("!!! What is length of cbsa_info1? !!!")

        cbsa_info = cbsa_info.iloc[0].to_dict()

        cbsa_name = cbsa_info['geoinfo']['cbsaname']

        insert_list.append({
            'cbsacode': cbsacode,
            'cbsaname':cbsa_name,
            'urlslug': create_url_slug(cbsacode, cbsa_name)
        })
    print("inserting AllMarkets")

    mongoclient.insert_list_mongo(list_data=insert_list,
                                  dbname='ScopeOut',
                                  collection_name='AllMarkets',
                                  prod_env=ProductionEnvironment.GEO_ONLY,
                                  collection_update_existing={})


def store_scopeout_markets():
    collection_filter = {
        'cbsacode': {'$in': scopeout_market_list},
    }

    scopeout_markets = mongoclient.query_collection(database_name="Geographies",
                                                 collection_name="Cbsa",
                                                 collection_filter=collection_filter,
                                                 prod_env=ProductionEnvironment.GEO_ONLY)
    insert_list = []

    for i, row in scopeout_markets.iterrows():
        insert_list.append({
            'cbsacode': row['cbsacode'],
            'cbsaname': row['cbsaname'],
            'urlslug': create_url_slug( row['cbsacode'], row['cbsaname'])
        })

    mongoclient.insert_list_mongo(list_data=insert_list,
                                  dbname='ScopeOut',
                                  collection_name='ScopeOutMarkets',
                                  prod_env=ProductionEnvironment.GEO_ONLY,
                                  collection_update_existing={})

scopeout_market_list = [
    "35620",
    "31080",
    "16980",
    "19100",
    "26420",
    "47900",
    "37980",
    "33100",
    "12060",
    "38060",
    "14460",
    "41860",
    "40140",
    "19820",
    "42660",
    "33460",
    "41740",
    "45300",
    "19740",
    "41180",
    "12580",
    "16740",
    "36740",
    "41700",
    "38900",
    "40900",
    "38300",
    "12420",
    "29820",
    "17140",
    "28140",
    "18140",
    "26900",
    "17460",
    "34980",
    "41940",
    "47260",
    "39300",
    "27260",
    "33340",
    "36420",
    "39580",
    "32820",
    "40060",
    "31140",
    "35380",
    "41620",
    "25540",
    "15380",
]