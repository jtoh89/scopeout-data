import sys

from database import mongoclient
from enums import GeoLevels
from enums import DefaultGeoIds
from enums import ProductionEnvironment



def initialize_market_trends(geo_level, default_geoid, geoid_field, geoname_field, collection_name):
    '''
    Function creates cbsa records for MarketTrend.
    :return:
    '''
    collection_filter = {
        'geolevel': geo_level.value,
    }

    current_collection = mongoclient.query_collection(database_name="MarketProfiles",
                                                      collection_name=collection_name,
                                                      collection_filter=collection_filter,
                                                      prod_env=ProductionEnvironment.MARKET_PROFILES)
    if len(current_collection) > 0:
        print('Cannot initialize {} because it already exists'.format(collection_name))
        return
    else:
        geographies_df = mongoclient.query_geography(geo_level=geo_level, stateid=default_geoid)

        init_all_geos = []
        for i, row in geographies_df.iterrows():
            init_all_geos.append({geoid_field: row[geoid_field],
                                  'geolevel': geo_level.value,
                                  'geoname': row[geoname_field],
                                  })

    mongoclient.insert_list_mongo(list_data=init_all_geos,
                                  dbname='MarketProfiles',
                                  collection_name=collection_name,
                                  prod_env=ProductionEnvironment.MARKET_PROFILES)
