import sys
from database import mongoclient
from enums import ProductionEnvironment, GeoLevels, GeoIdField
from utils.utils import get_county_cbsa_lookup


def process_market_profiles():

    us_profiles = mongoclient.query_collection(database_name="MarketTrends",
                                                     collection_name="markettrends",
                                                     collection_filter={'geolevel': GeoLevels.USA.value},
                                                     prod_env=ProductionEnvironment.MARKET_TRENDS)


    cbsa_profiles = mongoclient.query_collection(database_name="MarketTrends",
                                                 collection_name="markettrends",
                                                 collection_filter={'geolevel': GeoLevels.CBSA.value},
                                                 prod_env=ProductionEnvironment.MARKET_TRENDS)

    county_profiles = mongoclient.query_collection(database_name="MarketTrends",
                                              collection_name="markettrends",
                                              collection_filter={'geolevel': GeoLevels.COUNTY.value},
                                              prod_env=ProductionEnvironment.MARKET_TRENDS)

    county_cbsa_lookup = get_county_cbsa_lookup(state_id='')

    for i, county in county_profiles.iterrows():
        cbsa_match = county_cbsa_lookup[county_cbsa_lookup['countyfullcode'] == county.countyfullcode]

        if len(cbsa_match) == 1:
            cbsaid = cbsa_match.cbsacode.iloc[0]
            cbsa_profile = cbsa_profiles[cbsa_profiles['cbsacode'] == cbsaid]
            print('')




