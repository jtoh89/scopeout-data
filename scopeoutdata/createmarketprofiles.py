import sys
from database import mongoclient
from enums import ProductionEnvironment, GeoLevels, GeoIdField
from utils.utils import get_county_cbsa_lookup, check_dataframe_has_one_record
from math import nan

def create_market_profiles():
    us_profile = mongoclient.query_collection(database_name="MarketTrends",
                                                     collection_name="markettrends",
                                                     collection_filter={'geolevel': GeoLevels.USA.value},
                                                     prod_env=ProductionEnvironment.MARKET_TRENDS).to_dict('records')[0]

    cbsa_profiles = mongoclient.query_collection(database_name="MarketTrends",
                                                 collection_name="markettrends",
                                                 collection_filter={'geolevel': GeoLevels.CBSA.value},
                                                 prod_env=ProductionEnvironment.MARKET_TRENDS)

    county_cbsa_lookup = get_county_cbsa_lookup(state_id='')

    county_profiles = mongoclient.query_collection(database_name="MarketTrends",
                                              collection_name="markettrends",
                                              collection_filter={'geolevel': GeoLevels.COUNTY.value},
                                              prod_env=ProductionEnvironment.MARKET_TRENDS)

    county_profile_list = []

    for i, county_profile in county_profiles.iterrows():
        county_profile = county_profile.to_dict()
        set_real_estate_and_rental_keys(county_profile, county_cbsa_lookup, cbsa_profiles, us_profile)
        county_profile_list.append(county_profile)

    client = mongoclient.connect_to_client(prod_env=ProductionEnvironment.MARKET_TRENDS)
    dbname = 'MarketTrends'
    db = client[dbname]

    collection = db['markettrendprofiles']

    collection_filter = {'geolevel': GeoLevels.COUNTY.value}
    success = mongoclient.store_market_trends(county_profile_list, collection, collection_filter, GeoIdField.COUNTY.value)

    if success:
        print("Successfully stored batch into Mongo. County market profiles inserted: ", len(county_profile_list))
        return success


def set_real_estate_and_rental_keys(county_profile, county_cbsa_lookup, cbsa_profiles, us_profile):
    if 'realestatetrends' in county_profile.keys() and not county_profile['realestatetrends'] != county_profile['realestatetrends']:
        county_profile['countyrealestatetrends'] = county_profile['realestatetrends']
    else:
        county_profile['countyrealestatetrends'] = False

    county_profile.pop('realestatetrends')

    cbsa_match = county_cbsa_lookup[county_cbsa_lookup['countyfullcode'] == county_profile[GeoIdField.COUNTY.value]]

    if check_dataframe_has_one_record(cbsa_match):
        cbsaid = cbsa_match.cbsacode.iloc[0]
        cbsa_profile_for_county = cbsa_profiles[cbsa_profiles['cbsacode'] == cbsaid].to_dict('records')[0]

        if 'realestatetrends' in cbsa_profile_for_county.keys() and not cbsa_profile_for_county['realestatetrends'] != cbsa_profile_for_county['realestatetrends']:
            county_profile['cbsarealestatetrends'] = cbsa_profile_for_county['realestatetrends']
        else:
            county_profile['cbsarealestatetrends'] = False

        if 'rentaltrends' in cbsa_profile_for_county.keys() and not cbsa_profile_for_county['rentaltrends'] != cbsa_profile_for_county['rentaltrends']:
            county_profile['cbsarentaltrends'] = cbsa_profile_for_county['rentaltrends']
        else:
            county_profile['cbsarentaltrends'] = False
    else:
        county_profile['cbsarealestatetrends'] = False
        county_profile['cbsarentaltrends'] = False

    county_profile['usrealestatetrends'] = us_profile['realestatetrends']
    county_profile['usrentaltrends'] = us_profile['rentaltrends']
