from enums import ProductionEnvironment
from database import mongoclient
from utils.utils import get_county_cbsa_lookup
import sys

def get_all_geo_data_for_neighborhoods(stateid, prod_env):
    census_tract_data_filter = {
        'stateid': {'$eq': stateid},
        'geolevel': {'$eq': 'tract'},
    }

    census_tract_data = mongoclient.query_collection(database_name=prod_env.value,
                                                     collection_name="CensusData",
                                                     collection_filter=census_tract_data_filter,
                                                     prod_env=prod_env)

    if len(census_tract_data) < 1:
        print('Did not find any census_tract_data. Check which database state uses for censusdata')
        sys.exit()

    counties_to_get = []
    for i, record in census_tract_data.iterrows():
        countyfullcode = record.geoinfo['countyfullcode']

        if countyfullcode not in counties_to_get:
            counties_to_get.append(countyfullcode)


    census_county_data_filter = {
        'stateid': {'$eq': stateid},
        'geolevel': {'$eq': 'county'},
        'geoid': {'$in': counties_to_get},
    }

    county_data = mongoclient.query_collection(database_name="CensusData1",
                                               collection_name="CensusData",
                                               collection_filter=census_county_data_filter,
                                               prod_env=ProductionEnvironment.CENSUS_DATA1)


    county_market_profiles = mongoclient.query_collection(database_name="MarketTrends",
                                                          collection_name="countymarketprofile",
                                                          collection_filter={'countyfullcode': {'$in': counties_to_get}},
                                                          prod_env=ProductionEnvironment.MARKET_TRENDS)

    county_cbsa_lookup = get_county_cbsa_lookup(state_id=stateid)

    all_cbsa = list(county_cbsa_lookup['cbsacode'].drop_duplicates())

    census_cbsa_data_filter = {
        'geolevel': {'$eq': 'cbsa'},
        'geoid': {'$in': all_cbsa}
    }

    cbsa_data = mongoclient.query_collection(database_name="CensusData1",
                                             collection_name="CensusData",
                                             collection_filter=census_cbsa_data_filter,
                                             prod_env=ProductionEnvironment.CENSUS_DATA1)

    usa_data = mongoclient.query_collection(database_name="CensusData1",
                                            collection_name="CensusData",
                                            collection_filter={'geolevel': {'$eq': 'us'}},
                                            prod_env=ProductionEnvironment.CENSUS_DATA1)

    return {
        'census_tract_data': census_tract_data,
        'county_data': county_data,
        'county_market_profiles': county_market_profiles,
        'county_cbsa_lookup': county_cbsa_lookup,
        'cbsa_data': cbsa_data,
        'usa_data': usa_data
    }


def process_all_geo_profiles(tract_profile, neighborhood_profile, county_data, cbsa_data, usa_data, county_cbsa_lookup):
    # County
    countyfullcode = tract_profile.geoinfo['countyfullcode']
    county_profile = county_data[county_data['geoid'] == countyfullcode]

    if len(county_profile) > 1:
        print('!!!ERROR - Check why there is more than 1 county record for tractid: {}!!!'.format(tract_profile.geoid))
        sys.exit()
    elif len(county_profile) == 0:
        print('!!!ERROR - Check why there is there no county record for tractid: {}!!!'.format(tract_profile.geoid))
        sys.exit()
    else:
        county_profile = county_profile.iloc[0]
        neighborhood_profile.countyfullcode = county_profile.geoid
        neighborhood_profile.countyname = county_profile.geoinfo['countyname']

    # Cbsa
    cbsainfo = county_cbsa_lookup[county_cbsa_lookup['countyfullcode'] == countyfullcode]

    if len(cbsainfo) == 1:
        cbsacode = cbsainfo['cbsacode'].iloc[0]
        cbsa_profile = cbsa_data[cbsa_data['geoid'] == cbsacode]

        if len(cbsa_profile) != 1:
            print('!!!WARNING - Why do we have missing cbsaid for cbsa data for cbsacode: {}!!!'.format(cbsacode))
            cbsa_profile = None
        else:
            cbsa_profile = cbsa_profile.iloc[0]
            neighborhood_profile.cbsacode = cbsa_profile.geoid
            neighborhood_profile.cbsaname = cbsa_profile.geoinfo['cbsaname']

    elif len(cbsainfo) > 1:
        print('!!!ERROR - Check why there is more than 1 cbsa record for tractid: {}!!!'.format(tract_profile.geoid))
        sys.exit()
    else:
        cbsa_profile = None

    usa_profile = usa_data.iloc[0]

    return {
        'neighborhood_profile': neighborhood_profile,
        'cbsa_profile': cbsa_profile,
        'county_profile': county_profile,
        'usa_profile': usa_profile
    }