import sys
from database import mongoclient
from enums import ProductionEnvironment, GeoLevels, GeoIdField, GeoNameField, Collections_Historical_Profiles, Collections_Profiles
from utils.utils import get_county_cbsa_lookup, check_dataframe_has_one_record, set_na_to_false_from_dict, create_url_slug, list_float_to_percent
from math import nan
import pandas as pd
from realestate.redfin import REDFIN_PROPERTY_TYPES, REDFIN_DATA_CATEGORIES
from models import cbsamarketprofile

def generate_cbsa_market_profiles(prod_env, geoid_field):
    # us_historical_profile = mongoclient.query_collection(database_name="MarketProfiles",
    #                                              collection_name=Collections_Historical_Profiles.USA.value,
    #                                              collection_filter={'geolevel': GeoLevels.USA.value},
    #                                              prod_env=ProductionEnvironment.MARKET_PROFILES)
    # us_historical_profile = us_historical_profile.to_dict()

    cbsa_historical_profiles = mongoclient.query_collection(database_name="MarketProfiles",
                                                 collection_name=Collections_Historical_Profiles.CBSA.value,
                                                 collection_filter={'geolevel': GeoLevels.CBSA.value},
                                                 prod_env=ProductionEnvironment.MARKET_PROFILES)


    census_cbsa_data = mongoclient.query_collection(database_name="CensusData1",
                                                 collection_name="CensusData",
                                                 collection_filter={'geolevel': GeoLevels.CBSA.value},
                                                 prod_env=ProductionEnvironment.CENSUS_DATA1)


    cbsa_market_profile_list = []

    for i, row in cbsa_historical_profiles.iterrows():
        cbsa_profile = row.to_dict()
        set_na_to_false_from_dict(cbsa_profile)
        cbsa_market_profile = cbsamarketprofile.CbsaMarketProfile()
        cbsa_market_profile.cbsacode = row[geoid_field]
        cbsa_market_profile.cbsaname = row['geoname']

        cbsa_market_profile.urlslug = create_url_slug(marketname=row['geoname'], cbsacode=cbsa_market_profile.cbsacode)

        census_cbsa_data_match = census_cbsa_data[census_cbsa_data['geoid'] == row[geoid_field]]

        if len(census_cbsa_data_match) > 0:
            census_cbsa_data_match = census_cbsa_data_match.data.iloc[0]
        else:
            census_cbsa_data_match = False

        if cbsa_profile['realestatetrends']:
            cbsa_market_profile.mediansaleprice.labels = cbsa_profile['realestatetrends']['dates']
            cbsa_market_profile.mediansaleprice.data = cbsa_profile['realestatetrends']['mediansaleprice']

            cbsa_market_profile.mediansalepricemom.labels = cbsa_profile['realestatetrends']['dates']
            cbsa_market_profile.mediansalepricemom.data = list_float_to_percent(cbsa_profile['realestatetrends']['mediansalepricemom'])

            cbsa_market_profile.medianppsf.data = cbsa_profile['realestatetrends']['medianppsf']
            cbsa_market_profile.medianppsf.labels = cbsa_profile['realestatetrends']['dates']

            cbsa_market_profile.monthsofsupply.data = cbsa_profile['realestatetrends']['monthsofsupply']
            cbsa_market_profile.monthsofsupply.labels = cbsa_profile['realestatetrends']['dates']

            cbsa_market_profile.mediandom.data = cbsa_profile['realestatetrends']['mediandom']
            cbsa_market_profile.mediandom.labels = cbsa_profile['realestatetrends']['dates']

            cbsa_market_profile.pricedrops.data = list_float_to_percent(cbsa_profile['realestatetrends']['pricedrops'])
            cbsa_market_profile.pricedrops.labels = cbsa_profile['realestatetrends']['dates']

        if cbsa_profile['rentaltrends']:
            cbsa_market_profile.rentaltrends.dataName = "Median Rent"
            cbsa_market_profile.rentaltrends.data = cbsa_profile['rentaltrends']['median_rent']
            cbsa_market_profile.rentaltrends.labels = cbsa_profile['rentaltrends']['dates']

        if 'buildingpermits' in cbsa_profile.keys() and cbsa_profile['buildingpermits']:
            cbsa_market_profile.buildingpermits.dataName = "Total Units Permitted"
            cbsa_market_profile.buildingpermits.labels = cbsa_profile['buildingpermits']['dates']
            cbsa_market_profile.buildingpermits.data = cbsa_profile['buildingpermits']['total']

        if 'historicunemploymentrate' in cbsa_profile.keys() and cbsa_profile['historicunemploymentrate']:
            cbsa_market_profile.unemploymentrate.dataName = "Unemployment Rate"
            cbsa_market_profile.unemploymentrate.labels = cbsa_profile['historicunemploymentrate']['dates']
            cbsa_market_profile.unemploymentrate.data = cbsa_profile['historicunemploymentrate']['unemploymentrate']

        if census_cbsa_data_match:
            cbsa_market_profile.housingunitsvshouseholds.data1Name = "Total Housing Units"
            cbsa_market_profile.housingunitsvshouseholds.labels = census_cbsa_data_match['Housing Unit Growth']['years']
            cbsa_market_profile.housingunitsvshouseholds.data1 = census_cbsa_data_match['Housing Unit Growth']['Total Housing Units']
            cbsa_market_profile.housingunitsvshouseholds.data2Name = "Total Households"
            cbsa_market_profile.housingunitsvshouseholds.data2 = census_cbsa_data_match['Total Households']['Total Households']

            cbsa_market_profile.totalpopulationgrowth.dataName = "Total Population"
            cbsa_market_profile.totalpopulationgrowth.labels = census_cbsa_data_match['Population Growth']['years']
            cbsa_market_profile.totalpopulationgrowth.data = census_cbsa_data_match['Population Growth']['Total Population']

        cbsa_market_profile.convert_to_dict()
        cbsa_market_profile_list.append(cbsa_market_profile.__dict__)


    client = mongoclient.connect_to_client(prod_env=prod_env)
    dbname = 'MarketProfiles'
    db = client[dbname]
    collection = db[Collections_Profiles.CBSA.value]

    collection_filter = {}

    success = mongoclient.batch_inserts_with_list(cbsa_market_profile_list, collection, collection_filter, geoid_field)

    if success:
        print("Successfully stored batch into Mongo. Rows inserted: ", len(cbsa_market_profile_list))
        return success
