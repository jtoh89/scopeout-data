import sys
from database import mongoclient
from enums import ProductionEnvironment, GeoLevels, GeoIdField, GeoNameField
from utils.utils import get_county_cbsa_lookup, check_dataframe_has_one_record, set_na_to_false_from_dict
from math import nan
import pandas as pd
from realestate.redfin import REDFIN_PROPERTY_TYPES, REDFIN_DATA_CATEGORIES
from models import cbsamarketprofile

def generate_cbsa_market_profiles(prod_env, geoid_field):
    cbsa_profiles = mongoclient.query_collection(database_name="MarketTrends",
                                                 collection_name="markettrends",
                                                 collection_filter={'geolevel': GeoLevels.CBSA.value},
                                                 prod_env=ProductionEnvironment.MARKET_TRENDS)


    census_cbsa_data = mongoclient.query_collection(database_name="CensusData1",
                                                 collection_name="CensusData",
                                                 collection_filter={'geolevel': GeoLevels.CBSA.value},
                                                 prod_env=ProductionEnvironment.CENSUS_DATA1)


    cbsa_market_profile_list = []

    for i, row in cbsa_profiles.iterrows():
        cbsa_profile = row.to_dict()
        set_na_to_false_from_dict(cbsa_profile)
        cbsa_market_profile = cbsamarketprofile.CbsaMarketProfile()
        cbsa_market_profile.cbsacode = row[geoid_field]
        cbsa_market_profile.cbsaname = row['geoname']
        cbsa_market_profile.urlslug = row['geoname'].split(", ")[0].replace('--','-').replace(' ','-').lower() + "-real-estate-market-trends"

        census_cbsa_data_match = census_cbsa_data[census_cbsa_data['geoid'] == row[geoid_field]]

        if len(census_cbsa_data_match) > 0:
            census_cbsa_data_match = census_cbsa_data_match.data.iloc[0]
        else:
            census_cbsa_data_match = False

        if cbsa_profile['realestatetrends']:
            cbsa_market_profile.mediansaleprice.data1 = cbsa_profile['realestatetrends']['all']['median_sale_price']
            cbsa_market_profile.mediansaleprice.labels = cbsa_profile['realestatetrends']['all']['dates']

            cbsa_market_profile.medianppsf.data1 = cbsa_profile['realestatetrends']['all']['median_ppsf']
            cbsa_market_profile.medianppsf.labels = cbsa_profile['realestatetrends']['all']['dates']

            cbsa_market_profile.monthsofsupply.data1 = cbsa_profile['realestatetrends']['all']['months_of_supply']
            cbsa_market_profile.monthsofsupply.labels = cbsa_profile['realestatetrends']['all']['dates']

            cbsa_market_profile.mediandom.data1 = cbsa_profile['realestatetrends']['all']['median_dom']
            cbsa_market_profile.mediandom.labels = cbsa_profile['realestatetrends']['all']['dates']

            cbsa_market_profile.pricedrops.data1 = cbsa_profile['realestatetrends']['all']['price_drops']
            cbsa_market_profile.pricedrops.labels = cbsa_profile['realestatetrends']['all']['dates']

            all_dates_count = len(cbsa_profile['realestatetrends']['all']['dates'])

            if 'singlefamily' in cbsa_profile['realestatetrends'].keys():
                dates_count = len(cbsa_profile['realestatetrends']['singlefamily']['dates'])

                if dates_count != all_dates_count:
                    print("!!!ERROR - Why is there a mismatch on dates?!!!")
                    sys.exit()

                cbsa_market_profile.mediansaleprice.data2 = \
                cbsa_profile['realestatetrends']['singlefamily']['median_sale_price']
                cbsa_market_profile.medianppsf.data2 = cbsa_profile['realestatetrends']['singlefamily'][
                    'median_ppsf']
                cbsa_market_profile.monthsofsupply.data2 = \
                cbsa_profile['realestatetrends']['singlefamily']['months_of_supply']
                cbsa_market_profile.mediandom.data2 = cbsa_profile['realestatetrends']['singlefamily'][
                    'median_dom']
                cbsa_market_profile.pricedrops.data2 = cbsa_profile['realestatetrends']['singlefamily'][
                    'price_drops']

            if 'multifamily' in cbsa_profile['realestatetrends'].keys():
                cbsa_market_profile.mediansaleprice.data3 = cbsa_profile['realestatetrends']['multifamily'][
                    'median_sale_price']
                cbsa_market_profile.medianppsf.data3 = cbsa_profile['realestatetrends']['multifamily'][
                    'median_ppsf']
                cbsa_market_profile.monthsofsupply.data3 = cbsa_profile['realestatetrends']['multifamily'][
                    'months_of_supply']
                cbsa_market_profile.mediandom.data3 = cbsa_profile['realestatetrends']['multifamily'][
                    'median_dom']
                cbsa_market_profile.pricedrops.data3 = cbsa_profile['realestatetrends']['multifamily'][
                    'price_drops']
                dates_count = len(cbsa_profile['realestatetrends']['multifamily']['dates'])

                if dates_count != all_dates_count:
                    print("!!!ERROR - Why is there a mismatch on dates?!!!")
                    sys.exit()

        if cbsa_profile['rentaltrends']:
            cbsa_market_profile.rentaltrends.dataName = "Median Rent"
            cbsa_market_profile.rentaltrends.data = cbsa_profile['rentaltrends']['All Residential']['median_rent']
            cbsa_market_profile.rentaltrends.labels = cbsa_profile['rentaltrends']['All Residential']['dates']

        if cbsa_profile['buildingpermits']:
            cbsa_market_profile.buildingpermits.dataName = "Total Units Permitted"
            cbsa_market_profile.buildingpermits.labels = cbsa_profile['buildingpermits']['dates']
            cbsa_market_profile.buildingpermits.data = cbsa_profile['buildingpermits']['total']

        if cbsa_profile['historicunemploymentrate']:
            cbsa_market_profile.unemploymentrate.dataName = "Unemployment Rate"
            cbsa_market_profile.unemploymentrate.labels = cbsa_profile['historicunemploymentrate']['dates']
            cbsa_market_profile.unemploymentrate.data = cbsa_profile['historicunemploymentrate']['unemploymentrate']

        if census_cbsa_data_match:
            cbsa_market_profile.totalhousingunit.dataName = "Total Housing Units"
            cbsa_market_profile.totalhousingunit.labels = census_cbsa_data_match['Housing Unit Growth']['years']
            cbsa_market_profile.totalhousingunit.data = census_cbsa_data_match['Housing Unit Growth']['Total Housing Units']

            cbsa_market_profile.totalhouseholdgrowth.dataName = "Total Households"
            cbsa_market_profile.totalhouseholdgrowth.labels = census_cbsa_data_match['Total Households']['years']
            cbsa_market_profile.totalhouseholdgrowth.data = census_cbsa_data_match['Total Households']['Total Households']

        cbsa_market_profile.convert_to_dict()
        cbsa_market_profile_list.append(cbsa_market_profile.__dict__)


    client = mongoclient.connect_to_client(prod_env=prod_env)
    dbname = 'MarketTrends'
    db = client[dbname]
    collection = db['cbsamarketprofiles']

    collection_filter = {}

    success = mongoclient.batch_inserts_with_list(cbsa_market_profile_list, collection, collection_filter, geoid_field)

    if success:
        print("Successfully stored batch into Mongo. Rows inserted: ", len(cbsa_market_profile_list))
        return success
