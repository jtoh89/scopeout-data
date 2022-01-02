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
    cbsa_market_profile_list = []

    for i, row in cbsa_profiles.iterrows():
        cbsa_profile = row.to_dict()
        set_na_to_false_from_dict(cbsa_profile)
        cbsa_market_profile = cbsamarketprofile.CbsaMarketProfile()
        cbsa_market_profile.cbsacode = row['cbsacode']

        if cbsa_profile['realestatetrends']:
            cbsa_market_profile.mediansaleprice.data1 = cbsa_profile['realestatetrends']['All Residential']['median_sale_price']
            cbsa_market_profile.mediansaleprice.labels = cbsa_profile['realestatetrends']['All Residential']['dates']

            cbsa_market_profile.medianppsf.data1 = cbsa_profile['realestatetrends']['All Residential']['median_ppsf']
            cbsa_market_profile.medianppsf.labels = cbsa_profile['realestatetrends']['All Residential']['dates']

            cbsa_market_profile.monthsofsupply.data1 = cbsa_profile['realestatetrends']['All Residential']['months_of_supply']
            cbsa_market_profile.monthsofsupply.labels = cbsa_profile['realestatetrends']['All Residential']['dates']

            cbsa_market_profile.mediandom.data1 = cbsa_profile['realestatetrends']['All Residential']['median_dom']
            cbsa_market_profile.mediandom.labels = cbsa_profile['realestatetrends']['All Residential']['dates']

            cbsa_market_profile.pricedrops.data1 = cbsa_profile['realestatetrends']['All Residential']['price_drops']
            cbsa_market_profile.pricedrops.labels = cbsa_profile['realestatetrends']['All Residential']['dates']

            all_dates_count = len(cbsa_profile['realestatetrends']['All Residential']['dates'])

            if 'Single Family Residential' in cbsa_profile['realestatetrends'].keys():
                dates_count = len(cbsa_profile['realestatetrends']['Single Family Residential']['dates'])

                if dates_count != all_dates_count:
                    print("!!!ERROR - Why is there a mismatch on dates?!!!")
                    sys.exit()

                cbsa_market_profile.mediansaleprice.data2 = \
                cbsa_profile['realestatetrends']['Single Family Residential']['median_sale_price']
                cbsa_market_profile.medianppsf.data2 = cbsa_profile['realestatetrends']['Single Family Residential'][
                    'median_ppsf']
                cbsa_market_profile.monthsofsupply.data2 = \
                cbsa_profile['realestatetrends']['Single Family Residential']['months_of_supply']
                cbsa_market_profile.mediandom.data2 = cbsa_profile['realestatetrends']['Single Family Residential'][
                    'median_dom']
                cbsa_market_profile.pricedrops.data2 = cbsa_profile['realestatetrends']['Single Family Residential'][
                    'price_drops']

            if 'Multi-Family (2-4 Unit)' in cbsa_profile['realestatetrends'].keys():
                cbsa_market_profile.mediansaleprice.data3 = cbsa_profile['realestatetrends']['Multi-Family (2-4 Unit)'][
                    'median_sale_price']
                cbsa_market_profile.medianppsf.data3 = cbsa_profile['realestatetrends']['Multi-Family (2-4 Unit)'][
                    'median_ppsf']
                cbsa_market_profile.monthsofsupply.data3 = cbsa_profile['realestatetrends']['Multi-Family (2-4 Unit)'][
                    'months_of_supply']
                cbsa_market_profile.mediandom.data3 = cbsa_profile['realestatetrends']['Multi-Family (2-4 Unit)'][
                    'median_dom']
                cbsa_market_profile.pricedrops.data3 = cbsa_profile['realestatetrends']['Multi-Family (2-4 Unit)'][
                    'price_drops']
                dates_count = len(cbsa_profile['realestatetrends']['Multi-Family (2-4 Unit)']['dates'])

                if dates_count != all_dates_count:
                    print("!!!ERROR - Why is there a mismatch on dates?!!!")
                    sys.exit()

        if cbsa_profile['rentaltrends']:
            cbsa_market_profile.rentaltrends.data = cbsa_profile['rentaltrends']['All Residential']['median_rent']
            cbsa_market_profile.rentaltrends.labels = cbsa_profile['rentaltrends']['All Residential']['dates']

        if cbsa_profile['buildingpermits']:
            cbsa_market_profile.buildingpermits.labels = cbsa_profile['buildingpermits']['dates']
            cbsa_market_profile.buildingpermits.units_all = cbsa_profile['buildingpermits']['total']
            cbsa_market_profile.buildingpermits.units_1 = cbsa_profile['buildingpermits']['unit_1']
            cbsa_market_profile.buildingpermits.units_2_to_4 = cbsa_profile['buildingpermits']['units_2_to_4']
            cbsa_market_profile.buildingpermits.units_5plus = cbsa_profile['buildingpermits']['units_5plus']

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
