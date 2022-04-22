import sys
from database import mongoclient
from models.zipcodemarketprofile import shortzipcodeprofile
from enums import ProductionEnvironment
from utils.utils import list_length_okay, create_url_slug, calculate_percentiles_from_list, number_to_string, assign_color, COLOR_LEVEL_NA, assign_legend_details
import numpy as np

def create_short_zipcode_profiles():
    # zip_code_data = mongoclient.query_collection(database_name="MarketProfiles",
    #                                              collection_name="zipcodehistoricalprofile",
    #                                              collection_filter={},
    #                                              prod_env=ProductionEnvironment.MARKET_PROFILES)

    zipcodes_by_scopeout_markets = mongoclient.query_collection(database_name="ScopeOut",
                                                    collection_name="EsriZipcodesBySOMarkets",
                                                    collection_filter={},
                                                    prod_env=ProductionEnvironment.GEO_ONLY)

    for i, row in zipcodes_by_scopeout_markets.iterrows():
        cbsacode = row.cbsacode

        cbsa_market = mongoclient.query_collection(database_name="MarketProfiles",
                                                   collection_name="cbsamarketprofiles",
                                                   collection_filter={"cbsacode":cbsacode},
                                                   prod_env=ProductionEnvironment.MARKET_PROFILES)

        if len(cbsa_market) == 0:
            print("!!! WHY IS CBSA MARKET MISSING FOR SO MARKET? !!!")
            sys.exit()

        cbsa_market = cbsa_market.iloc[0]

        latest_month_cbsa = cbsa_market.mediansaleprice['labels'][-1]

        zip_historical = mongoclient.query_collection(database_name="MarketProfiles",
                                                      collection_name="zipcodehistoricalprofiles",
                                                      collection_filter={'zipcode': {'$in': row.zipcodes}},
                                                      prod_env=ProductionEnvironment.MARKET_PROFILES)

        for zipcode in row.zipcodes:
            zip_short_profile = shortzipcodeprofile.ShortZipcodeProfile()

            if zipcode in list(zip_historical['zipcode']):
                zipcode_historical = zip_historical[zip_historical['zipcode'] == zipcode]

                if len(zipcode_historical) == 0:
                    print()
                else:
                    zipcode_historical = zipcode_historical.iloc[0]

                    if 'realestatetrends' in zipcode_historical.keys():
                        if latest_month_cbsa != zipcode_historical.realestatetrends['date'][-1]:
                            print("!!! LATEST MONTH DOES NOT ALIGN. Cbsa: {} !!!".format(cbsacode))
                            sys.exit()


                        last_12_months = zipcode_historical.realestatetrends['date'][-12:]

                        zip_short_profile.mediansaleprice.labels = last_12_months
                        zip_short_profile.mediansaleprice.data1Name = zipcode
                        zip_short_profile.mediansaleprice.data1 = zipcode_historical.realestatetrends['median_sale_price'][-12:]
                        zip_short_profile.mediansaleprice.data2Name = zipcode
                        zip_short_profile.mediansaleprice.data2 = cbsa_market.realestatetrends['median_sale_price'][-12:]

                        zip_short_profile.mediansalepriceMom.labels = last_12_months
                        zip_short_profile.mediansalepriceMom.data1Name = zipcode
                        zip_short_profile.mediansalepriceMom.data1 = zipcode_historical.realestatetrends['median_sale_price'][-12:]

                print("")
            else:
                print("")


