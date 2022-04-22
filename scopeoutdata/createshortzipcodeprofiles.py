import sys
from database import mongoclient
from models.zipcodemarketprofile import shortzipcodeprofile
from enums import ProductionEnvironment
from utils.utils import list_length_okay, create_url_slug, calculate_percentiles_from_list, number_to_string, assign_color, COLOR_LEVEL_NA, assign_legend_details
import numpy as np

def create_short_zipcode_profiles():
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

        cbsa_market = cbsa_market.iloc[0].to_dict()
        cbsaname = cbsa_market['cbsaname']

        latest_month_cbsa = cbsa_market['mediansaleprice']['labels'][-1]

        zip_historical = mongoclient.query_collection(database_name="MarketProfiles",
                                                      collection_name="zipcodehistoricalprofiles",
                                                      collection_filter={'zipcode': {'$in': row.zipcodes}},
                                                      prod_env=ProductionEnvironment.MARKET_PROFILES)

        for zipcode in row.zipcodes:
            zip_short_profile = shortzipcodeprofile.ShortZipcodeProfile()

            if zipcode in list(zip_historical['zipcode']):
                zipcode_historical_profile = zip_historical[zip_historical['zipcode'] == zipcode]

                if len(zipcode_historical_profile) == 0:
                    print("!!! ERROR - why is there no zipcode historical found?")
                    sys.exit()
                else:
                    zipcode_historical_profile = zipcode_historical_profile.iloc[0].to_dict()

                    if len(zipcode_historical_profile['realestatetrends']) > 0:
                        zip_latest_month = zipcode_historical_profile['realestatetrends']['dates'][-1]

                        finish_latest_month_match = True
                        index = 0

                        while finish_latest_month_match:
                            index -= 1
                            if cbsa_market['mediansaleprice']['labels'][index] == zip_latest_month:
                                finish_latest_month_match = False

                        zip_short_profile.mediansaleprice.labels = [zipcode, cbsaname]
                        zip_short_profile.mediansaleprice.data = [cbsa_market['mediansaleprice']['data'][index], zipcode_historical_profile['realestatetrends']['mediansaleprice'][-1]]
                        zip_short_profile.mediansaleprice.colors = ['#00d6b4', '#4F6D7A']


                        zip_short_profile.mediansalepricemom.labels = [zipcode, cbsaname]
                        zip_short_profile.mediansalepricemom.data = [cbsa_market['mediansalepricemom']['data'][index], zipcode_historical_profile['realestatetrends']['mediansalepricemom'][-1]]
                        zip_short_profile.mediansalepricemom.colors = ['#00d6b4', '#4F6D7A']

                        zip_short_profile.dom.labels = [zipcode, cbsaname]
                        zip_short_profile.dom.data = [cbsa_market['mediansaleprice']['data'][index], zipcode_historical_profile['realestatetrends']['mediansaleprice'][-1]]
                        zip_short_profile.dom.colors = ['#00d6b4', '#4F6D7A']


                        last_12_months = zipcode_historical_profile.realestatetrends['date'][-12:]

                        zip_short_profile.mediansaleprice.labels = last_12_months
                        zip_short_profile.mediansaleprice.data1Name = zipcode
                        zip_short_profile.mediansaleprice.data1 = zipcode_historical_profile['realestatetrends']['median_sale_price'][-12:]
                        zip_short_profile.mediansaleprice.data2Name = zipcode
                        zip_short_profile.mediansaleprice.data2 = cbsa_market['realestatetrends']['median_sale_price'][-12:]

                        zip_short_profile.mediansalepriceMom.labels = last_12_months
                        zip_short_profile.mediansalepriceMom.data1Name = zipcode
                        zip_short_profile.mediansalepriceMom.data1 = zipcode_historical_profile['realestatetrends']['median_sale_price'][-12:]

                    if zipcode_historical_profile['rentaltrends'] == zipcode_historical_profile['rentaltrends']:
                        print("")

                print("")
            else:
                print("")


