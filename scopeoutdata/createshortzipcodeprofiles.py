import sys
from database import mongoclient
from models.zipcodemarketprofile import shortzipcodeprofile
from enums import ProductionEnvironment, GeoIdField
from utils.utils import drop_na_values_from_dict, list_length_okay, create_url_slug, calculate_percentiles_from_list, number_to_string, assign_color, COLOR_LEVEL_NA, assign_legend_details
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

        cbsa_has_rental = True
        if len(cbsa_market['rentaltrends']['labels']) < 1:
            cbsa_has_rental = False

        zip_historical = mongoclient.query_collection(database_name="MarketProfiles",
                                                      collection_name="zipcodehistoricalprofiles",
                                                      collection_filter={'zipcode': {'$in': row.zipcodes}},
                                                      prod_env=ProductionEnvironment.MARKET_PROFILES)

        insert_list = []

        for zipcode in row.zipcodes:
            zip_short_profile = shortzipcodeprofile.ShortZipcodeProfile()
            zip_short_profile.geoid = zipcode
            zip_short_profile.cbsacode = cbsacode

            if zipcode in list(zip_historical['zipcode']):
                zipcode_historical_profile = zip_historical[zip_historical['zipcode'] == zipcode]

                if len(zipcode_historical_profile) == 0:
                    print("!!! ERROR - why is there no zipcode historical found?")
                    sys.exit()
                else:
                    zipcode_historical_profile = zipcode_historical_profile.iloc[0].to_dict()

                    zipcode_historical_profile = drop_na_values_from_dict(zipcode_historical_profile)

                    if 'realestatetrends' in zipcode_historical_profile.keys():
                        zip_latest_month = zipcode_historical_profile['realestatetrends']['dates'][-1]

                        found_latest_month_match = True
                        index = 0

                        while found_latest_month_match:
                            index -= 1
                            if cbsa_market['mediansaleprice']['labels'][index] == zip_latest_month:
                                found_latest_month_match = False

                        zip_short_profile.mediansaleprice.labels = [zipcode, cbsaname]
                        zip_short_profile.mediansaleprice.data = [cbsa_market['mediansaleprice']['data'][index], zipcode_historical_profile['realestatetrends']['mediansaleprice'][-1]]
                        zip_short_profile.mediansaleprice.colors = ['#00d6b4', '#4F6D7A']


                        zip_short_profile.mediansalepricemom.labels = [zipcode, cbsaname]
                        zip_short_profile.mediansalepricemom.data = [cbsa_market['mediansalepricemom']['data'][index], zipcode_historical_profile['realestatetrends']['mediansalepricemom'][-1]]
                        zip_short_profile.mediansalepricemom.colors = ['#00d6b4', '#4F6D7A']

                        zip_short_profile.dom.labels = [zipcode, cbsaname]
                        zip_short_profile.dom.data = [cbsa_market['mediandom']['data'][index], zipcode_historical_profile['realestatetrends']['mediandom'][-1]]
                        zip_short_profile.dom.colors = ['#00d6b4', '#4F6D7A']

                        zip_short_profile.redfinupdatedate = zip_latest_month

                    if 'rentaltrends' in zipcode_historical_profile.keys():
                        zip_rental_latest_month = zipcode_historical_profile['rentaltrends']['dates'][-1]

                        if cbsa_has_rental:
                            if cbsa_market['rentaltrends']['labels'][-1] != zip_rental_latest_month:
                                print("!!! ERROR - why does the last month not match? !!!")
                                sys.exit()

                            last_12_months = zipcode_historical_profile['rentaltrends']['dates'][-12:]

                            zip_short_profile.rentaltrends.labels = last_12_months
                            zip_short_profile.rentaltrends.data1Name = zipcode
                            zip_short_profile.rentaltrends.data1 = zipcode_historical_profile['rentaltrends']['median_rent'][-12:]
                            zip_short_profile.rentaltrends.data2Name = cbsaname
                            zip_short_profile.rentaltrends.data2 = cbsa_market['rentaltrends']['data'][:12]
                            zip_short_profile.zillowupdatedate = zip_rental_latest_month
                        else:
                            last_12_months = zipcode_historical_profile['rentaltrends']['dates'][-12:]
                            zip_short_profile.rentaltrends.labels = last_12_months
                            zip_short_profile.rentaltrends.data1Name = zipcode
                            zip_short_profile.rentaltrends.data1 = zipcode_historical_profile['rentaltrends']['median_rent'][-12:]
                            zip_short_profile.rentaltrends.data2Name = cbsaname
                            zip_short_profile.rentaltrends.data2 = []
                            zip_short_profile.zillowupdatedate = zip_rental_latest_month


                    elif cbsa_has_rental and 'rentaltrends' in cbsa_market.keys():
                        last_12_months = cbsa_market['rentaltrends']['labels'][-12:]
                        zip_short_profile.rentaltrends.labels = last_12_months
                        zip_short_profile.rentaltrends.data1Name = zipcode
                        zip_short_profile.rentaltrends.data1 = []
                        zip_short_profile.rentaltrends.data2Name = cbsaname
                        zip_short_profile.rentaltrends.data2 = cbsa_market['rentaltrends']['data'][:12]
                        zip_short_profile.zillowupdatedate = last_12_months[-1]

                    zip_short_profile = zip_short_profile.convert_to_dict()
                    insert_list.append(zip_short_profile.__dict__)
            else:
                print("!!! ERROR - why is there no zipcode in historical? !!!")
                sys.exit()
                # insert_list.append(zip_short_profile)

        client = mongoclient.connect_to_client(prod_env=ProductionEnvironment.PROD)
        dbname = 'ShortProfiles'
        db = client[dbname]
        collection = db['shortzipcodeprofiles']

        collection_filter = {}

        success = mongoclient.batch_inserts_with_list(insert_list, collection, collection_filter, 'geoid')

        if not success:
            print("!!! zipcode short profiles insert failed !!!", len(insert_list))
            sys.exit()
        else:
            print("Successfully inserted zip short profile for", cbsaname)

