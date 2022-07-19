import sys
from database import mongoclient
from models.zipcodemarketprofile import shortzipcodeprofile
from enums import ProductionEnvironment, GeoLevels
from utils.utils import drop_na_values_from_dict, truncate_decimals
from globals import SCOPEOUT_COLOR, CBSA_COLOR
from realestate.redfin import REDFIN_KEY

def create_short_zipcode_profiles():
    zipcodes_by_scopeout_markets = mongoclient.query_collection(database_name="ScopeOut",
                                                    collection_name="EsriZipcodesBySOMarkets",
                                                    collection_filter={},
                                                    prod_env=ProductionEnvironment.GEO_ONLY)

    for i, row in zipcodes_by_scopeout_markets.iterrows():
        cbsacode = row.cbsacode

        # if cbsacode != "28140":
        #     continue

        cbsa_market_historical = mongoclient.query_collection(database_name="MarketProfiles",
                                                   collection_name="cbsahistoricalprofiles",
                                                   collection_filter={"cbsacode":cbsacode},
                                                   prod_env=ProductionEnvironment.MARKET_PROFILES)


        if len(cbsa_market_historical) == 0:
            print("!!! WHY IS CBSA MARKET MISSING FOR SO MARKET? !!!")
            sys.exit()


        cbsa_market_historical = cbsa_market_historical.iloc[0].to_dict()
        cbsaname = cbsa_market_historical['geoname']

        cbsa_has_rental = True
        if 'rentaltrends' not in cbsa_market_historical.keys():
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

                    if '{}'.format(REDFIN_KEY) in zipcode_historical_profile.keys():
                        zip_latest_month = zipcode_historical_profile[REDFIN_KEY]['dates'][-1]

                        found_latest_month_match = True
                        index = 0

                        while found_latest_month_match:
                            index -= 1
                            if cbsa_market_historical[REDFIN_KEY]['dates'][index] == zip_latest_month:
                                found_latest_month_match = False


                        # assign median sale price
                        median_sale_price = zipcode_historical_profile[REDFIN_KEY]['mediansaleprice'][-1]
                        zip_short_profile.mediansaleprice.labels = [zipcode, cbsaname]
                        zip_short_profile.mediansaleprice.data = [median_sale_price, cbsa_market_historical[REDFIN_KEY]['mediansaleprice'][index]]
                        zip_short_profile.mediansaleprice.colors = [SCOPEOUT_COLOR, CBSA_COLOR]


                        # assign median sale price mom
                        zip_short_profile.mediansalepricemom.labels = [zipcode, cbsaname]
                        zip_mediansaleprice_mom = zipcode_historical_profile[REDFIN_KEY]['mediansalepricemom'][-1]
                        cbsa_mediansaleprice_mom = cbsa_market_historical[REDFIN_KEY]['mediansalepricemom'][index]
                        cbsa_mediansaleprice_mom = truncate_decimals(cbsa_mediansaleprice_mom * 100, 2)

                        if zip_mediansaleprice_mom != None:
                            zip_mediansaleprice_mom = truncate_decimals(zip_mediansaleprice_mom * 100, 2)
                        else:
                            zip_mediansaleprice_mom = calculate_mom_or_yoy_from_list(latest_month=zip_latest_month,
                                                                                     latest_value=median_sale_price,
                                                                                     historical_profile=zipcode_historical_profile,
                                                                                     data_type='mediansaleprice',
                                                                                     type='mom')

                        zip_short_profile.mediansalepricemom.data = [zip_mediansaleprice_mom, cbsa_mediansaleprice_mom]
                        zip_short_profile.mediansalepricemom.colors = [SCOPEOUT_COLOR, CBSA_COLOR]

                        # assign median sale price yoy
                        zip_short_profile.mediansalepriceyoy.labels = [zipcode, cbsaname]
                        zip_mediansaleprice_yoy = zipcode_historical_profile[REDFIN_KEY]['mediansalepriceyoy'][-1]
                        cbsa_mediansaleprice_yoy = cbsa_market_historical[REDFIN_KEY]['mediansalepriceyoy'][index]
                        cbsa_mediansaleprice_yoy = truncate_decimals(cbsa_mediansaleprice_yoy * 100, 2)

                        if zip_mediansaleprice_yoy != None:
                            zip_mediansaleprice_yoy = truncate_decimals(zip_mediansaleprice_yoy * 100, 2)
                        else:
                            zip_mediansaleprice_yoy = calculate_mom_or_yoy_from_list(latest_month=zip_latest_month,
                                                                                     latest_value=median_sale_price,
                                                                                     historical_profile=zipcode_historical_profile,
                                                                                     data_type='mediansaleprice',
                                                                                     type='yoy')

                        zip_short_profile.mediansalepriceyoy.data = [zip_mediansaleprice_yoy, cbsa_mediansaleprice_yoy]
                        zip_short_profile.mediansalepriceyoy.colors = [SCOPEOUT_COLOR, CBSA_COLOR]

                        # assign dom
                        zip_short_profile.dom.labels = [zipcode, cbsaname]
                        zip_short_profile.dom.data = [zipcode_historical_profile[REDFIN_KEY]['mediandom'][-1], cbsa_market_historical[REDFIN_KEY]['mediandom'][index]]
                        zip_short_profile.dom.colors = [SCOPEOUT_COLOR, CBSA_COLOR]

                        zip_short_profile.redfinupdatedate = zip_latest_month

                    if 'rentaltrends' in zipcode_historical_profile.keys():
                        zip_rental_latest_month = zipcode_historical_profile['rentaltrends']['dates'][-1]

                        if cbsa_has_rental:
                            if cbsa_market_historical['rentaltrends']['dates'][-1] != zip_rental_latest_month:
                                if cbsa_market_historical['rentaltrends']['dates'][-2] != zip_rental_latest_month:
                                    print("!!! ERROR - why do cbsa and zip rental months not match? !!!")
                                    zip_short_profile.rentaltrends.data2Name = cbsaname
                                    zip_short_profile.rentaltrends.labels = cbsa_market_historical['rentaltrends']['dates'][-13:-1]
                                    zip_short_profile.rentaltrends.data2 = cbsa_market_historical['rentaltrends']['median_rent'][-13:-1]
                                    continue
                                    # sys.exit()
                                else:
                                    zip_short_profile.rentaltrends.data2 = cbsa_market_historical['rentaltrends']['median_rent'][-13:-1]
                            else:
                                zip_short_profile.rentaltrends.data2 = cbsa_market_historical['rentaltrends']['median_rent'][-12:]

                            last_12_months = zipcode_historical_profile['rentaltrends']['dates'][-12:]

                            zip_short_profile.rentaltrends.labels = last_12_months
                            zip_short_profile.rentaltrends.data1Name = zipcode
                            zip_short_profile.rentaltrends.data1 = zipcode_historical_profile['rentaltrends']['median_rent'][-12:]
                            zip_short_profile.rentaltrends.data2Name = cbsaname

                            zip_short_profile.zillowupdatedate = zip_rental_latest_month
                        else:
                            last_12_months = zipcode_historical_profile['rentaltrends']['dates'][-12:]
                            zip_short_profile.rentaltrends.labels = last_12_months
                            zip_short_profile.rentaltrends.data1Name = zipcode
                            zip_short_profile.rentaltrends.data1 = zipcode_historical_profile['rentaltrends']['median_rent'][-12:]
                            zip_short_profile.rentaltrends.data2Name = cbsaname
                            zip_short_profile.rentaltrends.data2 = []
                            zip_short_profile.zillowupdatedate = zip_rental_latest_month


                    elif cbsa_has_rental and 'rentaltrends' in cbsa_market_historical.keys():
                        last_12_months = cbsa_market_historical['rentaltrends']['dates'][-12:]
                        zip_short_profile.rentaltrends.labels = last_12_months
                        zip_short_profile.rentaltrends.data1Name = zipcode
                        zip_short_profile.rentaltrends.data1 = []
                        zip_short_profile.rentaltrends.data2Name = cbsaname
                        zip_short_profile.rentaltrends.data2 = cbsa_market_historical['rentaltrends']['median_rent'][-12:]
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


def calculate_mom_or_yoy_from_list(latest_month, latest_value, historical_profile, data_type, type):
    if type == 'mom':
        months = 1
    else:
        months = 12

    index = False
    max_percent_change = 200
    yoy_date_list = copy.deepcopy(historical_profile[REDFIN_KEY]['dates'])
    yoy_date_list.reverse()

    latest_month_datetime = month_string_to_datetime(latest_month)
    month_comparison = latest_month_datetime - relativedelta(months=months)

    for i, date in enumerate(yoy_date_list):
        date_datetime = month_string_to_datetime(date)
        if date_datetime == month_comparison:
            index = -(i + 1)
            break

    if not index:
        return None

    past_month_datetime = month_string_to_datetime(historical_profile[REDFIN_KEY]['dates'][index])
    past_month_median_sale_price = historical_profile[REDFIN_KEY][data_type][index]

    if past_month_median_sale_price and month_comparison == past_month_datetime:
        percent_change = calculate_percent_change(starting_data=past_month_median_sale_price,
                                                      ending_data=latest_value,
                                                      move_decimal=True,
                                                      decimal_places=2)
        if percent_change < max_percent_change:
            zip_mediansaleprice_mom = percent_change
            return zip_mediansaleprice_mom

    return None

