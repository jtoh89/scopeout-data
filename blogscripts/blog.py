import sys
from database import mongoclient
from enums import ProductionEnvironment, GeoLevels, Collections_Historical_Profiles
from utils.utils import calculate_percent_change, float_to_percent
import pandas as pd

CURRENT_MONTH = "April 2022"

def run_analysis():
    scopeout_markets = mongoclient.query_collection(database_name="ScopeOut",
                                                    collection_name="ScopeOutMarkets",
                                                    collection_filter={},
                                                    prod_env=ProductionEnvironment.GEO_ONLY)

    all_scopeout_markets = list(scopeout_markets['cbsacode'])

    cbsa_historical_profiles = mongoclient.query_collection(database_name="MarketProfiles",
                                                            collection_name=Collections_Historical_Profiles.CBSA.value,
                                                            collection_filter={'geolevel': GeoLevels.CBSA.value},
                                                            prod_env=ProductionEnvironment.MARKET_PROFILES)

    cbsa_lookup_dict = {}
    cbsacode_list = []
    median_sale_price = []
    median_sale_price_mom = []
    median_list_price = []
    dom = []
    price_drops = []

    rental_cbsacode_list = []
    rental_list = []
    rental_list_mom = []
    for i, row in cbsa_historical_profiles.iterrows():
        cbsa_dict = row.to_dict()

        if cbsa_dict['cbsacode'] not in all_scopeout_markets:
            continue

        if cbsa_dict['cbsacode'] not in cbsa_lookup_dict.keys():
            cbsa_lookup_dict[cbsa_dict['cbsacode']] = cbsa_dict['geoname']

        if cbsa_dict['realestatetrends'] != cbsa_dict['realestatetrends']:
            continue

        if cbsa_dict['realestatetrends']['dates'][-1] != CURRENT_MONTH:
            print("!!! Why does scopeout market miss latest real estate trends? !!!")
            sys.exit()


        cbsacode_list.append(cbsa_dict['cbsacode'])

        median_sale_price.append(cbsa_dict['realestatetrends']['mediansaleprice'][-1])
        median_sale_price_mom.append(float_to_percent(cbsa_dict['realestatetrends']['mediansalepricemom'][-1]))
        median_list_price.append(cbsa_dict['realestatetrends']['medianlistprice'][-1])
        dom.append(cbsa_dict['realestatetrends']['mediandom'][-1])
        price_drops.append(float_to_percent(cbsa_dict['realestatetrends']['pricedrops'][-1]))

        if cbsa_dict['rentaltrends'] != cbsa_dict['rentaltrends']:
            continue

        if cbsa_dict['realestatetrends']['dates'][-1] != CURRENT_MONTH:
            if cbsa_dict['cbsacode'] in all_scopeout_markets:
                print("!!! Why does scopeout market miss latest real estate trends? !!!")
                sys.exit()
            continue


        # def calculate_percent_change(starting_data, ending_data, move_decimal=True, decimal_places=1):
        prev_rent = cbsa_dict['rentaltrends']['median_rent'][-2]
        curr_rent = cbsa_dict['rentaltrends']['median_rent'][-1]

        if curr_rent == None or prev_rent == None:
            continue
        try:
            median_rent_mom = calculate_percent_change(prev_rent, curr_rent)
        except Exception as e:
            print(e)

        rental_cbsacode_list.append(cbsa_dict['cbsacode'])
        rental_list.append(curr_rent)
        rental_list_mom.append(median_rent_mom)

    median_sale_price_df = setup_df(data=median_sale_price, cbsacode_list=cbsacode_list, cbsa_lookup_dict=cbsa_lookup_dict)
    median_sale_price_mom_df = setup_df(data=median_sale_price_mom, cbsacode_list=cbsacode_list, cbsa_lookup_dict=cbsa_lookup_dict, append_dict={'name':'value2','df':median_sale_price_df})

    median_list_price_df = setup_df(data=median_list_price, cbsacode_list=cbsacode_list, cbsa_lookup_dict=cbsa_lookup_dict)
    dom_df = setup_df(data=dom, cbsacode_list=cbsacode_list, cbsa_lookup_dict=cbsa_lookup_dict)
    price_drops_df = setup_df(data=price_drops, cbsacode_list=cbsacode_list, cbsa_lookup_dict=cbsa_lookup_dict)
    rental_list_df = setup_df(data=rental_list, cbsacode_list=rental_cbsacode_list, cbsa_lookup_dict=cbsa_lookup_dict)
    rental_list_mom_df = setup_df(data=rental_list_mom, cbsacode_list=rental_cbsacode_list, cbsa_lookup_dict=cbsa_lookup_dict, append_dict={'name':'value2','df':rental_list_df})


    # top_median_sale_price_df = median_sale_price_df.head(10)
    # top_median_sale_price_df['category'] = 'topmediansaleprice'
    # bottom_median_sale_price_df = median_sale_price_df.tail(10)
    # bottom_median_sale_price_df['category'] = 'bottommediansaleprice'

    top_median_sale_price_mom_df = median_sale_price_mom_df.head(10)
    top_median_sale_price_mom_df['category'] = 'topmediansalepricemom'
    bottom_median_sale_price_mom_df = median_sale_price_mom_df.tail(10)
    bottom_median_sale_price_mom_df['category'] = 'bottommediansalepricemom'

    top_dom_df = dom_df.head(10)
    top_dom_df['category'] = 'topdom'
    bottom_top_dom_df = dom_df.tail(10)
    bottom_top_dom_df['category'] = 'bottomdom'

    top_price_drops_df = price_drops_df.head(10)
    top_price_drops_df['category'] = 'toppricedrops'
    bottom_price_drops_df = price_drops_df.tail(10)
    bottom_price_drops_df['category'] = 'bottompricedrops'

    top_rental_list_mom_df = rental_list_mom_df.head(10)
    top_rental_list_mom_df['category'] = 'toprentmom'
    bottom_rental_list_mom_df = rental_list_mom_df.tail(10)
    bottom_rental_list_mom_df['category'] = 'bottomrentmom'

    client = mongoclient.connect_to_client(prod_env=ProductionEnvironment.PROD)
    dbname = 'BlogData'
    db = client[dbname]
    collection = db['test']
    collection.delete_many({})

    collection.insert_many(top_median_sale_price_mom_df.to_dict('records'))
    collection.insert_many(bottom_median_sale_price_mom_df.to_dict('records'))
    collection.insert_many(top_dom_df.to_dict('records'))
    collection.insert_many(bottom_top_dom_df.to_dict('records'))
    collection.insert_many(top_price_drops_df.to_dict('records'))
    collection.insert_many(bottom_price_drops_df.to_dict('records'))
    collection.insert_many(top_rental_list_mom_df.to_dict('records'))
    collection.insert_many(bottom_rental_list_mom_df.to_dict('records'))



def setup_df(data, cbsacode_list, cbsa_lookup_dict, append_dict=None):
    df = pd.DataFrame(list(zip(cbsacode_list, data)), columns=["cbsacode", "value"]).sort_values(by=['value'], ascending=False)
    df = df.dropna()
    df['cbsaname'] = df['cbsacode'].map(cbsa_lookup_dict)

    if append_dict is not None:
        add_field = append_dict['name']
        look_up_dict = dict(zip(append_dict['df']['cbsacode'], append_dict['df']['value']))
        df[add_field] = df['cbsacode'].map(look_up_dict)

    return df

