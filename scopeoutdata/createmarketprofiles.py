import sys
from database import mongoclient
from enums import ProductionEnvironment, GeoLevels, GeoIdField, GeoNameField, Collections_Historical_Profiles
from utils.utils import set_na_to_false_from_dict
from utils.production import get_county_cbsa_lookup, check_dataframe_has_one_record
from math import nan
import pandas as pd
from realestate.redfin import REDFIN_KEY, REDFIN_DATA_CATEGORIES, REDFIN_PROPERTY_TYPES_LOWERCASE
from models import cbsamarketprofile

def create_county_market_profiles(collection_name):
    us_profile = mongoclient.query_collection(database_name="MarketProfiles",
                                              collection_name=Collections_Historical_Profiles.USA.value,
                                              collection_filter={'geolevel': GeoLevels.USA.value},
                                              prod_env=ProductionEnvironment.MARKET_PROFILES).to_dict('records')[0]

    cbsa_profiles = mongoclient.query_collection(database_name="MarketProfiles",
                                                 collection_name=Collections_Historical_Profiles.CBSA.value,
                                                 collection_filter={'geolevel': GeoLevels.CBSA.value},
                                                 prod_env=ProductionEnvironment.MARKET_PROFILES)

    county_cbsa_lookup = get_county_cbsa_lookup(state_id='')

    county_profiles = mongoclient.query_collection(database_name="MarketProfiles",
                                                   collection_name=Collections_Historical_Profiles.COUNTY.value,
                                                   collection_filter={'geolevel': GeoLevels.COUNTY.value},
                                                   prod_env=ProductionEnvironment.MARKET_PROFILES)

    county_profile_list = []

    for i, county_profile in county_profiles.iterrows():
        county_profile = county_profile.to_dict()

        final_county_profile = aggregate_all_to_county_profile(county_profile, county_cbsa_lookup, cbsa_profiles, us_profile)
        county_profile_list.append(final_county_profile)

    client = mongoclient.connect_to_client(prod_env=ProductionEnvironment.MARKET_PROFILES)
    dbname = 'MarketProfiles'
    db = client[dbname]

    collection = db[collection_name]

    collection_filter = {}
    success = mongoclient.batch_inserts_with_list(county_profile_list, collection, collection_filter, GeoIdField.COUNTY.value)

    if success:
        print("Successfully stored batch into Mongo. County market profiles inserted: ", len(county_profile_list))
        return success

def aggregate_all_to_county_profile(county_profile, county_cbsa_lookup, cbsa_profiles, us_profile):
    # Add countyrealestatetrends. Set False if no data available
    set_na_to_false_from_dict(county_profile)

    if '{}'.format(REDFIN_KEY) in county_profile.keys() and county_profile[REDFIN_KEY]:
        county_profile['countyrealestatetrends'] = add_missing_redfin_keys(county_profile[REDFIN_KEY])
        county_profile.pop('{}'.format(REDFIN_KEY))
    else:
        county_profile['countyrealestatetrends'] = False

    cbsa_match = county_cbsa_lookup[county_cbsa_lookup['countyfullcode'] == county_profile[GeoIdField.COUNTY.value]]

    add_unemployment_to_county_profile(county_profile, us_profile)
    add_realestate_rental_to_county_profile(county_profile, cbsa_profiles, cbsa_match, us_profile)

    return group_geo_market_data(county_profile)

def add_unemployment_to_county_profile(county_profile, us_profile):
    if 'historicalunemploymentrate' in county_profile.keys():
        county_profile['ushistoricalunemploymentrate'] = {
            'dates': us_profile['historicalunemploymentrate']['dates'],
            'unemploymentrate': us_profile['historicalunemploymentrate']['unemploymentrate'],
        }
    else:
        county_profile['historicalunemploymentrate'] = False

def add_realestate_rental_to_county_profile(county_profile, cbsa_profiles, cbsa_match, us_profile):
    # Check if cbsa exists for county
    if check_dataframe_has_one_record(cbsa_match):
        cbsaid = cbsa_match.cbsacode.iloc[0]
        county_profile[GeoNameField.CBSA.value] = cbsa_match.cbsaname.iloc[0] + ' Metro'
        cbsa_profile_for_county = cbsa_profiles[cbsa_profiles['cbsacode'] == cbsaid].to_dict('records')[0]
        set_na_to_false_from_dict(cbsa_profile_for_county)

        # Add cbsarealestatetrends. Set False if no data available
        if '{}'.format(REDFIN_KEY) in cbsa_profile_for_county.keys() and cbsa_profile_for_county[REDFIN_KEY]:
            county_profile['cbsarealestatetrends'] = add_missing_redfin_keys(cbsa_profile_for_county[REDFIN_KEY])
        else:
            county_profile['cbsarealestatetrends'] = False

        if 'rentaltrends' in cbsa_profile_for_county.keys() and not cbsa_profile_for_county['rentaltrends'] != cbsa_profile_for_county['rentaltrends']:
            county_profile['cbsarentaltrends'] = cbsa_profile_for_county['rentaltrends']
        else:
            county_profile['cbsarentaltrends'] = False
    else:
        county_profile['cbsarealestatetrends'] = False
        county_profile['cbsarentaltrends'] = False
        county_profile[GeoNameField.CBSA.value] = 'N/A'

    # Add us realestate and rental trends
    county_profile['usrealestatetrends'] = us_profile[REDFIN_KEY]
    county_profile['usrentaltrends'] = us_profile['rentaltrends']

def group_geo_market_data(county_profile):
    final_dict = {
        'countyfullcode': county_profile['countyfullcode'],
    }

    countyname = county_profile['geoname']
    cbsaname = county_profile['cbsaname']

    if county_profile['countyrealestatetrends']:
        data_dict = create_redfin_dict(county_profile, countyname, cbsaname)
        final_dict['realestatedata'] = data_dict
    else:
        final_dict['realestatedata'] = False

    if county_profile['cbsarentaltrends']:
        data_dict = create_rental_dict(county_profile, cbsaname)
        final_dict['rentaldata'] = data_dict
    else:
        final_dict['rentaldata'] = False

    if county_profile['historicalunemploymentrate']:
        data_dict = create_unemployment_dict(county_profile, countyname)
        final_dict['unemploymentrate'] = data_dict
    else:
        final_dict['unemploymentrate'] = False

    return final_dict

def create_redfin_dict(county_profile, countyname, cbsaname):

    redfin_lowercase_ptype_to_keys = {'all': 'allresidential',
                            'singlefamily': 'singlefamily',
                            'multifamily': 'multifamily'}

    redfin_ptype_to_title = {'all': 'All',
                            'singlefamily': 'Single Family',
                            'multifamily': '2-4 units'}

    return_dict = {}


    for cat in REDFIN_DATA_CATEGORIES:
        return_dict[cat] = {}
        for ptype in REDFIN_PROPERTY_TYPES_LOWERCASE:
            us_df = pd.DataFrame.from_dict(county_profile['usrealestatetrends'][ptype])
            county_df = pd.DataFrame.from_dict(county_profile['countyrealestatetrends'][ptype])

            combined_df = us_df.merge(county_df, on='dates', how='left', suffixes=('', '_county'))

            return_dict[cat][redfin_lowercase_ptype_to_keys[ptype]] = {
                    'title': redfin_ptype_to_title[ptype],
                    'charttype': 'line',
                    'labels': list(combined_df['dates']),
                    'data1Name': countyname,
                    'data1': list_replace_nan_with_none(list(combined_df[cat + '_county'])),
                    'data3Name': 'United States',
                    'data3': list_replace_nan_with_none(list(combined_df[cat]))
                }

            if county_profile['cbsarealestatetrends']:
                cbsa_df = pd.DataFrame.from_dict(county_profile['cbsarealestatetrends'][ptype])
                combined_df = combined_df.merge(cbsa_df, on='dates', how='left', suffixes=('', '_cbsa'))
                return_dict[cat][redfin_lowercase_ptype_to_keys[ptype]]['data2Name'] = cbsaname
                return_dict[cat][redfin_lowercase_ptype_to_keys[ptype]]['data2'] = list_replace_nan_with_none(list(combined_df[cat + '_cbsa']))
            else:
                return_dict[cat][redfin_lowercase_ptype_to_keys[ptype]]['data2Name'] = cbsaname
                return_dict[cat][redfin_lowercase_ptype_to_keys[ptype]]['data2'] = []


    return return_dict

def create_rental_dict(county_profile, cbsaname):
    us_rental_df = pd.DataFrame.from_dict(county_profile['usrentaltrends'])

    cbsa_rental_df = pd.DataFrame.from_dict(county_profile['cbsarentaltrends'])

    combined_rental_df = us_rental_df.merge(cbsa_rental_df, on='dates', how='left', suffixes=('', '_cbsa'))

    return_dict = {
        'title': 'Median Rent',
        'charttype': 'line',
        'labels': list(combined_rental_df['dates']),
        'data1Name': cbsaname,
        'data1': list_replace_nan_with_none(list(combined_rental_df['median_rent_cbsa'])),
        'data2Name': 'United States',
        'data2': list_replace_nan_with_none(list(combined_rental_df['median_rent'])),
    }

    return return_dict

def create_unemployment_dict(county_profile, countyname):
    us_unemployment_df = pd.DataFrame.from_dict(county_profile['ushistoricalunemploymentrate'])

    if 'historicalunemploymentrate' not in county_profile.keys():
        print('')
    try:
        county_unemployment_df = pd.DataFrame.from_dict(county_profile['historicalunemploymentrate'])
    except Exception as e:
        print('')
    combined_unemployment_df = us_unemployment_df.merge(county_unemployment_df, on='dates', how='inner', suffixes=('', '_county'))

    return_dict = {
        'title': 'Unemployment Rate',
        'charttype': 'line',
        'labels': list(combined_unemployment_df['dates']),
        'data1Name': countyname,
        'data1': list_replace_nan_with_none(list(combined_unemployment_df['unemploymentrate_county'])),
        'data2Name': 'United States',
        'data2': list_replace_nan_with_none(list(combined_unemployment_df['unemploymentrate'])),
    }

    return return_dict

def list_replace_nan_with_none(data_list):
    return_list = []
    for item in data_list:
        if item != item:
            return_list.append(None)
        else:
            return_list.append(item)

    return return_list

def add_missing_redfin_keys(geo_real_estate_trend):
    empty_dict = {
        'dates': [],
        'median_sale_price': [],
        'median_ppsf': [],
        'months_of_supply': [],
        'median_dom': [],
        'price_drops': []
    }

    if not geo_real_estate_trend:
        return empty_dict


    for redfin_key in REDFIN_PROPERTY_TYPES_LOWERCASE:
        if redfin_key not in geo_real_estate_trend.keys():
            geo_real_estate_trend[redfin_key] = empty_dict

    return geo_real_estate_trend