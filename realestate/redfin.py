import sys
import pandas as pd
import os
import csv
import math
from database import mongoclient
from lookups import MONTH_FORMAT, INDEX_TO_MONTH, REDFIN_PROPERTY_TYPES_LOWERCASE_CONVERSION, REDFIN_MSA_TO_CBSA, REDFIN_COUNTYID_TO_FIPS, REDFIN_USA_TO_FIPS, MONTH_TO_INDEX, INDEX_TO_MONTH
from database import mongoclient
from copy import deepcopy
from realestate import initialize
from enums import GeoLevels, DefaultGeoIds, ProductionEnvironment, GeoIdField, GeoNameField
import datetime
from utils.utils import string_to_int, nat_to_none, string_to_float
from models import zipcodemarketmap
from utils.production import calculate_percentiles_from_list, assign_legend_details, assign_color
import numpy as np
from dateutil import relativedelta
from dateutil.relativedelta import relativedelta


REDFIN_MIN_YEAR = 2016
REDFIN_MAX_YEAR = 2022
REDFIN_PROPERTY_TYPES = ['All Residential', 'Single Family Residential', 'Multi-Family (2-4 Unit)']
REDFIN_PROPERTY_TYPES_LOWERCASE = ['all', 'singlefamily', 'multifamily']
REDFIN_DATA_CATEGORIES = ['median_sale_price', 'median_ppsf', 'months_of_supply', 'median_dom', 'price_drops']


def import_redfin_historical_data(geo_level, default_geoid, geoid_field, geoname_field, collection_name):
    latest_update_date = datetime.datetime(1900, 1, 1)
    initialize_historical_profiles(geo_level, collection_name)
    currpath = os.path.dirname(os.path.abspath(__file__))
    rootpath = os.path.dirname(os.path.abspath(currpath))

    geographies_df = mongoclient.query_geography(geo_level=geo_level, stateid=default_geoid)
    geo_list = list(geographies_df[geoid_field])

    file_dir = ''
    if geo_level == GeoLevels.USA:
        file_dir = '/files/us_national_market_tracker.tsv'
    elif geo_level == GeoLevels.CBSA:
        file_dir = '/files/redfin_metro_market_tracker.tsv'
    elif geo_level == GeoLevels.COUNTY:
        file_dir = '/files/county_market_tracker.tsv'
    elif geo_level == GeoLevels.ZIPCODE:
        file_dir = '/files/zip_code_market_tracker.tsv'

    geo_data_dict = {}

    with open(rootpath + file_dir, 'r') as csvfile:
        datareader = csv.reader(csvfile, delimiter='\t')
        headers = []

        for i, row in enumerate(datareader):
            if i < 1:
                headers = row
                continue

            if i % 100000 == 0:
                print("Reading {} total lines from redfin zip file".format(i+1))

            ### create dict
            row_dict = dict(zip(headers, row))

            geoid = row_dict['table_id']

            if row_dict['is_seasonally_adjusted'] != 'f':
                continue

            if row_dict['property_type'] != 'All Residential':
                continue

            if geo_level == GeoLevels.USA:
                geoid = REDFIN_USA_TO_FIPS[geoid]
            elif geo_level == GeoLevels.CBSA:
                if geoid in REDFIN_MSA_TO_CBSA.keys():
                    geoid = REDFIN_MSA_TO_CBSA[geoid]
            elif geo_level == GeoLevels.COUNTY:
                if geoid in REDFIN_COUNTYID_TO_FIPS.keys():
                    geoid = REDFIN_COUNTYID_TO_FIPS[geoid]
            elif geo_level == GeoLevels.ZIPCODE:
                geoid = row_dict['region'].replace('Zip Code: ','')
                geoid = geoid.zfill(5)


            if geoid not in geo_list and geo_level != GeoLevels.ZIPCODE:
                continue

            year, month, day = row_dict['period_end'].split('-')

            if latest_update_date < datetime.datetime(int(year), int(month), int(day)):
                latest_update_date = datetime.datetime(int(year), int(month), int(day))

            if int(year) < REDFIN_MIN_YEAR:
                continue

            try:
                median_sale_price = string_to_int(row_dict['median_sale_price'])
                median_sale_price_mom = string_to_float(row_dict['median_sale_price_mom'], 5)
                median_sale_price_yoy = string_to_float(row_dict['median_sale_price_yoy'], 5)
                median_dom = string_to_int(row_dict['median_dom'])
                median_dom_mom = string_to_float(row_dict['median_dom_mom'], 5)
                median_dom_yoy = string_to_float(row_dict['median_dom_yoy'], 5)
                median_ppsf = string_to_int(row_dict['median_ppsf'])
                median_list_price = string_to_int(row_dict['median_list_price'])
                median_list_price_mom = string_to_float(row_dict['median_list_price_mom'], 5)
                months_of_supply = string_to_float(row_dict['months_of_supply'], 5)
                price_drops = string_to_float(row_dict['price_drops'], 5)
                inventory = string_to_float(row_dict['inventory'], 5)
                inventory = string_to_float(row_dict['inventory'], 5)
                homes_sold = string_to_int(row_dict['homes_sold'])
                sold_above_list = string_to_float(row_dict['sold_above_list'], 5)
            except Exception as e:
                print("Parse error")
                print(e)
                sys.exit()

            if geoid not in geo_data_dict.keys():
                geo_data_dict[geoid] = {
                    'realestatetrends': {
                        'dates': [datetime.datetime(int(year), int(month), int(day))],
                        'mediansaleprice': [median_sale_price],
                        'mediansalepricemom': [median_sale_price_mom],
                        'mediansalepriceyoy': [median_sale_price_yoy],
                        'mediandom': [median_dom],
                        'mediandommom': [median_dom_mom],
                        'mediandomyoy': [median_dom_yoy],
                        'medianppsf': [median_ppsf],
                        'medianlistprice': [median_list_price],
                        'medianlistpricemom': [median_list_price_mom],
                        'monthsofsupply': [months_of_supply],
                        'pricedrops': [price_drops],
                        'inventory': [inventory],
                        'homessold': [homes_sold],
                        'soldabovelist': [sold_above_list]
                    }
                }
            else:
                existing_geo_data = geo_data_dict[geoid]['realestatetrends']
                existing_geo_data['dates'].append(datetime.datetime(int(year), int(month), int(day)))
                existing_geo_data['mediansaleprice'].append(median_sale_price)
                existing_geo_data['mediansalepricemom'].append(median_sale_price_mom)
                existing_geo_data['mediansalepriceyoy'].append(median_sale_price_yoy)
                existing_geo_data['mediandom'].append(median_dom)
                existing_geo_data['mediandommom'].append(median_dom_mom)
                existing_geo_data['mediandomyoy'].append(median_dom_yoy)
                existing_geo_data['medianppsf'].append(median_ppsf)
                existing_geo_data['medianlistprice'].append(median_list_price)
                existing_geo_data['medianlistpricemom'].append(median_list_price_mom)
                existing_geo_data['monthsofsupply'].append(months_of_supply)
                existing_geo_data['pricedrops'].append(price_drops)
                existing_geo_data['inventory'].append(inventory)
                existing_geo_data['homessold'].append(homes_sold)
                existing_geo_data['soldabovelist'].append(sold_above_list)

    insert_list = []

    for k, zip_data in geo_data_dict.items():
        temp_df = pd.DataFrame.from_dict(zip_data['realestatetrends']).sort_values(by='dates')
        temp_df = temp_df.replace({np.nan: None})

        # check if at least 12 months exists
        if len(temp_df) < 12:
            continue

        if temp_df.iloc[len(temp_df)-1].dates.year != REDFIN_MAX_YEAR:
            # print("skipping zipcode: {}. Does not have any data for current year: {}".format(k, REDFIN_MAX_YEAR))
            continue

        temp_dict = {
            '{}'.format(geoid_field): k,
            'realestatetrends': {
                'dates': [],
                'mediansaleprice': [],
                'mediansalepricemom': [],
                'mediansalepriceyoy': [],
                'mediandom': [],
                'mediandommom': [],
                'mediandomyoy': [],
                'medianppsf': [],
                'medianlistprice': [],
                'medianlistpricemom': [],
                'monthsofsupply': [],
                'pricedrops': [],
                'inventory': [],
                'homessold': [],
                'soldabovelist': []
            }
        }
        temp_df = temp_df.reset_index(drop=True)
        prev_date = None
        for i, row in temp_df.iterrows():
            if i == 0:
                prev_date = temp_df['dates'].iloc[0]
                continue

            diff = relativedelta(row['dates'], prev_date)
            month_diff = diff.months
            if month_diff != None and month_diff > 1:
                fill_missing_dates(temp_dict, prev_date, month_diff-1)

            prev_date = row['dates']
            try:
                temp_dict['realestatetrends']['dates'].append(INDEX_TO_MONTH[row['dates'].month-1] + ' ' + str(row['dates'].year))
            except Exception as e:
                print(e)
                sys.exit()
            try:
                temp_dict['realestatetrends']['mediansaleprice'].append(nat_to_none(row.mediansaleprice))
                temp_dict['realestatetrends']['mediansalepricemom'].append(nat_to_none(row.mediansalepricemom))
                temp_dict['realestatetrends']['mediansalepriceyoy'].append(nat_to_none(row.mediansalepriceyoy))
                temp_dict['realestatetrends']['mediandom'].append(nat_to_none(row.mediandom))
                temp_dict['realestatetrends']['mediandommom'].append(nat_to_none(row.mediandommom))
                temp_dict['realestatetrends']['mediandomyoy'].append(nat_to_none(row.mediandomyoy))
                temp_dict['realestatetrends']['medianppsf'].append(nat_to_none(row.medianppsf))
                temp_dict['realestatetrends']['medianlistprice'].append(nat_to_none(row.medianlistprice))
                temp_dict['realestatetrends']['medianlistpricemom'].append(nat_to_none(row.medianlistpricemom))
                temp_dict['realestatetrends']['monthsofsupply'].append(nat_to_none(row.monthsofsupply))
                temp_dict['realestatetrends']['pricedrops'].append(nat_to_none(row.pricedrops))
                temp_dict['realestatetrends']['inventory'].append(nat_to_none(row.inventory))
                temp_dict['realestatetrends']['homessold'].append(nat_to_none(row.homessold))
                temp_dict['realestatetrends']['soldabovelist'].append(nat_to_none(row.soldabovelist))


            except Exception as e:
                print(e)
                sys.exit()

        insert_list.append(temp_dict)

    print("Updating existing historical profiles")
    new_insert_list = update_existing_historical_profile(insert_list, geoid_field, collection_name, geo_level)

    client = mongoclient.connect_to_client(prod_env=ProductionEnvironment.MARKET_PROFILES)
    dbname = 'MarketProfiles'
    db = client[dbname]
    collection = db[collection_name]

    collection_filter = {}

    print("Inserting records to database")
    success = mongoclient.batch_inserts_with_list(new_insert_list, collection, collection_filter, geoid_field)

    if not success:
        print("!!! geo historical batch insert failed !!!", len(new_insert_list))
        return success
    else:
        print("Redfin import finished")
        try:
            client = mongoclient.connect_to_client(prod_env=ProductionEnvironment.QA)
            dbname = 'LatestUpdates'
            db = client[dbname]

            insert = {
                      'category':'redfin',
                      'geolevel': geo_level.value,
                      'lastupdatedate': latest_update_date,
                      'year': latest_update_date.year,
                      'month': latest_update_date.month,
                      'datestring': INDEX_TO_MONTH[latest_update_date.month-1] + ' ' + str(latest_update_date.year)
                      }

            collection = db['lastupdates']
            collection.delete_one({'geolevel': geo_level.value})
            collection.insert_one(insert)
        except Exception as e:
            print("!!! ERROR storing latestupdatedate run to Mongo!!!", e)
            sys.exit()


def fill_missing_dates(temp_dict, prev_date, month_diff):

    add_date = prev_date + relativedelta(months=1)

    for month_index in range(month_diff):
        temp_dict['realestatetrends']['dates'].append(INDEX_TO_MONTH[add_date.month-1] + ' ' + str(add_date.year))
        temp_dict['realestatetrends']['mediansaleprice'].append(None)
        temp_dict['realestatetrends']['mediansalepricemom'].append(None)
        temp_dict['realestatetrends']['mediansalepriceyoy'].append(None)
        temp_dict['realestatetrends']['mediandom'].append(None)
        temp_dict['realestatetrends']['mediandommom'].append(None)
        temp_dict['realestatetrends']['mediandomyoy'].append(None)
        temp_dict['realestatetrends']['medianppsf'].append(None)
        temp_dict['realestatetrends']['medianlistprice'].append(None)
        temp_dict['realestatetrends']['medianlistpricemom'].append(None)
        temp_dict['realestatetrends']['monthsofsupply'].append(None)
        temp_dict['realestatetrends']['pricedrops'].append(None)
        temp_dict['realestatetrends']['inventory'].append(None)
        temp_dict['realestatetrends']['homessold'].append(None)
        temp_dict['realestatetrends']['soldabovelist'].append(None)
        add_date = add_date + relativedelta(months=1)


def update_existing_historical_profile(insert_list, geoid_field, collection_name, geo_level):
    existing_historical_profiles = mongoclient.query_collection(database_name="MarketProfiles",
                                                                collection_name=collection_name,
                                                                collection_filter={},
                                                                prod_env=ProductionEnvironment.MARKET_PROFILES).drop(columns=["_id"])
    new_insert_list = []
    for updated_profile in insert_list:
        existing_profile = existing_historical_profiles[existing_historical_profiles[geoid_field] == updated_profile[geoid_field]]

        if len(existing_profile) == 0:
            if geo_level == GeoLevels.ZIPCODE:
                new_insert_list.append({
                    'zipcode': updated_profile[geoid_field],
                    'geolevel': geo_level.value,
                    'geoname': updated_profile[geoid_field],
                    'realestatetrends': updated_profile['realestatetrends']
                })
            continue

        existing_profile = existing_profile.iloc[0].to_dict()
        existing_profile['realestatetrends'] = updated_profile['realestatetrends']

        if 'rentaltrends' in existing_profile.keys() and existing_profile['rentaltrends'] != existing_profile['rentaltrends']:
            del existing_profile['rentaltrends']

        new_insert_list.append(existing_profile)

    return new_insert_list


def initialize_historical_profiles(geo_level, collection_name):
    if geo_level == GeoLevels.USA:
        initialize.initialize_market_trends(geo_level=GeoLevels.USA,
                                            default_geoid=DefaultGeoIds.USA,
                                            geoid_field=GeoIdField.USA.value,
                                            geoname_field=GeoNameField.USA.value,
                                            collection_name=collection_name
                                            )
    if geo_level == GeoLevels.CBSA:
        initialize.initialize_market_trends(geo_level=GeoLevels.CBSA,
                                            default_geoid=DefaultGeoIds.CBSA,
                                            geoid_field=GeoIdField.CBSA.value,
                                            geoname_field=GeoNameField.CBSA.value,
                                            collection_name=collection_name
                                            )
    elif geo_level == GeoLevels.COUNTY:
        initialize.initialize_market_trends(geo_level=GeoLevels.COUNTY,
                                            default_geoid=DefaultGeoIds.COUNTY.value,
                                            geoid_field=GeoIdField.COUNTY.value,
                                            geoname_field=GeoNameField.COUNTY.value,
                                            collection_name=collection_name
                                            )
    elif geo_level == GeoLevels.ZIPCODE:
        initialize.initialize_market_trends(geo_level=GeoLevels.ZIPCODE,
                                            default_geoid=DefaultGeoIds.ZIPCODE.value,
                                            geoid_field=GeoIdField.ZIPCODE.value,
                                            geoname_field=GeoNameField.ZIPCODE.value,
                                            collection_name=collection_name
                                            )

