import sys
import pandas as pd
import os
import csv
from database import mongoclient
from lookups import MONTH_FORMAT, INDEX_TO_MONTH, REDFIN_PROPERTY_TYPES_LOWERCASE_CONVERSION, REDFIN_MSA_TO_CBSA, REDFIN_COUNTYID_TO_FIPS, REDFIN_USA_TO_FIPS, MONTH_TO_INDEX, INDEX_TO_MONTH
from database import mongoclient
from copy import deepcopy
from realestate import initialize
from enums import GeoLevels, DefaultGeoIds, ProductionEnvironment, GeoIdField, GeoNameField
import datetime
from utils.utils import calculate_percentiles_from_list, string_to_float, string_to_int, nat_to_none
from models import zipcodemarketmap
from realestate import redfin
import numpy as np

REDFIN_MIN_YEAR_ZIPCODES = 2020
REDFIN_MAX_YEAR_ZIPCODES = redfin.REDFIN_MAX_YEAR
REDFIN_PROPERTY_TYPES = ['All Residential', 'Single Family Residential', 'Multi-Family (2-4 Unit)']
REDFIN_PROPERTY_TYPES_LOWERCASE = ['all', 'singlefamily', 'multifamily']
REDFIN_DATA_CATEGORIES = ['median_sale_price', 'median_ppsf', 'months_of_supply', 'median_dom', 'price_drops']

def import_redfin_zipcode_historical_data(geoid_field, prod_env):
    currpath = os.path.dirname(os.path.abspath(__file__))
    rootpath = os.path.dirname(os.path.abspath(currpath))

    file_dir = '/files/zip_code_market_tracker.tsv'

    zipcode_data_dict = {}

    with open(rootpath + file_dir, 'r') as csvfile:
        datareader = csv.reader(csvfile, delimiter='\t')
        headers = []
        for i, row in enumerate(datareader):
            if i < 1:
                headers = row
                continue

            if i % 100000 == 0:
                print("Reading {} total lines from redfin zip file".format(i+1))

            row_dict = dict(zip(headers, row))

            if row_dict['property_type'] != 'All Residential' or row_dict['is_seasonally_adjusted'] != 'f':
                continue

            zipcode = row_dict['region'].replace('Zip Code: ','')

            year, month, day = row_dict['period_end'].split('-')

            if int(year) < REDFIN_MIN_YEAR_ZIPCODES:
                continue

            try:
                median_sale_price = string_to_int(row_dict['median_sale_price'])
                median_sale_price_mom = string_to_float(row_dict['median_sale_price_mom'], 2)
                median_sale_price_yoy = string_to_float(row_dict['median_sale_price_yoy'], 2)
                median_dom = string_to_float(row_dict['median_dom'],2)
                median_dom_mom = string_to_float(row_dict['median_dom_mom'], 2)
                median_dom_yoy = string_to_float(row_dict['median_dom_yoy'], 2)
                median_ppsf = string_to_float(row_dict['median_ppsf'], 2)
                median_ppsf_mom = string_to_float(row_dict['median_ppsf_mom'], 2)
                median_ppsf_yoy = string_to_float(row_dict['median_ppsf_yoy'], 2)
            except Exception as e:
                print(e)

            if zipcode not in zipcode_data_dict.keys():
                zipcode_data_dict[zipcode] = {
                    'realestatetrends': {
                        'dates': [datetime.datetime(int(year), int(month), int(day))],
                        'median_sale_price': [median_sale_price],
                        'median_sale_price_mom': [median_sale_price_mom],
                        'median_sale_price_yoy': [median_sale_price_yoy],
                        'median_dom': [median_dom],
                        'median_dom_mom': [median_dom_mom],
                        'median_dom_yoy': [median_dom_yoy],
                        'median_ppsf': [median_ppsf],
                        'median_ppsf_mom': [median_ppsf_mom],
                        'median_ppsf_yoy': [median_ppsf_yoy]
                    }
                }
            else:
                existing_zipcode_data = zipcode_data_dict[zipcode]['realestatetrends']
                existing_zipcode_data['dates'].append(datetime.datetime(int(year), int(month), int(day)))
                existing_zipcode_data['median_sale_price'].append(median_sale_price)
                existing_zipcode_data['median_sale_price_mom'].append(median_sale_price_mom)
                existing_zipcode_data['median_sale_price_yoy'].append(median_sale_price_yoy)
                existing_zipcode_data['median_dom'].append(median_dom)
                existing_zipcode_data['median_dom_mom'].append(median_dom_mom)
                existing_zipcode_data['median_dom_yoy'].append(median_dom_yoy)
                existing_zipcode_data['median_ppsf'].append(median_ppsf)
                existing_zipcode_data['median_ppsf_mom'].append(median_ppsf_mom)
                existing_zipcode_data['median_ppsf_yoy'].append(median_ppsf_yoy)



    insert_list = []

    for k, zip_data in zipcode_data_dict.items():

        temp_df = pd.DataFrame.from_dict(zip_data['realestatetrends']).sort_values(by='dates')
        temp_df = temp_df.replace({np.nan: None})

        # check if at least 12 months exists
        if len(temp_df) < 12:
            continue

        if temp_df.iloc[len(temp_df)-1].date.year != REDFIN_MAX_YEAR_ZIPCODES:
            print("skipping zipcode: {}. Does not have any data for current year: {}".format(k, REDFIN_MAX_YEAR_ZIPCODES))
            continue

        temp_dict = {
            '{}'.format(geoid_field): k,
            'realestatetrends': {
                'date': [],
                'median_sale_price': [],
                'median_sale_price_mom': [],
                'median_sale_price_yoy': [],
                'median_dom': [],
                'median_dom_mom': [],
                'median_dom_yoy': [],
                'median_ppsf': [],
                'median_ppsf_mom': [],
                'median_ppsf_yoy': []
            }
        }

        prev_month = 1
        prev_year = REDFIN_MIN_YEAR_ZIPCODES
        for i, row in temp_df.iterrows():
            if row['date'].year > prev_year:
                prev_year = row['date'].year
                prev_month = 1

            if row['date'].month != 1 and row['date'].month - 1 != prev_month:
                fill_missing_dates(row, temp_dict, prev_month)
                prev_month = row['date'].month
            else:
                prev_month = row['date'].month
            try:
                temp_dict['realestatetrends']['date'].append(INDEX_TO_MONTH[row['date'].month-1] + ' ' + str(row['date'].year))
            except Exception as e:
                print(e)
                sys.exit()

            temp_dict['realestatetrends']['median_sale_price'].append(nat_to_none(row.median_sale_price))
            temp_dict['realestatetrends']['median_sale_price_mom'].append(nat_to_none(row.median_sale_price_mom))
            temp_dict['realestatetrends']['median_sale_price_yoy'].append(nat_to_none(row.median_sale_price_yoy))
            temp_dict['realestatetrends']['median_dom'].append(nat_to_none(row.median_dom))
            temp_dict['realestatetrends']['median_dom_mom'].append(nat_to_none(row.median_dom_mom))
            temp_dict['realestatetrends']['median_dom_yoy'].append(nat_to_none(row.median_dom_yoy))
            temp_dict['realestatetrends']['median_ppsf'].append(nat_to_none(row.median_ppsf))
            temp_dict['realestatetrends']['median_ppsf_mom'].append(nat_to_none(row.median_ppsf_mom))
            temp_dict['realestatetrends']['median_ppsf_yoy'].append(nat_to_none(row.median_ppsf_yoy))

        insert_list.append(temp_dict)


    client = mongoclient.connect_to_client(prod_env=prod_env)
    dbname = 'MarketProfiles'
    db = client[dbname]
    collection = db['zipcodehistoricalprofiles']

    collection_filter = {}

    success = mongoclient.batch_inserts_with_list(insert_list, collection, collection_filter, geoid_field)

    if not success:
        print("!!! zipcodehistorical batch insert failed !!!", len(insert_list))
        return success

def fill_missing_dates(row, temp_dict, prev_month):
    num_months_between = row['date'].month - prev_month

    # reset temp_dict if a huge gap
    if num_months_between > 3:
        temp_dict['realestatetrends'] = {
            'date': [],
            'median_sale_price': [],
            'median_sale_price_mom': [],
            'median_sale_price_yoy': [],
            'median_dom': [],
            'median_dom_mom': [],
            'median_dom_yoy': [],
            'median_ppsf': [],
            'median_ppsf_mom': [],
            'median_ppsf_yoy': []
        }
    else:
        add_month = prev_month + 1
        for month_index in range(1, num_months_between):
            temp_dict['realestatetrends']['date'].append(INDEX_TO_MONTH[add_month-1] + ' ' + str(row['date'].year))
            temp_dict['realestatetrends']['median_sale_price'].append(None)
            temp_dict['realestatetrends']['median_sale_price_mom'].append(None)
            temp_dict['realestatetrends']['median_sale_price_yoy'].append(None)
            temp_dict['realestatetrends']['median_dom'].append(None)
            temp_dict['realestatetrends']['median_dom_mom'].append(None)
            temp_dict['realestatetrends']['median_dom_yoy'].append(None)
            temp_dict['realestatetrends']['median_ppsf'].append(None)
            temp_dict['realestatetrends']['median_ppsf_mom'].append(None)
            temp_dict['realestatetrends']['median_ppsf_yoy'].append(None)
            add_month += 1


def import_redfin_zipcode_data(geo_level, geoid_field):
    currpath = os.path.dirname(os.path.abspath(__file__))
    rootpath = os.path.dirname(os.path.abspath(currpath))

    geographies_df = mongoclient.query_geography(geo_level=geo_level)
    geo_list = list(geographies_df[geoid_field])

    collection_find_finished_runs = {
        'category': 'realestatetrends',
        'geo_level': geo_level.value
    }

    finished_runs = mongoclient.get_finished_runs(collection_find_finished_runs)

    if len(finished_runs) > 0:
        last_finished_year = finished_runs.year.iloc[0]
    else:
        last_finished_year = 0

    redfin_dict = {}

    file_dir = '/files/zip_code_market_tracker.tsv'

    zipcode_data_dict = {}
    zipcode_data_list_dict = {}

    most_recent_date = datetime.datetime(1900, 1, 1)

    with open(rootpath + file_dir, 'r') as csvfile:
        datareader = csv.reader(csvfile, delimiter='\t')

        headers = []
        for i, row in enumerate(datareader):
            if i < 1:
                headers = row
                continue

            row_dict = dict(zip(headers, row))

            if row_dict['property_type'] != 'All Residential' or row_dict['is_seasonally_adjusted'] != 'f':
                continue

            zipcode = row_dict['region'].replace('Zip Code: ','')

            year, month, day = row_dict['period_end'].split('-')

            row_date = datetime.datetime(int(year), int(month), int(day))

            try:
                median_sale_price = string_to_float(row_dict['median_sale_price'],0)
                median_sale_price_mom = string_to_float(row_dict['median_sale_price_mom'], 2)
                median_sale_price_yoy = string_to_float(row_dict['median_sale_price_yoy'], 2)
                median_dom = string_to_float(row_dict['median_dom'],2)
                median_dom_mom = string_to_float(row_dict['median_dom_mom'], 2)
                median_dom_yoy = string_to_float(row_dict['median_dom_yoy'], 2)
                median_ppsf = string_to_float(row_dict['median_ppsf'], 2)
                median_ppsf_mom = string_to_float(row_dict['median_ppsf_mom'], 2)
                median_ppsf_yoy = string_to_float(row_dict['median_ppsf_yoy'], 2)
            except Exception as e:
                print(e)

            if zipcode not in zipcode_data_dict.keys():
                zipcode_data_dict[zipcode] = {
                    'date': datetime.datetime(int(year), int(month), int(day)),
                    'median_sale_price': median_sale_price,
                    'median_sale_price_mom': median_sale_price_mom,
                    'median_sale_price_yoy': median_sale_price_yoy,
                    'median_dom': median_dom,
                    'median_dom_mom': median_dom_mom,
                    'median_dom_yoy': median_dom_yoy,
                    'median_ppsf': median_ppsf,
                    'median_ppsf_mom': median_ppsf_mom,
                    'median_ppsf_yoy': median_ppsf_yoy
                }
            else:
                existing_zipcode_data = zipcode_data_dict[zipcode]

                if row_date > existing_zipcode_data['date']:
                    zipcode_data_dict[zipcode] = {
                        'date': datetime.datetime(int(year), int(month), int(day)),
                        'median_sale_price': median_sale_price,
                        'median_sale_price_mom': median_sale_price_mom,
                        'median_sale_price_yoy': median_sale_price_yoy,
                        'median_dom': median_dom,
                        'median_dom_mom': median_dom_mom,
                        'median_dom_yoy': median_dom_yoy,
                        'median_ppsf': median_ppsf,
                        'median_ppsf_mom': median_ppsf_mom,
                        'median_ppsf_yoy': median_ppsf_yoy
                    }

            if row_date > most_recent_date:
                most_recent_date = row_date

    insert_list = []

    for k, v in zipcode_data_dict.items():
        if v['date'] != most_recent_date:
            continue

        insert_list.append({'zipcode': k,
                            'date': INDEX_TO_MONTH[v['date'].month] + ' ' + str(v['date'].year),
                            'median_sale_price': v['median_sale_price'],
                            'median_sale_price_mom': v['median_sale_price_mom'],
                            'median_sale_price_yoy': v['median_sale_price_yoy'],
                            'median_dom': v['median_dom'],
                            'median_dom_mom': v['median_dom_mom'],
                            'median_dom_yoy': v['median_dom_yoy'],
                            'median_ppsf': v['median_ppsf'],
                            'median_ppsf_mom': v['median_ppsf_mom'],
                            'median_ppsf_yoy': v['median_ppsf_yoy']
                            })

    mongoclient.insert_list_mongo(list_data=insert_list,
                                  dbname='MarketProfiles',
                                  collection_name='zipcodemarketprofile',
                                  prod_env=ProductionEnvironment.MARKET_PROFILES,
                                  collection_update_existing={})
