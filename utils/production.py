from database import mongoclient
from enums import ProductionEnvironment
from lookups import INDEX_TO_MONTH
from census.censusdata import STATES1, STATES2
import numpy as np
from dateutil.relativedelta import relativedelta
import datetime
from utils.utils import number_to_string, calculate_percent_change, month_string_to_datetime, truncate_decimals
import copy
from globals import COLOR_LEVEL_NA, COLOR_LEVEL_1, COLOR_LEVEL_2, COLOR_LEVEL_3, COLOR_LEVEL_4, COLOR_LEVEL_5


def calculate_percentiles_from_list(list_data):
    final_list = []
    for val in list_data:
        if val != None :
            final_list.append(val)

    np_list = np.array(final_list)
    return {
        "percentile_20": int(round(np.percentile(np_list, 20), 0)),
        "percentile_40": int(round(np.percentile(np_list, 40), 0)),
        "percentile_60": int(round(np.percentile(np_list, 60), 0)),
        "percentile_80": int(round(np.percentile(np_list, 80), 0))
    }

def calculate_percentiles_from_percent_list(list_data):
    final_list = []
    for val in list_data:
        if val != None :
            if val < 0:
                continue
            final_list.append(val)

    np_list = np.array(final_list)


    return {
        "percentile_20": 0,
        "percentile_40": 3,
        "percentile_60": 5,
        "percentile_80": 10
    }


def calculate_percentiles_using_dict(label_dict):
    return {
        "percentile_20": label_dict['percentile_20'],
        "percentile_40": label_dict['percentile_40'],
        "percentile_60": label_dict['percentile_60'],
        "percentile_80": label_dict['percentile_80']
    }



def calculate_percentiles_by_median_value(median_value):
    return {
        "percentile_20":  median_value * .6,
        "percentile_40": median_value * .8,
        "percentile_60": median_value * 1.2,
        "percentile_80":  median_value * 1.4
    }

def assign_legend_details(legend_details, percentiles_dict, data_type, order):
    if data_type == "dollar":
        legend_details.level1description = "Under " + number_to_string(data_type, percentiles_dict['percentile_20'])
        legend_details.level2description = number_to_string(data_type, percentiles_dict['percentile_20']) + " to " + number_to_string(data_type, percentiles_dict['percentile_40'])
        legend_details.level3description = number_to_string(data_type, percentiles_dict['percentile_40']) + " to " + number_to_string(data_type, percentiles_dict['percentile_60'])
        legend_details.level4description = number_to_string(data_type, percentiles_dict['percentile_60']) + " to " + number_to_string(data_type, percentiles_dict['percentile_80'])
        legend_details.level5description = number_to_string(data_type, percentiles_dict['percentile_80']) + " or More"
    elif data_type == "percent":
        legend_details.level1description = "Under " + number_to_string(data_type, percentiles_dict['percentile_20'])
        legend_details.level2description = number_to_string(data_type, percentiles_dict['percentile_20']) + " to " + number_to_string(data_type, percentiles_dict['percentile_40'])
        legend_details.level3description = number_to_string(data_type, percentiles_dict['percentile_40']) + " to " + number_to_string(data_type, percentiles_dict['percentile_60'])
        legend_details.level4description = number_to_string(data_type, percentiles_dict['percentile_60']) + " to " + number_to_string(data_type, percentiles_dict['percentile_80'])
        legend_details.level5description = number_to_string(data_type, percentiles_dict['percentile_80']) + " or More"
    else:
        legend_details.level1description = "Under " + str(percentiles_dict['percentile_20'])
        legend_details.level2description = str(percentiles_dict['percentile_20']) + " to " + str(percentiles_dict['percentile_40'])
        legend_details.level3description = str(percentiles_dict['percentile_40']) + " to " + str(percentiles_dict['percentile_60'])
        legend_details.level4description = str(percentiles_dict['percentile_60']) + " to " + str(percentiles_dict['percentile_80'])
        legend_details.level5description = str(percentiles_dict['percentile_80']) + " or More"

    if order == "ascending":
        legend_details.level1color = COLOR_LEVEL_1
        legend_details.level2color = COLOR_LEVEL_2
        legend_details.level3color = COLOR_LEVEL_3
        legend_details.level4color = COLOR_LEVEL_4
        legend_details.level5color = COLOR_LEVEL_5
    elif order == "descending":
        legend_details.level1color = COLOR_LEVEL_5
        legend_details.level2color = COLOR_LEVEL_4
        legend_details.level3color = COLOR_LEVEL_3
        legend_details.level4color = COLOR_LEVEL_2
        legend_details.level5color = COLOR_LEVEL_1

def assign_color(value, percentiles_dict, order):
    if value != value or value is None:
        return COLOR_LEVEL_NA

    if order == "ascending":
        if value < percentiles_dict['percentile_20']:
            return COLOR_LEVEL_1
        elif value < percentiles_dict['percentile_40']:
            return COLOR_LEVEL_2
        elif value < percentiles_dict['percentile_60']:
            return COLOR_LEVEL_3
        elif value < percentiles_dict['percentile_80']:
            return COLOR_LEVEL_4
        else:
            return COLOR_LEVEL_5
    elif order == "descending":
        if value < percentiles_dict['percentile_20']:
            return COLOR_LEVEL_5
        if value < percentiles_dict['percentile_40']:
            return COLOR_LEVEL_4
        if value < percentiles_dict['percentile_60']:
            return COLOR_LEVEL_3
        if value < percentiles_dict['percentile_80']:
            return COLOR_LEVEL_2
        else:
            return COLOR_LEVEL_1

def get_prod_by_stateid(stateid):
    if stateid in STATES1:
        return ProductionEnvironment.FULL_NEIGHBORHOOD_PROFILES_1
    else:
        return ProductionEnvironment.FULL_NEIGHBORHOOD_PROFILES_2

def create_url_slug(cbsacode, marketname):
    urlslug = marketname.replace(', ','-').replace('--','-').replace(' ','-').lower() + "-real-estate-market-trends"
    if (cbsacode) == "17980":
        urlslug = marketname.split(", ")[0].replace('--','-').replace(' ','-').lower() + "GA-AL-real-estate-market-trends"

    return urlslug

def get_county_cbsa_lookup(state_id):
    if state_id == '':
        collection_filter = {}
    else:
        collection_filter = {'stateid': {'$eq': state_id}}

    counties_to_cbsa = mongoclient.query_collection(database_name="Geographies",
                                                    collection_name="CountyByCbsa",
                                                    collection_filter=collection_filter,
                                                    prod_env=ProductionEnvironment.GEO_ONLY)

    county_cbsa_lookup = counties_to_cbsa[['countyfullcode', 'cbsacode', 'cbsaname']]

    return county_cbsa_lookup


def check_dataframe_has_one_record(df):
    if len(df) == 0:
        return False
    elif len(df) > 1:
        print('!!! WARN - more than one record found for dataframe.')
        return False
    else:
        return True
