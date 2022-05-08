from database import mongoclient
from enums import ProductionEnvironment
from lookups import INDEX_TO_MONTH
from census.censusdata import STATES1, STATES2
import numpy as np
from dateutil.relativedelta import relativedelta
import datetime
from utils.utils import number_to_string, calculate_percent_change, month_string_to_datetime, truncate_decimals
import copy

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


COLOR_LEVEL_NA = "#999999"
COLOR_LEVEL_1 = "#ff0000"
COLOR_LEVEL_2 = "#ff7f01"
COLOR_LEVEL_3 = "#ffff01"
COLOR_LEVEL_4 = "#004c00"
COLOR_LEVEL_5 = "#00ff01"


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
    # urlslug = marketname.split(", ")[0].replace('--','-').replace(' ','-').lower() + "-real-estate-market-trends"
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

def calculate_yoy_from_list(median_sale_price, historical_list, latest_update_date, multiply_100=False):
    date_minus_year = latest_update_date['lastupdatedate'] - datetime.timedelta(days=(1*365))

    month_minus_year = INDEX_TO_MONTH[date_minus_year.month-1] + " " + str(date_minus_year.year)

    if len(historical_list['realestatetrends']['dates']) < 12:
        print("Less than a year")
    else:
        if historical_list['realestatetrends']['dates'][-13] == month_minus_year:
            prev_year_median_sale_price = historical_list['realestatetrends']['mediansaleprice'][-13]

            if prev_year_median_sale_price != None:
                if multiply_100:
                    return 100 * calculate_percent_change(prev_year_median_sale_price, median_sale_price, move_decimal=False)
                else:
                    return calculate_percent_change(prev_year_median_sale_price, median_sale_price, move_decimal=False)
            else:
                return None
        else:
            return None

def calculate_mom_or_yoy_from_list(latest_month, latest_value, historical_profile, data_type, type):
    if type == 'mom':
        months = 1
    else:
        months = 12

    index = False
    max_percent_change = 200
    yoy_date_list = copy.deepcopy(historical_profile['realestatetrends']['dates'])
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

    past_month_datetime = month_string_to_datetime(historical_profile['realestatetrends']['dates'][index])
    past_month_median_sale_price = historical_profile['realestatetrends'][data_type][index]

    if past_month_median_sale_price and month_comparison == past_month_datetime:
        percent_change = calculate_percent_change(starting_data=past_month_median_sale_price,
                                                      ending_data=latest_value,
                                                      move_decimal=True,
                                                      decimal_places=2)
        if percent_change < max_percent_change:
            zip_mediansaleprice_mom = percent_change
            return zip_mediansaleprice_mom

    return None


# skipped_geos = []
# too_many_missing_dates = False
# reversed_date_list = list(temp_df['dates'])
# reversed_date_list.reverse()
# for i, date in enumerate(reversed_date_list):
#     if i > 11:
#         break
#
#     prev_index = i + 1
#
#     if prev_index < len(reversed_date_list):
#         prev_date = reversed_date_list[i + 1]
#
#         num_months_between = ((date.year - prev_date.year) * 12) + (date.month - prev_date.month) - 1
#
#         if num_months_between > 3:
#             too_many_missing_dates = True
#             skipped_geos.append(k)
#             break
#
#
# if too_many_missing_dates:
#     print("SKIPPING - too many missing dates")
#     skipped_geos.append(k)
#     continue

# with open("skippedgeos.txt", "w") as outfile:
#     outfile.write("\n".join(skipped_geos))
