import requests as r
import pandas as pd
from ast import literal_eval
from database import mongoclient
import sys
from enums import GeoLevels
from census import censusgroupslookups
import time

CENSUS_LATEST_YEAR = 2019
CENSUS_YEARS = [2012, 2013, 2014, 2015, 2016, 2017, 2018, CENSUS_LATEST_YEAR]

# CENSUS_END_YEAR = 2013
# CENSUS_YEARS = [2012, CENSUS_END_YEAR]

SCOPEOUT_YEAR = 2021

STATES = [
    '01','02','04','05','06','08','09','10','11','12','13','15','16','17','18','19','20','21','22','23','24',
    '25','26','27','28','29','30','31','32','33','34','35','36', '37','38','39','40','41','42','44','45','46',
    '47','48','49','50','51','53','54','55','56'
]

def run_census_data_import(geo_level):
    lookups = censusgroupslookups.get_census_lookup()
    all_categories = lookups['Category'].drop_duplicates()

    if geo_level == GeoLevels.USA:
        for i, category in all_categories.items():
            # Filter look ups for current category
            variables_df = lookups[lookups['Category'] == category]
            # variables_df = lookups[lookups['Category'] == "Population Growth"]

            success = get_and_store_census_data_USA(geo_level=geo_level, variables_df=variables_df)

            if not success:
                print("*** END RUN - get_and_store_census_data Failed ***")
                sys.exit()
    else:
        finished_runs = mongoclient.get_finished_runs(geo_level, SCOPEOUT_YEAR)
        geographies_df = mongoclient.query_geography(geo_level=geo_level)

        for stateid in STATES:
            # if stateid in finished_runs['state_id'].values:
            #     print("Skipping because finished run record found. Stateid: ", stateid)
            #     continue

            print('Starting import for stateid: ', stateid)

            # Wait 3 seconds because Census API tends to fail with too many consecutive requests
            # print("Waiting 3 seconds before starting import")
            # time.sleep(3)

            finished_cats = finished_runs[finished_runs['state_id'] == stateid]['category'].values

            for i, category in all_categories.items():
                if category in finished_cats:
                    print("Skipping category: " + category + ". State: ", stateid)
                    continue
                # Filter look ups for current category
                variables_df = lookups[lookups['Category'] == category]
                # variables_df = lookups[lookups['Category'] == "Population Growth"]

                success = get_and_store_census_data(geo_level=geo_level, state_id=stateid, variables_df=variables_df, geographies_df=geographies_df)

                if not success:
                    print("*** END RUN - get_and_store_census_data Failed ***")
                    sys.exit()

                mongoclient.add_finished_run(geo_level, stateid, SCOPEOUT_YEAR, category)




def get_and_store_census_data(geo_level, state_id, variables_df, geographies_df):
    '''
    Stores CensusData object into Mongo. state_id is required to use census api. All geolevels for the state will be stored.
    If mongo inserts are successfull, function will return true. If not, it will return false.
    :param geo_level:
    :param state_id:
    :param variables_df:
    :return: bool
    '''

    category = variables_df['Category'].iloc[0]
    aggregate_type = variables_df['AggregateType'].iloc[0]
    historical = variables_df['Historical'].iloc[0]
    variable_list = list(variables_df['VariableID'])

    state_and_category_exists = check_state_category(geo_level, category, state_id)
    if state_and_category_exists:
        print("!!! Already have category: {} for state: {}".format(category, state_id))
        return
    else:
        print("Starting download for geo_level: {}, state_id: {}, category: {}".format(geo_level.value, state_id, category))

    # geographies_df = mongoclient.query_geography(geo_level=geo_level)

    results_dict = {}
    geo_id = None

    for year in CENSUS_YEARS:
        # if we don't need historical then skip until we reach CENSUS_END_YEAR
        if not historical and year != CENSUS_LATEST_YEAR:
            continue

        query_url = build_query(year, geo_level, state_id, variable_list)
        df = census_api(query_url)

        if len(df) == 0:
            if year == CENSUS_LATEST_YEAR:
                print("!!! DATA DOES NOT EXIST FOR CENSUS_END_YEAR")
                sys.exit()
            continue

        year_string = '{}'.format(year)

        if year == CENSUS_LATEST_YEAR:
            year_string = 'LatestYear'

        for i, row in df.iterrows():
            if aggregate_type in ['Percentage']:
                category_sum_dict = sum_categories(variable_data_dict=row.to_dict(), variables_df=variables_df)
                aggregate_dict = calculate_percentage(category_sum_dict)
            elif aggregate_type in ['PercentageWithSubCategories']:
                category_sum_dict = sum_categories(variable_data_dict=row.to_dict(), variables_df=variables_df)
                aggregate_dict = calculate_percentage(category_sum_dict)
                check_percentages(aggregate_dict)
            elif aggregate_type == 'PercentageWithSubCategoriesWithOwnerRenter':
                aggregate_dict = sum_categories_with_owner_renter(variable_data_dict=row.to_dict(), variables_df=variables_df)
                if category != '% Income on Housing Costs':
                    check_percentages(aggregate_dict, True)
            elif aggregate_type == 'PercentageNoTotal':
                category_sum_dict = sum_categories_and_total(variable_data_dict=row.to_dict(), variables_df=variables_df)
                aggregate_dict = calculate_percentage(category_sum_dict)
                check_percentages(aggregate_dict)
            else:
                # path for aggregate_type None
                aggregate_dict = sum_categories(variable_data_dict=row.to_dict(), variables_df=variables_df)


            if geo_level == GeoLevels.STATE:
                geo_info = geographies_df.loc[geographies_df['fipsstatecode'] == state_id]
                geo_id = state_id
            elif geo_level == GeoLevels.COUNTY:
                geo_id = row.state + row.county
                geo_info = geographies_df.loc[geographies_df['countyfullcode'] == geo_id]
            elif geo_level == GeoLevels.TRACT:
                geo_id = row.state + row.county + row.tract
                geo_info = geographies_df.loc[geographies_df['tractcode'] == geo_id]

            if len(geo_info) < 1:
                mongoclient.store_missing_geo(geo_id=geo_id, geo_level=geo_level, category=category)
                continue

            data_dict = {}
            data_dict[year_string] = {
                '{}'.format(category): aggregate_dict
            }

            census_result_object = {}
            census_result_object['scopeoutyear'] = SCOPEOUT_YEAR
            census_result_object['geoinfo'] = geo_info.to_dict('records')[0]
            census_result_object['geoid'] = geo_id
            census_result_object['stateid'] = state_id
            census_result_object['geolevel'] = geo_level.value
            census_result_object['data'] = data_dict

            if geo_id in results_dict.keys():
                results_dict[geo_id]['data'][year_string] = census_result_object['data'][year_string]
            else:
                results_dict[geo_id] = census_result_object

    return mongoclient.store_census_data(results_dict)


def get_and_store_census_data_USA(geo_level, variables_df):
    '''
    Stores CensusData object into Mongo. This only stores USA.
    If mongo inserts are successfull, function will return true. If not, it will return false.
    :param geo_level:
    :param state_id:
    :param variables_df:
    :return: bool
    '''

    category = variables_df['Category'].iloc[0]
    aggregate_type = variables_df['AggregateType'].iloc[0]
    historical = variables_df['Historical'].iloc[0]
    variable_list = list(variables_df['VariableID'])

    results_dict = {}

    for year in CENSUS_YEARS:
        # if we don't need historical then skip until we reach CENSUS_END_YEAR
        if not historical and year != CENSUS_LATEST_YEAR:
            continue

        query_url = build_query(year, geo_level, None, variable_list)
        df = census_api(query_url)

        if len(df) == 0:
            if year == CENSUS_LATEST_YEAR:
                print("!!! DATA DOES NOT EXIST FOR CENSUS_END_YEAR")
                sys.exit()
            continue

        year_string = '{}'.format(year)

        if year == CENSUS_LATEST_YEAR:
            year_string = 'LatestYear'

        for i, row in df.iterrows():
            if aggregate_type in ['Percentage']:
                category_sum_dict = sum_categories(variable_data_dict=row.to_dict(), variables_df=variables_df)
                aggregate_dict = calculate_percentage(category_sum_dict)
            elif aggregate_type in ['PercentageWithSubCategories']:
                category_sum_dict = sum_categories(variable_data_dict=row.to_dict(), variables_df=variables_df)
                aggregate_dict = calculate_percentage(category_sum_dict)
                check_percentages(aggregate_dict)
            elif aggregate_type == 'PercentageWithSubCategoriesWithOwnerRenter':
                aggregate_dict = sum_categories_with_owner_renter(variable_data_dict=row.to_dict(), variables_df=variables_df)
                if category != '% Income on Housing Costs':
                    check_percentages(aggregate_dict, True)
            elif aggregate_type == 'PercentageNoTotal':
                category_sum_dict = sum_categories_and_total(variable_data_dict=row.to_dict(), variables_df=variables_df)
                aggregate_dict = calculate_percentage(category_sum_dict)
                check_percentages(aggregate_dict)
            else:
                # path for aggregate_type None
                aggregate_dict = sum_categories(variable_data_dict=row.to_dict(), variables_df=variables_df)

            geo_id = '99999'

            data_dict = {}
            data_dict[year_string] = {
                '{}'.format(category): aggregate_dict
            }

            census_result_object = {}
            census_result_object['scopeoutyear'] = SCOPEOUT_YEAR
            census_result_object['geoinfo'] = { 'name': 'United States' }
            census_result_object['geoid'] = "99999"
            census_result_object['stateid'] = "99999"
            census_result_object['geolevel'] = geo_level.value
            census_result_object['data'] = data_dict

            if geo_id in results_dict.keys():
                results_dict[geo_id]['data'][year_string] = census_result_object['data'][year_string]
            else:
                results_dict[geo_id] = census_result_object

    return mongoclient.store_census_data(results_dict)


def build_query(year, geo_level, state_id, variable_list):
    '''
    Builds query for census api based on geolevel, stateid.
    State will request state data for state_id.
    County will request all counties in state_id.
    Tract will request all tracts in all counties in state_id

    :param year:
    :param geo_level:
    :param state_id:
    :param variable_list:
    :return: string
    '''

    query_url = ''

    query_variables = ''
    for variable in variable_list:
        query_variables += variable + ','

    query_variables = query_variables[:-1]

    if geo_level == GeoLevels.STATE:
        query_url = 'https://api.census.gov/data/{}/acs/acs5?get={}&for=state:{}' \
            .format(str(year), query_variables, state_id)
    elif geo_level == GeoLevels.CBSA:
        query_url = 'https://api.census.gov/data/{}/acs/acs5?get={}&for=metropolitan%20statistical%20area/micropolitan%20statistical%20area:*'.format(str(year), query_variables)
    elif geo_level == GeoLevels.COUNTY:
        query_url = 'https://api.census.gov/data/{}/acs/acs5?get={}&for=county:*&in=state:{}' \
            .format(str(year), query_variables, state_id)
    elif geo_level == GeoLevels.TRACT:
        query_url = 'https://api.census.gov/data/{}/acs/acs5?get={}&for=tract:*&in=state:{}%20county:*' \
            .format(str(year), query_variables, state_id)
    elif geo_level == GeoLevels.USA:
        query_url = 'https://api.census.gov/data/{}/acs/acs5?get={}&for=us:1'\
            .format(str(year), query_variables)

    query_url = query_url + '&key=32e910ae670ecaa2d3bb41d33bebf6e9131a500d'

    return query_url

def census_api(query_url):
    '''
    Runs api query provided. Returns dataframe.

    :param query_url:
    :return: dataframe
    '''

    print("Sending census api request: {}".format(query_url))

    retry_count = 0
    retry = True

    while retry:
        if retry_count > 4:
            return pd.DataFrame()

        try:
            data = r.get(query_url)
        except:
            print("Error making GET request. Trying again")
            print("Sleep 5 seconds before retrying census api calls")
            time.sleep(5)
            retry_count += 1
            continue

        if data.status_code != 200:
            retry_count += 1
            print("ERROR - bad request: {}".format(data.text))
            print("Sleep 5 seconds before retrying census api calls")
            time.sleep(5)
            continue


        row_list = []
        headers = []
        count = 0

        for line in data.text.splitlines():
            if line[:2] == '[[':
                line = line[1:-1]
            elif line[-2:] == ']]':
                line = line[:-1]
            else:
                line = line[:-1]

            if 'null' in line:
                line = line.replace('null', '0')

            line_list = literal_eval(line)
            if count == 0:
                headers = line_list
                count += 1
            else:
                if len(line_list) != len(headers):
                    raise ValueError('There is a mismatch in columns and values')
                else:
                    row_list.append(line_list)
                count += 1

        df = pd.DataFrame(row_list, columns=headers)
        df = df.reset_index(drop=True)

        return df

def check_state_category(geo_level, category, state_id=None):
    '''
    Checks if census data already exists for geo level and category for the state.
    Function will check if data exists for the CENSUS_END_YEAR. If a record is found with the
    end year, function will return true.

    :param geo_level:
    :param category:
    :param state_id:
    :return: bool
    '''

    collection_filter = {
        'scopeoutyear': {'$eq': SCOPEOUT_YEAR},
        'geolevel': {'$eq': geo_level.value},
        'stateid': {'$eq': state_id},
    }


    existing_data_df = mongoclient.query_collection(database_name='scopeout',
                                                    collection_name='CensusData',
                                                    collection_filter=collection_filter,
                                                    prod_env="prod")
    census_end_year = str(CENSUS_LATEST_YEAR)
    if len(existing_data_df) == 0:
        return False
    else:
        existing_data_df = existing_data_df[existing_data_df['stateid'] == state_id]
        first_record = existing_data_df.iloc[0]['data']
        if census_end_year in first_record.keys():
            if category in existing_data_df.iloc[0]['data'][census_end_year].keys():
                return True
        return False

def calculate_percentage(category_sum_dict):
    '''
    Function iterates through categories and gets percentage by dividing each sum with total value.
    :param category_sum_dict: dictionary
    :return: dictionary
    '''
    percentage_dict = {}
    total = category_sum_dict['Total']
    del category_sum_dict['Total']

    for k, v in category_sum_dict.items():
        if total == 0:
            percentage_dict[k] = 0
        else:
            percentage_dict[k] = round(v / total * 100, 1)

    return percentage_dict

def check_percentages(percentage_dict, options=False):
    if options:
        for k, v in percentage_dict.items():
            values = v.values()
            total = sum(values)

            if total < 99.8 or total > 100.2:
                print("Percentages do not add up: {}".format(total))
    else:
        values = percentage_dict.values()
        total = sum(values)

        if total < 99.8 or total > 100.2:
            print("Percentages do not add up: {}".format(total))

def sum_categories(variable_data_dict, variables_df):
    '''
    Function will sum all values based on subcategories (Eg. Master's degree and Doctorate degree grouped under Master's/Doctorate),

    :param variable_data_dict:
    :param variables_df:
    :return: dataframe
    '''

    # Create dictionary mapping variableid to subcategory name
    col_dict = dict(zip(variables_df['VariableID'], variables_df['Sub-Category']))

    category_sum_dict = {}
    for k, data in variable_data_dict.items():
        # Skip Geography info that is returned from census api
        if k in [GeoLevels.CBSA.value, GeoLevels.STATE.value, GeoLevels.COUNTY.value, GeoLevels.TRACT.value, GeoLevels.USA.value]:
            continue

        value = int(data)
        category = col_dict[k]

        # Create dictionary with categories and sum up values
        if category in category_sum_dict:
            category_sum_dict[category] = value + category_sum_dict[category]
        else:
            category_sum_dict[category] = value

    return category_sum_dict

def sum_categories_and_total(variable_data_dict, variables_df):
    col_dict = dict(zip(variables_df['VariableID'], variables_df['Sub-Category']))

    category_sum_dict = {}
    total = 0
    for k, data in variable_data_dict.items():
        if k in [GeoLevels.CBSA.value, GeoLevels.STATE.value, GeoLevels.COUNTY.value, GeoLevels.USA.value, GeoLevels.TRACT.value]:
            continue

        value = int(data)
        category = col_dict[k]

        if category in category_sum_dict:
            category_sum_dict[category] = value + category_sum_dict[category]
        else:
            category_sum_dict[category] = value

        total += value

    category_sum_dict['Total'] = total

    return category_sum_dict

def sum_categories_with_owner_renter(variable_data_dict, variables_df):
    '''

    :param variable_data_dict:
    :param variables_df:
    :return:
    '''

    # Create dict mapping variableid to subcategory name
    col_dict = dict(zip(variables_df['VariableID'], variables_df['Sub-Category']))

    # Create dict mapping variableid to owner or renter
    option_dict = dict(zip(variables_df['VariableID'], variables_df['OwnerRenterOption']))

    category_sum_dict = {}
    owners_sum_dict = {}
    renters_sum_dict = {}

    owners_total = 0
    renters_total = 0

    for k, data in variable_data_dict.items():
        if k in [GeoLevels.CBSA.value, GeoLevels.STATE.value, GeoLevels.COUNTY.value, GeoLevels.TRACT.value, GeoLevels.USA.value]:
            continue

        value = int(data)
        category = col_dict[k]
        option = option_dict[k]

        if category not in category_sum_dict:
            category_sum_dict[category] = value
        else:
            category_sum_dict[category] = value + category_sum_dict[category]

        if option == 'Owner':
            owners_total += value
            if category not in owners_sum_dict:
                owners_sum_dict[category] = value
            else:
                owners_sum_dict[category] = value + owners_sum_dict[category]
        elif option == 'Renter':
            renters_total += value
            if category not in renters_sum_dict:
                renters_sum_dict[category] = value
            else:
                renters_sum_dict[category] = value + renters_sum_dict[category]

    owners_sum_dict['Total'] = owners_total
    renters_sum_dict['Total'] = renters_total

    aggregate_dict = {}
    aggregate_dict['All'] = calculate_percentage(category_sum_dict)
    aggregate_dict['Owners'] = calculate_percentage(owners_sum_dict)
    aggregate_dict['Renters'] = calculate_percentage(renters_sum_dict)

    return aggregate_dict


