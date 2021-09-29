import requests as r
import pandas as pd
import numpy as np
from ast import literal_eval
from database import mongoclient
import sys
from census import censusgroupslookups
from collections import ChainMap

# CENSUS_END_YEAR = 2019
# CENSUS_YEARS = [2012, 2013, 2014, 2015, 2016, 2017, 2018, CENSUS_END_YEAR]

CENSUS_END_YEAR = 2013
CENSUS_YEARS = [2012, CENSUS_END_YEAR]

SCOPEOUT_YEAR = 2021
LIMIT_NUM_OF_GEO_IN_QUERY = 3

def get_census_data(geo_level, state_id, variables_df):
    '''
    Stores CensusData object into Mongo. state_id is required to use census api. All geolevels for the state will be stored.

    :param geo_level:
    :param state_id:
    :param variables_df:
    :return:
    '''

    category = variables_df['Category'].iloc[0]
    aggregate_type = variables_df['AggregateType'].iloc[0]
    variable_list = list(variables_df['VariableID'])

    geographies_df = mongoclient.query_geography(geo_level=geo_level)

    state_and_category_exists = check_state_category(geo_level, category, state_id)

    if state_and_category_exists:
        print("!!! Already have category: {} for state: {}".format(category, state_id))
        return
    else:
        print("Starting download for geo_level: {}, state_id: {}, category: {}".format(geo_level, state_id, category))

    results_dict = {}
    geo_id = None

    for year in CENSUS_YEARS:
        query_url = build_query(year, geo_level, state_id, variable_list)
        df = census_api(query_url)

        if len(df) == 0:
            if year == CENSUS_END_YEAR:
                print("!!! DATA DOES NOT EXIST FOR CENSUS_END_YEAR")
                sys.exit()
            continue

        year_string = '{}'.format(year)

        for i, row in df.iterrows():
            if aggregate_type in ['Percentage']:
                category_sum_dict = sum_categories(variable_data_dict=row.to_dict(), variables_df=variables_df)
                aggregate_dict = calculate_percentage(category_sum_dict)
            elif aggregate_type in ['PercentageWithSubCategories']:
                category_sum_dict = sum_categories(variable_data_dict=row.to_dict(), variables_df=variables_df)
                aggregate_dict = calculate_percentage(category_sum_dict)
                check_percentages(aggregate_dict)
            elif aggregate_type == 'PercentageWithSubCategoriesAndOptions':
                aggregate_dict = sum_categories_with_options(variable_data_dict=row.to_dict(), variables_df=variables_df)
                if category != '% Income on Housing Costs':
                    check_percentages(aggregate_dict, True)
            elif aggregate_type == 'PercentageNoTotal':
                category_sum_dict = sum_categories_and_total(variable_data_dict=row.to_dict(), variables_df=variables_df)
                aggregate_dict = calculate_percentage(category_sum_dict)
                check_percentages(aggregate_dict)
            else:
                aggregate_dict = sum_categories(variable_data_dict=row.to_dict(), variables_df=variables_df)


            if geo_level == 'state':
                geo_info = geographies_df.loc[geographies_df['fipsstatecode'] == state_id]
                geo_id = state_id
            elif geo_level == 'county':
                geo_id = row.state + row.county
                geo_info = geographies_df.loc[geographies_df['countyfullcode'] == geo_id]
            elif geo_level == 'tracts':
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
            census_result_object['geolevel'] = geo_level
            census_result_object['data'] = data_dict

            if geo_id in results_dict.keys():
                results_dict[geo_id]['data'][year_string] = census_result_object['data'][year_string]
            else:
                results_dict[geo_id] = census_result_object

        mongoclient.store_census_data(results_dict)

def build_query(year, geo_level, state_id, variable_list):
    '''
    Builds query for census api based on geolevel, stateid.

    :param year:
    :param geo_level:
    :param state_id:
    :param variable_list:
    :return:
    '''

    query_url = ''

    query_variables = ''
    for variable in variable_list:
        query_variables += variable + ','

    query_variables = query_variables[:-1]

    if geo_level == 'state':
        query_url = 'https://api.census.gov/data/{}/acs/acs5?get={}&for=state:{}' \
            .format(str(year), query_variables, state_id)
    elif geo_level == 'county':
        query_url = 'https://api.census.gov/data/{}/acs/acs5?get={}&for=county:*&in=state:{}' \
            .format(str(year), query_variables, state_id)
    elif geo_level == 'tracts':
        query_url = 'https://api.census.gov/data/{}/acs/acs5?get={}&for=tract:*&in=state:{}%20county:*' \
            .format(str(year), query_variables, state_id)

    return query_url

def census_api(query_url):
    '''
    Runs api query provided. Returns dataframe.

    :param query_url:
    :return:
    '''

    print("Sending census api request: {}".format(query_url))

    data = r.get(query_url)

    if data.status_code != 200:
        print("ERROR - bad request: {}".format(data.text))
        return pd.DataFrame()



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

    :param geo_level:
    :param category:
    :param state_id:
    :return:
    '''

    collection_filter = {
        'scopeoutyear':{'$eq': SCOPEOUT_YEAR},
        'geolevel':{'$eq': geo_level},
        'stateid':{'$eq': state_id},
    }

    existing_data_df = mongoclient.query_collection('CensusData', collection_filter)
    census_end_year = str(CENSUS_END_YEAR)
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
    col_dict = dict(zip(variables_df['VariableID'], variables_df['Sub-Category']))

    category_sum_dict = {}
    for k, data in variable_data_dict.items():
        if k in ['state','county','cbsa','tract']:
            continue

        value = int(data)
        category = col_dict[k]

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
        if k in ['state','county','cbsa']:
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

def sum_categories_with_options(variable_data_dict, variables_df):
    col_dict = dict(zip(variables_df['VariableID'], variables_df['Sub-Category']))
    option_dict = dict(zip(variables_df['VariableID'], variables_df['OwnerRenterOption']))

    category_sum_dict = {}
    owners_sum_dict = {}
    renters_sum_dict = {}

    owners_total = 0
    renters_total = 0

    for k, data in variable_data_dict.items():
        if k in ['state','county','cbsa']:
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





def DEPRECATED_get_geographies_to_run(geo_level, category, state_id=None):
    '''
    Checks Mongo which geographies exists for a given census group type.
    :param geo_level:
    :param category:
    :param state_id:
    :return:
    '''

    geo_id_field = None
    geographies_df = mongoclient.query_geography(geo_level=geo_level)

    # Filter by state if possible
    if geo_level == 'state':
        geo_id_field = 'fipsstatecode'
        geographies_df = geographies_df[geographies_df['fipsstatecode'] == state_id]
    elif geo_level == 'county':
        geo_id_field = 'countyfullcode'
        geographies_df['fipsstatecode'] = geographies_df['countyfullcode'].str[:2]
        geographies_df = geographies_df[geographies_df['fipsstatecode'] == state_id]
    elif geo_level == 'tracts':
        geographies_df = geographies_df[geographies_df['fipsstatecode'] == state_id]


    collection_filter = {
        'scopeoutyear':{'$eq': SCOPEOUT_YEAR},
        'geolevel':{'$eq': geo_level},
    }

    existing_data_df = mongoclient.query_collection('CensusData', collection_filter)
    exclude = []
    census_end_year = str(CENSUS_END_YEAR)
    if len(existing_data_df) == 0:
        return geographies_df
    else:
        for i, data in existing_data_df.iterrows():
            if census_end_year in data['data'].keys():
                if category in data['data'][census_end_year].keys():
                    exclude.append(data['geoid'])

        geographies_df = geographies_df[~geographies_df['{}'.format(geo_id_field)].isin(exclude)]

        return geographies_df

def DEPRECATED_build_query2(year, geo_level, state_id, variable_list, geographies_to_run_df):
    query_geoids = ''
    query_url = ''

    geographies_to_run_df = geographies_to_run_df.head(LIMIT_NUM_OF_GEO_IN_QUERY)
    geographies_to_run_df = geographies_to_run_df.reset_index()

    query_variables = ''
    for variable in variable_list:
        query_variables += variable + ','

    query_variables = query_variables[:-1]

    if geo_level == 'state':
        query_url = 'https://api.census.gov/data/{}/acs/acs5?get={}&for=state:{}' \
            .format(str(year), query_variables, state_id)
    elif geo_level == 'county':
        for i, geography in geographies_to_run_df.iterrows():
            query_geoids += geography.fipscountycode + ','

        query_url = 'https://api.census.gov/data/{}/acs/acs5?get={}&for=county:{}&in=state:{}' \
            .format(str(year), query_variables, query_geoids[:-1], state_id)
    elif geo_level == 'tracts':
        current_county = None
        for i, geography in geographies_to_run_df.iterrows():
            #     query_geoids += geography.tractcode + ','
            # query_url = 'https://api.census.gov/data/{}/acs/acs5?get={}&for=tract:{}&in=state:{}%20county:*'\
            #     .format(str(year), query_variables, query_geoids, state_id)

            if i < 1:
                current_county = geography.countyfullcode[2:]

            if current_county != geography.countyfullcode[2:]:
                break
            else:
                query_geoids += geography.tractcode[5:] + ','

        query_url = 'https://api.census.gov/data/{}/acs/acs5?get={}&for=tract:{}&in=state:{}%20county:{}' \
            .format(str(year), query_variables, query_geoids[:-1], state_id, current_county)


    return query_url

def DEPRECATED_build_query(year, geo_level, state_id, variable_list, geographies_to_run_df):
    query_geoids = ''
    query_url = ''

    geographies_to_run_df = geographies_to_run_df.head(LIMIT_NUM_OF_GEO_IN_QUERY)

    query_variables = ''
    all_queries = []
    count = 0
    for variable in variable_list:
        count += 1
        if count > 48:
            all_queries.append(query_variables[:-1])
            query_variables = ''
            count = 0
        else:
            query_variables += variable + ','

    if len(query_variables) > 0:
        all_queries.append(query_variables[:-1])


    if geo_level == 'state':
        # query_url = 'https://api.census.gov/data/{}/acs/acs5?get={}&for=state:{}' \
        #     .format(str(year), query_variables, state_id)
        query_url = 'https://api.census.gov/data/{}/acs/acs5?get={{}}&for=state:{}' \
            .format(str(year), state_id)
    elif geo_level == 'county':
        for i, geography in geographies_to_run_df.iterrows():
            query_geoids += geography.fipscountycode + ','

        query_url = 'https://api.census.gov/data/{}/acs/acs5?get={}&for=county:{}&in=state:{}' \
            .format(str(year), query_variables, query_geoids[:-1], state_id)


    for i, query in all_queries:
        all_queries[i] = query.format(query_variables)
    return query_url
