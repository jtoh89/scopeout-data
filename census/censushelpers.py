from enums import GeoLevels

def calculate_category_percentage(category_sum_dict):
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

            # if total < 99.8 or total > 100.2:
            #     print("Percentages do not add up: {}".format(total))
    else:
        values = percentage_dict.values()
        total = sum(values)

        # if total < 99.8 or total > 100.2:
        #     print("Percentages do not add up: {}".format(total))

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

        # Skip Geography info that is returned from census2 api
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
    aggregate_dict['All'] = calculate_category_percentage(category_sum_dict)
    aggregate_dict['Owners'] = calculate_category_percentage(owners_sum_dict)
    aggregate_dict['Renters'] = calculate_category_percentage(renters_sum_dict)

    return aggregate_dict