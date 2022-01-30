from database import mongoclient
from enums import ProductionEnvironment

def list_length_okay(list, limit):
    if len(list) > limit:
        return False
    return True

def calculate_percent_change(starting_data, ending_data, move_decimal=True, decimal_places=1):
    if starting_data == 0 or ending_data == 0:
        return 0

    if move_decimal:
        percent_change = round((ending_data - starting_data) / starting_data * 100, decimal_places)
    else:
        percent_change = round((ending_data - starting_data) / starting_data, decimal_places)


    return percent_change



def get_county_cbsa_lookup(state_id):
    if state_id == '':
        collection_filter = {}
    else:
        collection_filter = {'stateid': {'$eq': state_id}}

    counties_to_cbsa = mongoclient.query_collection(database_name="Geographies",
                                                    collection_name="CountyToCbsa",
                                                    collection_filter=collection_filter,
                                                    prod_env=ProductionEnvironment.GEO_ONLY)

    county_cbsa_lookup = counties_to_cbsa[['countyfullcode', 'cbsacode', 'cbsaname']]

    return  county_cbsa_lookup


def check_dataframe_has_one_record(df):
    if len(df) == 0:
        return False
    elif len(df) > 1:
        print('!!! WARN - more than one record found for dataframe.')
        return False
    else:
        return True

def drop_na_values_from_dict(dict):
    return {k: v for k, v in dict.items() if v == v}

def set_na_to_false_from_dict(dict):
    for k, v in dict.items():
        if v != v:
            dict[k] = False