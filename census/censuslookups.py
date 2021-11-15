import os
import pandas as pd
from database import mongoclient
from enums import ProductionEnvironment

def get_census_lookup():
    '''
    Get census2 variable mappings. Mapping originally from google sheets.
    :return: dataframe
    '''
    currpath = os.path.dirname(os.path.abspath(__file__))
    rootpath = os.path.dirname(os.path.abspath(currpath))
    with open(rootpath + '/files/Census groups to use and map - Mapping.csv') as file:
        df = pd.read_csv(file, dtype=str)
        df = df[['Historical', 'AggregateType', 'Category', 'Sub-Category', 'Description', 'VariableID', 'Groupid', 'OwnerRenterOption']]

        df['Historical'] = df['Historical'].map({'FALSE': False, 'TRUE': True})

    return df



def create_county_to_cbsa_lookup():

    cbsa_data = mongoclient.query_collection(database_name="Geographies",
                                             collection_name="Cbsa",
                                             collection_filter={},
                                             prod_env=ProductionEnvironment.GEO_ONLY)

    counties_to_cbsa = []
    for i, cbsa in cbsa_data.iterrows():
        cbsaid = cbsa['cbsacode']
        cbsaname = cbsa['cbsatitle']
        for county in cbsa['counties']:
            stateid = county['stateinfo']['fipsstatecode']
            counties_to_cbsa.append({
                'countyfullcode': county['countyfullcode'],
                'cbsacode': cbsaid,
                'cbsaname': cbsaname,
                'stateid': stateid
            })

    print(counties_to_cbsa)
    mongoclient.store_county_cbsa_lookup(counties_to_cbsa)




