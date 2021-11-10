import os
import pandas as pd


def get_census_lookup():
    '''
    Get census variable mappings. Mapping originally from google sheets.
    :return: dataframe
    '''
    currpath = os.path.dirname(os.path.abspath(__file__))
    rootpath = os.path.dirname(os.path.abspath(currpath))
    with open(rootpath + '/files/Census groups to use and map - Mapping.csv') as file:
        df = pd.read_csv(file, dtype=str)
        df = df[['Historical', 'AggregateType', 'Category', 'Sub-Category', 'Description', 'VariableID', 'Groupid', 'OwnerRenterOption']]

        df['Historical'] = df['Historical'].map({'FALSE': False, 'TRUE': True})

    return df




