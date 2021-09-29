import os
import pandas as pd


def get_census_lookup():
    currpath = os.path.dirname(os.path.abspath(__file__))
    rootpath = os.path.dirname(os.path.abspath(currpath))
    with open(rootpath + '/files/Census groups to use and map - Mapping.csv') as file:
        df = pd.read_csv(file, dtype=str)
        df = df[['AggregateType', 'Category', 'Sub-Category', 'Description', 'VariableID', 'Groupid', 'OwnerRenterOption']]

    return df




