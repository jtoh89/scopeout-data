import pandas as pd
import requests as r
from database import mongoclient
import sys
from enums import ProductionEnvironment, GeoLevels, DefaultGeoIds
import os
from census.censusdata import STATES
import copy
from lookups import MONTH_FORMAT, MONTH_INTEGER
from lookups import OLD_TO_NEW_CBSAID, NECTAMSA_to_CBSA_conversion
from utils.utils import drop_na_values_from_dict
from enums import GeoLevels, Collections_Historical_Profiles

# CURRENT_YEAR = '2021'
# CURRENT_MONTH = 'M01'
MINIMUM_YEAR = 2008
DELIMETER = '\t'

US_UNEMPLOYMENT = 6.3
#https://fred.stlouisfed.org/series/UNRATE

def market_profile_add_unemployment(geo_level, geoid_field, prod_env=ProductionEnvironment.MARKET_PROFILES):
    collection_name = ""
    if geo_level == GeoLevels.USA:
        collection_name = Collections_Historical_Profiles.USA.value
    elif geo_level == GeoLevels.CBSA:
        collection_name = Collections_Historical_Profiles.CBSA.value
    else:
        collection_name = Collections_Historical_Profiles.COUNTY.value

    cbsa_profiles = mongoclient.query_collection(database_name="MarketProfiles",
                                                 collection_name=collection_name,
                                                 collection_filter={},
                                                 prod_env=ProductionEnvironment.MARKET_PROFILES)

    geo_data = mongoclient.query_collection(database_name="CensusData1",
                                                 collection_name="CensusData",
                                                 collection_filter={'geolevel': geo_level.value},
                                                 prod_env=ProductionEnvironment.CENSUS_DATA1)

    insert_list = []
    for i, row in cbsa_profiles.iterrows():
        add_dict = row.to_dict()
        geo_unemployment_data = geo_data[geo_data['geoid'] == add_dict[geoid_field]]

        if len(geo_unemployment_data) > 0:
            if 'Unemployment Historic' in geo_unemployment_data.data.iloc[0].keys():
                geo_unemployment_data = geo_unemployment_data.data.iloc[0]['Unemployment Historic']
                add_dict['historicunemploymentrate'] = {}
                add_dict['historicunemploymentrate']['unemploymentrate'] = geo_unemployment_data['Unemployment Historic']
                add_dict['historicunemploymentrate']['dates'] = geo_unemployment_data['Date']

        add_dict = drop_na_values_from_dict(add_dict)

        insert_list.append(add_dict)

    client = mongoclient.connect_to_client(prod_env=prod_env)
    dbname = 'MarketProfiles'
    db = client[dbname]
    collection = db[collection_name]

    collection_filter = {}

    success = mongoclient.batch_inserts_with_list(insert_list, collection, collection_filter, geoid_field)

    if success:
        print("Successfully stored batch into Mongo. Rows inserted: ", len(insert_list))
        return success




def download_cbsa_unemployment():
    '''
    Function will download unemployment data for cbsa geographies.
    :return:
    '''
    ############################################
    ######## METRO UNEMPLOYMENT
    ############################################

    urls = {
        'micro':'https://download.bls.gov/pub/time.series/la/la.data.62.Micro',
        'metro':'https://download.bls.gov/pub/time.series/la/la.data.60.Metro',
    }

    # DEFINE CURRENT MONTH AND YEAR TO PULL. SET US UNEMPLOYMENT RATE
    # CHECK DATA RELEASES HERE: https://www.bls.gov/schedule/news_release/empsit.htm
    bls_cbsa_data = pd.DataFrame()

    # Parse Data
    for k,v in urls.items():
        df = get_unemployment_df(v)

        for i, row in df.iterrows():
            df.at[i, 'cbsacode'] = row['series_id'][7:12]
            df.at[i, 'geo_type'] = 'cbsa'

        if bls_cbsa_data.empty:
            bls_cbsa_data = df
        else:
            bls_cbsa_data = bls_cbsa_data.append(df)

    for k,v in NECTAMSA_to_CBSA_conversion.items():
        if k in list(bls_cbsa_data[bls_cbsa_data['geo_type'] == 'cbsa']['cbsacode']):
            bls_cbsa_data.loc[bls_cbsa_data['cbsacode'] == k, 'cbsacode'] = v

    # Get list of MSA and State Ids to make sure there is no update we miss
    cbsa_data = mongoclient.query_collection(database_name='Geographies',
                                             collection_name='Cbsa',
                                             collection_filter={},
                                             prod_env=ProductionEnvironment.GEO_ONLY)

    cbsa_df = cbsa_data[['cbsacode']]

    common = bls_cbsa_data.merge(cbsa_df, on=['cbsacode', 'cbsacode'])
    no_match_from_final_df = bls_cbsa_data[~bls_cbsa_data.cbsacode.isin(common.cbsacode)]
    no_match_from_cbsa_df = cbsa_df[~cbsa_df.cbsacode.isin(common.cbsacode)]

    # Make sure every metro is accounted for in data pull
    if not no_match_from_final_df.empty:
        print('!!! ERROR - There are Geo IDs in the current BLS data that are not found in the scopeout cbsa list. Mismatch count: ', len(no_match_from_final_df['cbsacode'].drop_duplicates()))
    if not no_match_from_cbsa_df.empty and (~no_match_from_cbsa_df.cbsacode.isin(['12300'])).any():
        print('!!! ERROR - There are Geo IDs in scopeout cbsa list missing in the current BLS data. Mismatch count: ', len(no_match_from_cbsa_df['cbsacode'].drop_duplicates()))

    common['value'] = common['value'].astype(float)

    cbsa_unemployment = create_unemployment_dict(df=common,
                                                 geoid_field='cbsacode',
                                                 geo_df=cbsa_df)


    success = mongoclient.store_census_data(geo_level=GeoLevels.CBSA,
                                  state_id=DefaultGeoIds.CBSA.value,
                                  filtered_dict=cbsa_unemployment,
                                  prod_env=ProductionEnvironment.CENSUS_DATA1)

    if success:
        print('Successfully stored unemployment data')
    else:
        print('ERROR: Failed to store unemployment data')

def download_county_unemployment():
    '''
    Function will download unemployment data for county geographies.
    :return:
    '''
    urls = {
        'counties': 'https://download.bls.gov/pub/time.series/la/la.data.64.County',
    }

    final_df = pd.DataFrame()

    # Parse Data
    for k,v in urls.items():
        df = get_unemployment_df(v)

        for i, row in df.iterrows():
            if k == 'counties':
                if row['series_id'][5:7] == '72':
                    df.drop(i, inplace=True)
                    continue
                df.at[i, 'countyfullcode'] = row['series_id'][5:10]
                df.at[i, 'geo_type'] = 'Counties'

        if final_df.empty:
            final_df = df
        else:
            final_df = final_df.append(df)

    # Get list of MSA and State Ids to make sure there is no update we miss
    county_data = mongoclient.query_collection(database_name='Geographies',
                                              collection_name='County',
                                              collection_filter={},
                                              prod_env=ProductionEnvironment.GEO_ONLY)

    counties_df = county_data[['countyfullcode', 'fipscountycode']]

    common = final_df.merge(counties_df, on=['countyfullcode', 'countyfullcode'])
    no_match_from_final_df = final_df[~final_df.countyfullcode.isin(common.countyfullcode)]
    no_match_from_geo_names_df = counties_df[~counties_df.countyfullcode.isin(common.countyfullcode)]

    # Make sure every MSA/State is accounted for in data pull
    # no_match_from_final_df should be empty
    # no_match_from_geo_names_df should have 02201, 02232, 02280
    if not no_match_from_final_df.empty:
        print('!!! ERROR - There are Geo IDs in the current BLS data that are not found in the scopeout county list',  len(no_match_from_final_df['countyfullcode'].drop_duplicates()))
    if not no_match_from_geo_names_df.empty and (~no_match_from_geo_names_df.countyfullcode.isin(['02063','02066','15005'])).any():
        print('!!! ERROR - There are Geo IDs in the scopeout county list missing in the current BLS data', len(no_match_from_geo_names_df['countyfullcode'].drop_duplicates()))


    common['value'] = common['value'].astype(float)

    for stateid in STATES:
        counties_to_process = copy.deepcopy(counties_df)
        counties_to_process['stateid'] = counties_to_process['countyfullcode'].str[:2]
        counties_to_process = counties_to_process[counties_to_process['stateid'] == stateid]

        county_unemployment_dict = create_unemployment_dict(df=common,
                                                       geoid_field='countyfullcode',
                                                       geo_df=counties_to_process)


        success = mongoclient.store_census_data(geo_level=GeoLevels.COUNTY,
                                                state_id=None,
                                                filtered_dict=county_unemployment_dict,
                                                prod_env=ProductionEnvironment.CENSUS_DATA1)

        if success:
            print('Successfully stored unemployment data')
        else:
            print('ERROR: Failed to store unemployment data')

def download_usa_unemployment():
    '''
    Function will download unemployment data for USA.
    :return:
    '''
    currpath = os.path.dirname(os.path.abspath(__file__))
    rootpath = os.path.dirname(os.path.abspath(currpath))
    with open(rootpath + '/files/UNRATE.csv') as file:
        df = pd.read_csv(file, dtype=str)

        df[['Year','Month','Day']] = df['DATE'].str.split('-', expand=True)
        df = df.loc[df['Year'].astype(int) >= MINIMUM_YEAR]
        df['geoid'] = DefaultGeoIds.USA.value

        geo_df = pd.DataFrame.from_dict({
            'name': ['United States'],
            'geoid': [DefaultGeoIds.USA.value],
            'stateid': [DefaultGeoIds.USA.value]
        })

        df['UNRATE'] = df['UNRATE'].astype(float)

        df = df.rename(columns={'year':'Year','UNRATE':'Unemployment Historic'})
        us_unemployment_dict = create_unemployment_dict(df=df, geoid_field='geoid', geo_df=geo_df)


        success = mongoclient.store_census_data(geo_level=GeoLevels.USA,
                                                state_id=DefaultGeoIds.USA.value,
                                                filtered_dict=us_unemployment_dict,
                                                prod_env=ProductionEnvironment.CENSUS_DATA1)

        if success:
            print('Successfully stored unemployment data')
        else:
            print('ERROR: Failed to store unemployment data')

def get_unemployment_df(url):
    data = r.get(url)

    row_list = []


    count = 0
    for line in data.text.splitlines():
        if count == 0:
            headers = [x.strip() for x in line.split(DELIMETER)]
            count += 1
        else:
            row = [x.strip() for x in line.split(DELIMETER, maxsplit=len(headers) - 1)]

            # make sure we only look unemployment and unseasonal. Skip M13. Skip Puerto Rico (72)
            if row[0][-2:] != '03' or row[0][2] != 'U' or row[2] == 'M13' or row[0][5:7] == '72':
                continue

            if int(row[1]) < MINIMUM_YEAR:
                continue

            if len(row) != len(headers):
                print('There is a mismatch in columns and values')
                print(row)
                row.append('n/a')
                row_list.append(row)
            else:
                row_list.append(row)
            count += 1

    df = pd.DataFrame(row_list, columns=headers)

    return df

def create_unemployment_dict(df, geoid_field, geo_df):
    df = df.rename(columns={'year':'Year','period':'Month','value':'Unemployment Historic'})
    # df['geolevel'] = GeoLevels.COUNTY.value

    df['MonthName'] = df['Month'].replace(MONTH_FORMAT)
    df['MonthNum'] = df['Month'].replace(MONTH_INTEGER)
    df['Date'] = df['MonthName'] + ' ' + df['Year']
    try:
        df = df.drop(columns=['series_id','footnote_codes','geo_type','Month'])
    except:
        print('No columns in: series_id, footnote_codes, geo_type, Month')

    if df['Unemployment Historic'].isnull().values.any():
        print('!!! ERROR - There are missing unemployment rate values in the current BLS data')
        sys.exit()


    final_data_dict = {}
    for i, row in geo_df.iterrows():
        geo_unemployment_data = df[df[geoid_field] == row[geoid_field]]
        geo_unemployment_data = geo_unemployment_data.sort_values(by=['Year', 'MonthNum'])

        add_dict = {
            'Unemployment Historic': [],
            'Date': [],
        }

        for i, row in geo_unemployment_data.iterrows():
            add_dict['Unemployment Historic'].append(row['Unemployment Historic'])
            add_dict['Date'].append(row['Date'])

            final_data_dict[row[geoid_field]] = {
                'data': {
                    'Unemployment Historic': add_dict
                }
            }

    return final_data_dict

