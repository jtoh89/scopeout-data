import sys

from database import mongoclient
from enums import GeoLevels
from enums import ProductionEnvironment
from globals import CENSUS_ACS_YEAR
from database import mongoclient
from census.censusdata import STATES, STATES1, STATES2, CENSUS_LATEST_YEAR, SCOPEOUT_YEAR
from census.censusdata import calculate_category_percentage
from utils.utils import calculate_percent_change

def update_regional_unemployment(geo_level):
    '''
    Function updates ACS unemployment data with BLS monthly unemployment data for geographies.
    ACS unemployment is under Unemployment. BLS is under Unemployment Historic.
    Only updates Cbsa and County
    :param geo_level:
    :return:
    '''
    states = STATES

    if geo_level == GeoLevels.CBSA:
        states = ['00000']

    for stateid in states:
        print("Start {} update for stateid: {}".format(geo_level.value, stateid))
        collection_filter = {
            'geolevel': {'$eq': geo_level.value},
            'stateid': stateid
        }

        geo_data = mongoclient.query_collection(database_name="CensusData1",
                                                         collection_name="CensusData",
                                                         collection_filter=collection_filter,
                                                         prod_env=ProductionEnvironment.CENSUS_DATA1)

        unemployment_update = {}

        for i, row in geo_data.iterrows():
            if 'Unemployment Historic' not in row['data'].keys():
                print('COULD NOT SET UNEMPLOYMENT. Unemployment Historic is missing. Geocode: ', row['geoid'])
                continue

            current_unemployment = float(row['data']['Unemployment Historic']['Unemployment Historic'][-1:][0])
            most_recent_month = row['data']['Unemployment Historic']['Date'][-1:][0]

            unemployment_census_year_key = '{} Unemployment Rate'.format(CENSUS_LATEST_YEAR)

            if unemployment_census_year_key in row['data']['Unemployment Rate'].keys():
                county_census_unemployment = row['data']['Unemployment Rate'][unemployment_census_year_key]

                unemployment_rate_change = calculate_percent_change(starting_data=county_census_unemployment,
                                                                    ending_data=current_unemployment,
                                                                    move_decimal=False,
                                                                    decimal_places=7)
            else:
                county_census_unemployment = row['data']['Unemployment Rate']['Unemployment Rate']

                unemployment_rate_change = calculate_percent_change(starting_data=county_census_unemployment,
                                                                    ending_data=current_unemployment,
                                                                    move_decimal=False,
                                                                    decimal_places=7)

            unemployment_update[row['geoid']] = {
                'data': {
                    'Unemployment Rate': {
                        'Unemployment Rate': current_unemployment,
                        '{} Unemployment Rate'.format(CENSUS_LATEST_YEAR): county_census_unemployment,
                        'Unemployment Rate % Change': unemployment_rate_change,
                        'Last Update': most_recent_month,
                    }
                }
            }


        success = mongoclient.store_census_data(geo_level=geo_level,
                                                state_id=stateid,
                                                filtered_dict=unemployment_update,
                                                prod_env=ProductionEnvironment.CENSUS_DATA1)



        if success:
            print('Successfully stored unemployment data')
            collection_add_finished_run = {
                'state_id': stateid,
                'geo_level': geo_level.value,
                'category': 'Unemployment Update',
            }
            mongoclient.add_finished_run(collection_add_finished_run)

        else:
            print('ERROR: Failed to store unemployment data')


def  update_tract_unemployment():
    '''
    Function updates all tract unemployment rates using unemployment adjustments calculated from BLS data.
    :return:
    '''

    existing_updates = mongoclient.query_collection(database_name="CensusDataInfo",
                                                    collection_name="FinishedRuns",
                                                    collection_filter={'geo_level':  GeoLevels.TRACT.value,
                                                                       'category': 'Unemployment Update'},
                                                    prod_env=ProductionEnvironment.QA)

    if len(existing_updates) > 0:
        existing_updates = list(existing_updates['state_id'])
    else:
        existing_updates = []

    for stategroupindex, stategroup in enumerate([STATES1, STATES2]):
        prod_env = ProductionEnvironment.CENSUS_DATA1
        db_name = "CensusData1"
        if stategroupindex == 1:
            prod_env = ProductionEnvironment.CENSUS_DATA2
            db_name = "CensusData2"

        for stateid in stategroup:
            if stateid in existing_updates:
                print("Skipping {}. Already ran updates.".format(stateid))
                continue

            print("Running tract unemployment updates for: ", stateid)
            collection_filter = {
                'geolevel': {'$eq': GeoLevels.TRACT.value},
                'stateid': stateid
            }

            geo_data = mongoclient.query_collection(database_name=db_name,
                                                    collection_name="CensusData",
                                                    collection_filter=collection_filter,
                                                    prod_env=prod_env)

            county_filter = {
                'geolevel': GeoLevels.COUNTY.value,
                'stateid': stateid
            }

            county_data = mongoclient.query_collection(database_name="CensusData1",
                                                    collection_name="CensusData",
                                                    collection_filter=county_filter,
                                                    prod_env=ProductionEnvironment.CENSUS_DATA1)

            unemployment_update = {}

            recent_updates = 0

            for i, row in geo_data.iterrows():
                countyfullcode = row.geoinfo['countyfullcode']
                county_data_dict = county_data[county_data['geoid'] == countyfullcode].iloc[0]

                if 'Unemployment Rate' not in county_data_dict['data'].keys() or 'Unemployment Rate % Change' not in county_data_dict['data']['Unemployment Rate'].keys():
                    print('FIX - Why is Unemployment Rate % Change missing')
                    county_unemployment_rate_change = 1
                else:
                    county_unemployment_rate_change = county_data_dict['data']['Unemployment Rate']['Unemployment Rate % Change']


                unemployment_census_year_key = '{} Unemployment Rate'.format(CENSUS_LATEST_YEAR)

                if unemployment_census_year_key in row['data']['Unemployment Rate'].keys():
                    recent_updates += 1
                    tract_census_unemployment = row['data']['Unemployment Rate'][unemployment_census_year_key]
                    unemployment_adjustment = tract_census_unemployment * county_unemployment_rate_change
                    updated_tract_unemployment = round(tract_census_unemployment + unemployment_adjustment, 1)

                    unemployment_update[row['geoid']] = {
                        'data': {
                            'Unemployment Rate': {
                                'Unemployment Rate': updated_tract_unemployment,
                                unemployment_census_year_key: tract_census_unemployment,
                            }
                        }
                    }
                else:
                    recent_updates += 1
                    tract_census_unemployment = row['data']['Unemployment Rate']['Unemployment Rate']
                    unemployment_adjustment = tract_census_unemployment * county_unemployment_rate_change
                    updated_tract_unemployment = round(tract_census_unemployment + unemployment_adjustment, 1)

                    unemployment_update[row['geoid']] = {
                        'data': {
                            'Unemployment Rate': {
                                'Unemployment Rate': updated_tract_unemployment,
                                unemployment_census_year_key: tract_census_unemployment,
                            }
                        }
                    }

            if recent_updates > 0:
                if recent_updates != len(geo_data):
                    print("!!! ERROR - Why is there only partial unemployment in dataset? !!!")
                    sys.exit()

            success = mongoclient.store_census_data(geo_level=GeoLevels.TRACT,
                                                    state_id=stateid,
                                                    filtered_dict=unemployment_update,
                                                    prod_env=prod_env,
                                                    county_batches=True)

            if success:
                print("Finishd updating tract unemployment for stateid: ", stateid)
                collection_add_finished_run = {
                    'scopeout_year': SCOPEOUT_YEAR,
                    'state_id': stateid,
                    'geo_level': GeoLevels.TRACT.value,
                    'category': 'Unemployment Update',
                }
                mongoclient.add_finished_run(collection_add_finished_run)
            else:
                print('ERROR: Failed to store unemployment data')