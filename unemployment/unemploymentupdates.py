from database import mongoclient
from enums import GeoLevels
from enums import ProductionEnvironment
from globals import CENSUS_ACS_YEAR
from database import mongoclient
from census.censusdata import STATES, STATES1, STATES2
from census.censusdata import CENSUS_LATEST_YEAR
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

            update_unemployment = 0

            current_unemployment = float(row['data']['Unemployment Historic']['Unemployment Historic'][-1:][0])
            acs_unemployment = float(row['data']['Unemployment Rate']['2019 Unemployment Rate'])

            # if geo_level == GeoLevels.CBSA or geo_level == GeoLevels.COUNTY:




            unemployment_rate_change = calculate_percent_change(starting_data=acs_unemployment,
                                                                ending_data=current_unemployment,
                                                                move_decimal=False,
                                                                decimal_places=7)




            unemployment_update[row['geoid']] = {
                'data': {
                    'Unemployment Rate': {
                        'Unemployment Rate': current_unemployment,
                        '{} Unemployment Rate'.format(CENSUS_LATEST_YEAR): acs_unemployment,
                        'Unemployment Rate % Change'.format(CENSUS_LATEST_YEAR): unemployment_rate_change,
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


def update_tract_unemployment():
    '''
    Function updates all tract unemployment rates using unemployment adjustments calculated from BLS data.
    :return:
    '''

    for stategroupindex, stategroup in enumerate([STATES1, STATES2]):
        prod_env = ProductionEnvironment.CENSUS_DATA1
        if stategroupindex == 1:
            prod_env = ProductionEnvironment.CENSUS_DATA2

        for stateid in stategroup:
            collection_filter = {
                'geolevel': {'$eq': GeoLevels.TRACT.value},
                'stateid': stateid
            }

            geo_data = mongoclient.query_collection(database_name="CensusData1",
                                                    collection_name="CensusData",
                                                    collection_filter=collection_filter,
                                                    prod_env=prod_env)

            county_filter = {
                'geolevel': {'$eq': GeoLevels.COUNTY.value},
                'stateid': stateid
            }

            county_data = mongoclient.query_collection(database_name="CensusData1",
                                                    collection_name="CensusData",
                                                    collection_filter=county_filter,
                                                    prod_env=ProductionEnvironment.CENSUS_DATA1)

            unemployment_update = {}

            for i, row in geo_data.iterrows():
                countyfullcode = row.geoinfo['countyfullcode']
                county_data_dict = county_data[county_data['geoid'] == countyfullcode].iloc[0]

                if 'Unemployment Rate' not in county_data_dict['data'].keys() or 'Unemployment Rate % Change' not in county_data_dict['data']['Unemployment Rate'].keys():
                    print('FIX - Why is Unemployment Rate % Change missing')
                    county_unemployment_rate_change = 1
                else:
                    county_unemployment_rate_change = county_data_dict['data']['Unemployment Rate']['Unemployment Rate % Change']

                tract_unemployment = row['data']['Unemployment Rate']['Unemployment Rate']
                unemployment_adjustment = tract_unemployment * county_unemployment_rate_change
                tract_unemployment = round(tract_unemployment + unemployment_adjustment, 1)

                unemployment_update[row['geoid']] = {
                    'data': {
                        'Unemployment Rate': {
                            'Unemployment Rate': tract_unemployment
                        }
                    }
                }

            success = mongoclient.store_census_data(geo_level=GeoLevels.TRACT,
                                                    state_id=stateid,
                                                    filtered_dict=unemployment_update,
                                                    prod_env=prod_env,
                                                    county_batches=True)

            if success:
                collection_add_finished_run = {
                    'scopeout_year': SCOPEOUT_YEAR,
                    'state_id': stateid,
                    'geo_level': GeoLevels.TRACT.value,
                    'category': 'Census Unemployment',
                }
                mongoclient.add_finished_run(collection_add_finished_run)
            else:
                print('ERROR: Failed to store unemployment data')