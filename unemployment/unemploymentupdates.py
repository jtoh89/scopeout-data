from database import mongoclient
from enums import GeoLevels
from enums import ProductionEnvironment
from database import mongoclient
from census.censusdata import STATES
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
                print('COULD NOT SET UNEMPLOYMENT. Geocode: ', row['geoid'])
                continue

            current_unemployment = float(row['data']['Unemployment Historic']['Unemployment Historic'][-1:][0])
            old_unemployment = float(row['data']['Unemployment Rate']['Unemployment Rate'])

            unemployment_rate_change = calculate_percent_change(starting_data=old_unemployment,
                                                                ending_data=current_unemployment,
                                                                move_decimal=False,
                                                                decimal_places=7)

            unemployment_update[row['geoid']] = {
                'data': {
                    'Unemployment Rate': {
                        'Unemployment Rate': current_unemployment,
                        '{} Unemployment Rate'.format(CENSUS_LATEST_YEAR): old_unemployment,
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
        else:
            print('ERROR: Failed to store unemployment data')


def update_tract_unemployment():
    '''
    Function updates all tract unemployment rates using unemployment adjustments calculated from BLS data.
    :return:
    '''
    for stateid in STATES:
        collection_filter = {
            'geolevel': {'$eq': GeoLevels.TRACT.value},
            'stateid': stateid
        }

        geo_data = mongoclient.query_collection(database_name="CensusData1",
                                                collection_name="CensusData",
                                                collection_filter=collection_filter,
                                                prod_env=ProductionEnvironment.CENSUS_DATA1)

        county_filter = {
            'geolevel': {'$eq': GeoLevels.COUNTY.value},
            'stateid': stateid
        }

        county_data = mongoclient.query_collection(database_name="CensusData1",
                                                collection_name="CensusData",
                                                collection_filter=county_filter,
                                                prod_env=ProductionEnvironment.CENSUS_DATA1)



        for i, row in geo_data.iterrows():
            row['data']['Unemployment Rate']['Unemployment Rate'] = row['data']['Unemployment Historic']['Unemployment Historic'][-1:][0]

            unemployment_update[row['geoid']] = {
                'data': {
                    'Unemployment Rate': {
                        'Unemployment Rate': row['data']['Unemployment Historic']['Unemployment Historic'][-1:][0]
                    }
                }
            }


        success = mongoclient.store_census_data(geo_level=GeoLevels.TRACT,
                                                state_id=stateid,
                                                filtered_dict=unemployment_update,
                                                prod_env=ProductionEnvironment.CENSUS_DATA1)

        if success:
            print('Successfully stored unemployment data')
        else:
            print('ERROR: Failed to store unemployment data')