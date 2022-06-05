from database import mongoclient
from enums import GeoLevels

def reset_unemployment():
    mongoclient.delete_finished_run({
        'category': 'Unemployment Rate',
    })

    mongoclient.delete_finished_run({
        'category': 'Unemployment Update',
    })

    print('Unemployment reset done')

def reset_census_cbsa_by_state(state_id):
    mongoclient.delete_finished_run({
        'geo_level': GeoLevels.CBSA.value,
        'tablename': 'censusdata1',
        'state_id': state_id
    })
    print('Cbsa delete done for stateid: {}'.format(state_id))


def reset_census_county_by_state(state_id):
    mongoclient.delete_finished_run({
        'geo_level': GeoLevels.COUNTY.value,
        'tablename': 'censusdata1',
        'state_id': state_id
    })
    print('County delete done for stateid: {}'.format(state_id))

def reset_census_tract_by_state(state_id):
    mongoclient.delete_finished_run({
            'geo_level': GeoLevels.TRACT.value,
            'tablename': 'censusdata1',
            'state_id': state_id
        })

    mongoclient.delete_finished_run({
            'geo_level': GeoLevels.TRACT.value,
            'tablename': 'censusdata2',
            'state_id': state_id
        })

    print('Tract delete done for stateid: {}'.format(state_id))



def reset_shortneighborhoodprofiles():
    mongoclient.delete_finished_run({
        'geo_level': GeoLevels.TRACT.value,
        'category': 'shortneighborhoodprofiles'
    })
    print('Tract delete done')
