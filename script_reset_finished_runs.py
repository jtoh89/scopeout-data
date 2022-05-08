from database import mongoclient
from enums import GeoLevels


##################################################################
### Unemployment Update
##################################################################

## Cbsa
# mongoclient.delete_finished_run({
#     'geo_level': GeoLevels.CBSA.value,
#     'category': 'Unemployment Update'
# })
# print('Cbsa delete done')

## County
# mongoclient.delete_finished_run({
#     'geo_level': GeoLevels.COUNTY.value,
#     'category': 'Unemployment Update'
# })
# print('County delete done')

## Tract
# mongoclient.delete_finished_run({
#         'geo_level': GeoLevels.TRACT.value,
#         'category': 'Unemployment Update'
#     })
# print('Tract delete done')



##################################################################
### Short/Full Neighborhood Update
##################################################################

mongoclient.delete_finished_run({
    'geo_level': GeoLevels.TRACT.value,
    'category': 'shortneighborhoodprofiles'
})
print('Tract delete done')




