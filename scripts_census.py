from scopeoutdata import createneighborhoodprofiles, createmarketprofiles, createcbsamarketprofiles
from scopeoutdata import createshortneighborhoodprofiles, createshortzipcodeprofiles
from scopeoutdata import createzipcodemaps, createtractmaps
from unemployment import unemploymentdownload, unemploymentupdates
from enums import GeoLevels, DefaultGeoIds, ProductionEnvironment, GeoIdField, GeoNameField, Collections_Historical_Profiles, Collections_Profiles
from census import censusdata, censuslookups
from realestate import redfin, buildingpermits, initialize, zillow, DEPRECATED_redfin_zipcodes
from geographies import geography, scopeoutmarkets, esrigeographies
from database import mongoclient


##################################################################
##################################################################

# mongoclient.delete_finished_run({
#         'geo_level': GeoLevels.CBSA.value,
#         'category': 'buildingpermits'
#     })
# print('Delete done')

# mongoclient.delete_finished_run({
#     'state_id':"27",
#         'geo_level': GeoLevels.TRACT.value,
#         'category': 'Poverty Rate'
#     })
# print('Delete done')



##################################################################
###### CENSUS DATA
##################################################################

#### FORCE CENSUS RUNS

# force_run = {
#     'stateid':'28',
#     'category': 'Family Type'
# }
# censusdata.run_census_data_import(GeoLevels.TRACT, ProductionEnvironment.CENSUS_DATA1, force_run=force_run)

# censusdata.run_census_data_import(GeoLevels.USA, ProductionEnvironment.CENSUS_DATA1)
# censusdata.update_us_median_income_fred()
# censusdata.run_census_data_import(GeoLevels.STATE, ProductionEnvironment.CENSUS_DATA1)
# censusdata.run_census_data_import(GeoLevels.CBSA, ProductionEnvironment.CENSUS_DATA1)
# censusdata.run_census_data_import(GeoLevels.COUNTY, ProductionEnvironment.CENSUS_DATA1)
censusdata.run_census_data_import(GeoLevels.TRACT, ProductionEnvironment.CENSUS_DATA1)
# censusdata.run_census_data_import(GeoLevels.TRACT, ProductionEnvironment.CENSUS_DATA2)



##################################################################
##################################################################


##################################################################
###### UNEMPLOYMENT
##################################################################

# unemploymentdownload.download_cbsa_unemployment()
# unemploymentdownload.download_county_unemployment()
# unemploymentdownload.download_usa_unemployment()
# unemploymentupdates.update_regional_unemployment(GeoLevels.CBSA)
# unemploymentupdates.update_regional_unemployment(GeoLevels.COUNTY)

unemploymentupdates.update_tract_unemployment()


##################################################################
##################################################################


