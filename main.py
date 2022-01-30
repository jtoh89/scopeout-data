from scopeoutdata import createneighborhoodprofiles, createmarketprofiles, createcbsamarketprofiles, createmarketmaps
from unemployment import unemploymentdownload, unemploymentupdates
from enums import GeoLevels, DefaultGeoIds, ProductionEnvironment, GeoIdField, GeoNameField
from census import censusdata, censuslookups
from realestate import redfin, buildingpermits, initialize, zillow
from geographies import geographies, topmarkets, esrigeographies
from database import mongoclient

##################################################
##################################################
######  DONE
##################################################
##################################################

###################################################
###### GEOGRAPHIES

# # dump state, county, cbsa geographies
# geographies.dump_all_geographies()

# topmarkets.store_top_200_markets()

# # dump zipcodes to geographies
# geographies.dump_zipcode()

# esrigeographies.dump_geographies_by_cbsa()


# # dump zillow to cbsa mapping
# geographies.dump_zillow_cbsa_mapping()
###################################################


# mongoclient.delete_finished_run({
#         'geo_level': GeoLevels.CBSA.value,
#         'category': 'buildingpermits'
#     })
# print('Delete done')
#
# mongoclient.delete_finished_run({
#     'state_id':"27",
#         'geo_level': GeoLevels.TRACT.value,
#         'category': 'Poverty Rate'
#     })
# print('Delete done')




# censusdata.run_census_data_import(GeoLevels.USA, ProductionEnvironment.CENSUS_DATA1)
# censusdata.update_us_median_income_fred()
# censusdata.run_census_data_import(GeoLevels.STATE, ProductionEnvironment.CENSUS_DATA1)
# censusdata.run_census_data_import(GeoLevels.CBSA, ProductionEnvironment.CENSUS_DATA1)
# censusdata.run_census_data_import(GeoLevels.COUNTY, ProductionEnvironment.CENSUS_DATA1)
# censusdata.run_census_data_import(GeoLevels.TRACT, ProductionEnvironment.CENSUS_DATA1)
# censusdata.run_census_data_import(GeoLevels.TRACT, ProductionEnvironment.CENSUS_DATA2)


#### FORCE CENSUS RUNS

# force_run = {
#     'stateid':'28',
#     'category': 'Family Type'
# }
# censusdata.run_census_data_import(GeoLevels.TRACT, ProductionEnvironment.CENSUS_DATA1, force_run=force_run)

####


# mongoclient.create_county_to_cbsa_lookup()

# unemploymentdownload.download_cbsa_unemployment()
# unemploymentdownload.download_county_unemployment()
# unemploymentdownload.download_usa_unemployment()
# unemploymentupdates.update_regional_unemployment(GeoLevels.CBSA)
# unemploymentupdates.update_regional_unemployment(GeoLevels.COUNTY)


# unemploymentupdates.update_tract_unemployment()


# redfin.import_redfin_data(geo_level=GeoLevels.USA,
#                           default_geoid=DefaultGeoIds.USA.value,
#                           geoid_field=GeoIdField.USA.value,
#                           geoname_field=GeoNameField.USA.value)


# redfin.import_redfin_data(geo_level=GeoLevels.CBSA,
#                           default_geoid=DefaultGeoIds.CBSA.value,
#                           geoid_field=GeoIdField.CBSA.value,
#                           geoname_field=GeoNameField.CBSA.value)

# redfin.import_redfin_data(geo_level=GeoLevels.COUNTY,
#                               default_geoid=DefaultGeoIds.COUNTY.value,
#                               geoid_field=GeoIdField.COUNTY.value,
#                               geoname_field=GeoNameField.COUNTY.value)
#
# zillow.import_zillow_msa_rental_data(geo_level=GeoLevels.USA,
#                                      default_geoid=DefaultGeoIds.USA.value,
#                                      geoid_field=GeoIdField.USA.value,
#                                      geoname_field=GeoNameField.USA.value)

# zillow.import_zillow_msa_rental_data(geo_level=GeoLevels.CBSA,
#                                      default_geoid=DefaultGeoIds.CBSA.value,
#                                      geoid_field=GeoIdField.CBSA.value,
#                                      geoname_field=GeoNameField.CBSA.value)


# buildingpermits.run_cbsa_building_permit(geo_level=GeoLevels.CBSA,
#                                          geoid_field=GeoIdField.CBSA.value,
#                                          geoname_field=GeoNameField.CBSA.value)



# unemploymentdownload.market_profile_add_unemployment(GeoLevels.USA, GeoIdField.USA.value)
# unemploymentdownload.market_profile_add_unemployment(GeoLevels.CBSA, GeoIdField.CBSA.value)
# unemploymentdownload.market_profile_add_unemployment(GeoLevels.COUNTY, GeoIdField.COUNTY.value)


##################################################
##################################################
###### PRODUCTION DATA
##################################################
##################################################


# createmarketprofiles.create_county_market_profiles()

# createcbsamarketprofiles.generate_cbsa_market_profiles(prod_env=ProductionEnvironment.MARKET_TRENDS,
#                                                        geoid_field=GeoIdField.CBSA.value)


# createneighborhoodprofiles.create_neighborhood_profiles()

createmarketmaps.generate_market_maps()