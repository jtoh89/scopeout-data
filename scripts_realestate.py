from enums import GeoLevels, DefaultGeoIds, ProductionEnvironment, GeoIdField, GeoNameField, Collections_Historical_Profiles, Collections_Profiles

from realestate import redfin, buildingpermits, initialize, zillow



##################################################################
###### REAL ESTATE
##################################################################


redfin.import_redfin_historical_data(geo_level=GeoLevels.USA,
                                     default_geoid=DefaultGeoIds.USA.value,
                                     geoid_field=GeoIdField.USA.value,
                                     geoname_field=GeoNameField.USA.value,
                                     collection_name=Collections_Historical_Profiles.USA.value)


redfin.import_redfin_historical_data(geo_level=GeoLevels.CBSA,
                                     default_geoid=DefaultGeoIds.CBSA.value,
                                     geoid_field=GeoIdField.CBSA.value,
                                     geoname_field=GeoNameField.CBSA.value,
                                     collection_name=Collections_Historical_Profiles.CBSA.value)


redfin.import_redfin_historical_data(geo_level=GeoLevels.COUNTY,
                                     default_geoid=DefaultGeoIds.COUNTY.value,
                                     geoid_field=GeoIdField.COUNTY.value,
                                     geoname_field=GeoNameField.COUNTY.value,
                                     collection_name=Collections_Historical_Profiles.COUNTY.value)


#
redfin.import_redfin_historical_data(geo_level=GeoLevels.ZIPCODE,
                                     default_geoid=DefaultGeoIds.ZIPCODE.value,
                                     geoid_field=GeoIdField.ZIPCODE.value,
                                     geoname_field=GeoNameField.ZIPCODE.value,
                                     collection_name=Collections_Historical_Profiles.ZIPCODE.value)


zillow.import_zillow_msa_rental_data(geo_level=GeoLevels.USA,
                                     default_geoid=DefaultGeoIds.USA.value,
                                     geoid_field=GeoIdField.USA.value,
                                     geoname_field=GeoNameField.USA.value,
                                     collection_name=Collections_Historical_Profiles.USA.value)


zillow.import_zillow_msa_rental_data(geo_level=GeoLevels.CBSA,
                                     default_geoid=DefaultGeoIds.CBSA.value,
                                     geoid_field=GeoIdField.CBSA.value,
                                     geoname_field=GeoNameField.CBSA.value,
                                     collection_name=Collections_Historical_Profiles.CBSA.value)


zillow.import_zillow_zip_rental_data(collection_name=Collections_Historical_Profiles.ZIPCODE.value)

buildingpermits.run_cbsa_building_permit(geo_level=GeoLevels.CBSA,
                                         geoid_field=GeoIdField.CBSA.value,
                                         geoname_field=GeoNameField.CBSA.value)


##################################################################
##################################################################
