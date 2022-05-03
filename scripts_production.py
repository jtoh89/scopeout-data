from scopeoutdata import createneighborhoodprofiles, createmarketprofiles, createcbsamarketprofiles
from scopeoutdata import createshortneighborhoodprofiles, createshortzipcodeprofiles
from scopeoutdata import createzipcodemaps, createtractmaps
from unemployment import unemploymentdownload, unemploymentupdates
from enums import GeoLevels, DefaultGeoIds, ProductionEnvironment, GeoIdField, GeoNameField, Collections_Historical_Profiles, Collections_Profiles



##################################################################
###### PRODUCTION MARKET DATA
##################################################################

# #  Market Trends
unemploymentdownload.market_profile_add_unemployment(GeoLevels.USA, GeoIdField.USA.value)
# unemploymentdownload.market_profile_add_unemployment(GeoLevels.CBSA, GeoIdField.CBSA.value)
# unemploymentdownload.market_profile_add_unemployment(GeoLevels.COUNTY, GeoIdField.COUNTY.value)


# createmarketprofiles.create_county_market_profiles(collection_name=Collections_Profiles.COUNTY.value)

# createcbsamarketprofiles.generate_cbsa_market_profiles(prod_env=ProductionEnvironment.MARKET_PROFILES,
#                                                        geoid_field=GeoIdField.CBSA.value)


# createneighborhoodprofiles.create_neighborhood_profiles()
# createshortneighborhoodprofiles.create_short_neighborhood_profiles()
# createshortzipcodeprofiles.create_short_zipcode_profiles()


############# CREATE PRODUCTION MAP DATA ############
# createtractmaps.generate_tract_maps()
# createzipcodemaps.generate_zipcode_maps()
