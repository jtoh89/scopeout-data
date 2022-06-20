from geographies import geography, scopeoutmarkets, esrigeographies, tractgeojson



##################################################################
###### GEOGRAPHIES
##################################################################

# dump state, county, cbsa geographies
# geography.dump_state_geography()
# geography.dump_county_geography()
# geography.dump_cbsa_geography()
# geography.DEPRECATED_dump_all_geographies()

# dump scopeout markets
# scopeoutmarkets.store_scopeout_markets()-
# scopeoutmarkets.store_all_markets(300)

# dump tract info by county
# esrigeographies.DEPECRATED_dump_tract_by_county()
# geography.store_tract_lookups()

# dump tract geojson
# tractgeojson.store_tract_geojson_for_cbsacode()

# dump county by cbsa lookup
# geography.dump_county_by_cbsa_lookup()

# dump zillow to cbsa lookup
# geography.dump_zillow_cbsa_mapping()

# dump zipcodes by scopeout markets
geography.dump_zipcode_geojson_by_scopeout_markets()

##################################################################
##################################################################
