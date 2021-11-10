from census import censusdata, censusgroupslookups
from scopeoutdata import processtracts
from enums import GeoLevels


# geo_level = GeoLevels.TRACT

# censusdata.run_census_data_import(GeoLevels.USA)
# censusdata.run_census_data_import(GeoLevels.STATE)
# censusdata.run_census_data_import(GeoLevels.CBSA)
# censusdata.run_census_data_import(GeoLevels.COUNTY)
# censusdata.run_census_data_import(GeoLevels.TRACT)

processtracts.process_tracts()


