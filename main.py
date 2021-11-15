from scopeoutdata import processtracts
from unemployment import unemploymentdownload
from unemployment import unemploymentupdates
from enums import GeoLevels
from enums import ProductionEnvironment
from census import censusdata
from census import censuslookups

# censusdata.run_census_data_import(GeoLevels.USA, ProductionEnvironment.CENSUS_DATA1)
# censusdata.run_census_data_import(GeoLevels.STATE, ProductionEnvironment.CENSUS_DATA1)
# censusdata.run_census_data_import(GeoLevels.CBSA, ProductionEnvironment.CENSUS_DATA1)
# censusdata.run_census_data_import(GeoLevels.COUNTY, ProductionEnvironment.CENSUS_DATA1)
# censusdata.run_census_data_import(GeoLevels.TRACT, ProductionEnvironment.CENSUS_DATA1)

# censuslookups.create_county_to_cbsa_lookup()

# unemploymentdownload.download_cbsa_unemployment()
# unemploymentdownload.download_county_unemployment()
# unemploymentdownload.download_usa_unemployment()

# unemploymentupdates.update_regional_unemployment(GeoLevels.CBSA)
# unemploymentupdates.update_regional_unemployment(GeoLevels.COUNTY)


#NOT DONE
# unemploymentupdates.update_tract_unemployment()

processtracts.process_tracts()