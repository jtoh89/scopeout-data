from database import mongoclient
from enums import GeoLevels
from resetscripts import reset

##################################################################
### Unemployment Update
##################################################################
reset.reset_unemployment()

# reset.reset_census_cbsa_by_state("01")
# reset.reset_census_county_by_state("01")
# reset.reset_census_tract_by_state("01")




