from enum import Enum

class GeoLevels(Enum):
    USA = 'us'
    CBSA = 'cbsa'
    STATE = 'state'
    COUNTY = 'county'
    TRACT = 'tract'