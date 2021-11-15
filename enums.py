from enum import Enum

class GeoLevels(Enum):
    USA = 'us'
    CBSA = 'cbsa'
    STATE = 'state'
    COUNTY = 'county'
    TRACT = 'tract'

class DefaultGeoIds(Enum):
    USA = '99999'
    CBSA = '00000'

class ProductionEnvironment(Enum):
    PRODUCTION = 'prod'
    GEO_ONLY = 'geoonly'
    QA = 'qa'
    CENSUS_DATA1 = 'censusdata1'
    CENSUS_DATA2 = 'censusdata2'

class CensusDataByEnvironment(Enum):
    CENSUS_DATA1 = 'censusdata1'
    CENSUS_DATA2 = 'censusdata2'