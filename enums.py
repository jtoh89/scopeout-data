from enum import Enum

class GeoLevels(Enum):
    USA = 'us'
    CBSA = 'cbsa'
    STATE = 'state'
    COUNTY = 'county'
    TRACT = 'tract'
    ZIPCODE = 'zipcode'

class GeoIdField(Enum):
    USA = 'usacode'
    CBSA = 'cbsacode'
    COUNTY = 'countyfullcode'
    ZIPCODE = 'zipcode'

class GeoNameField(Enum):
    USA = 'name'
    CBSA = 'cbsaname'
    COUNTY = 'countyname'
    ZIPCODE = 'zipcodename'

class DefaultGeoIds(Enum):
    USA = '99999'
    CBSA = '00000'
    COUNTY = None
    ZIPCODE = '99999'

class ProductionEnvironment(Enum):
    PRODUCTION = 'prod'
    PRODUCTION2 = 'prod2'
    GEO_ONLY = 'geoonly'
    QA = 'qa'
    CENSUS_DATA1 = 'CensusData1'
    CENSUS_DATA2 = 'CensusData2'
    MARKET_TRENDS = 'markettrends'

class CensusDataByEnvironment(Enum):
    CENSUS_DATA1 = 'CensusData1'
    CENSUS_DATA2 = 'CensusData2'