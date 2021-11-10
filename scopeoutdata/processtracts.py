from database import mongoclient
from models import neighborhoodprofile
from database import mongoclient
import json
from enums import GeoLevels

CENSUS_LATEST_YEAR = 2019
CENSUS_YEARS = [2012, 2013, 2014, 2015, 2016, 2017, 2018, CENSUS_LATEST_YEAR]

def process_tracts():
    state_id = "01"

    neighborhood_profile = neighborhoodprofile.NeighborhoodProfile()

    # dict_test = json.loads(json.dumps(test, default=lambda o: o.__dict__))
    # dict_test = vars(test.convert_to_dict())


    counties_filter = {
        'stateinfo.fipsstatecode': state_id,
    }


    counties = mongoclient.query_collection(database_name='ScopeOutGeographies',
                                            collection_name="County",
                                            collection_filter=counties_filter,
                                            prod_env="geoonly")

    counties_data = get_geo_data(state_id=state_id)

    counties = counties[['countyfullcode','countyname']]





    census_tract_data_filter = {
        'stateid': {'$eq': state_id},
        'geolevel': {'$eq': 'tract'},
    }

    census_tract_data = mongoclient.query_collection(database_name="scopeout",
                                                     collection_name="CensusData",
                                                     collection_filter=census_tract_data_filter,
                                                     prod_env="prod")



    for i, row in census_tract_data.iterrows():
        tractid = row.geoinfo['tractcode']
        data = row.data
        test_neighborhood_profile_dict = calculate_history(row.data, neighborhood_profile)

        print(neighborhood_profile)

    # tracts_collection_filter = {
    #     'fipsstatecode': {'$eq': state_id},
    # }
    #
    # tracts = mongoclient.query_collection(collection_name="EsriTracts",
    #                                       collection_filter=tracts_collection_filter,
    #                                       prod_env="prod")
    #

def get_geo_data(state_id):
    state_data_filter = {
        'geolevel': 'state',
        'stateid': state_id,
    }

    state_data = mongoclient.query_collection(database_name='scopeout',
                                                  collection_name="CensusData",
                                                  collection_filter=state_data_filter,
                                                  prod_env="prod")

    counties_data_filter = {
        'geolevel': 'county',
        'stateid': state_id,
    }

    counties_data = mongoclient.query_collection(database_name='scopeout',
                                                 collection_name="CensusData",
                                                 collection_filter=counties_data_filter,
                                                 prod_env="prod")



    return counties_data


def calculate_top_5(data_dict, neighborhood_profile):
    five_year = CENSUS_LATEST_YEAR - 5

    industry_fiveyearandcurrent = {}

    for year in [str(five_year), 'LatestYear']:
        for k,v in data_dict[year]['Employment Industry Growth'].items():
            if year == str(five_year):
                industry_fiveyearandcurrent[k] = v
            else:
                startingvalue = industry_fiveyearandcurrent[k]
                industry_fiveyearandcurrent[k] = calculate_percent_change(startingvalue, v)

    top_industry_growth = sorted(industry_fiveyearandcurrent.items(), key=lambda x: (x[1],x[0]), reverse=True)[0:5]

def calculate_history(data_dict, neighborhood_profile):
    five_year = CENSUS_LATEST_YEAR - 5

    industry_fiveyearandcurrent = {}

    for year in [str(five_year), 'LatestYear']:
        for k,v in data_dict[year]['Employment Industry Growth'].items():
            if year == str(five_year):
                industry_fiveyearandcurrent[k] = v
            else:
                startingvalue = industry_fiveyearandcurrent[k]
                industry_fiveyearandcurrent[k] = calculate_percent_change(startingvalue, v)

    top_industry_growth = sorted(industry_fiveyearandcurrent.items(), key=lambda x: (x[1],x[0]), reverse=True)[0:5]

    # for i, item in enumerate(top_industry_growth):
    #     print(item)
    #     top_industry_growth[i][1] = str(item) + '%'

    population = []
    population_fiveyearandcurrent = []
    housingunit = []
    housingunit_fiveyearandcurrent = []
    homeowner_fiveyearandcurrent = []
    renter_fiveyearandcurrent = []

    for year in CENSUS_YEARS:
        if year == CENSUS_LATEST_YEAR:
            data_year = data_dict['LatestYear']
        else:
            data_year = data_dict[str(year)]

        population.append(data_year['Population Growth']['Total Population'])
        housingunit.append(data_year['Housing Unit Growth']['Total Housing Units'])

        if year == five_year or year == CENSUS_LATEST_YEAR:
            population_fiveyearandcurrent.append(data_year['Population Growth']['Total Population'])
            housingunit_fiveyearandcurrent.append(data_year['Housing Unit Growth']['Total Housing Units'])
            homeowner_fiveyearandcurrent.append(data_year['Homeowner Growth']['Owner'])
            renter_fiveyearandcurrent.append(data_year['Renter Growth']['Renter'])

    growth_years = CENSUS_YEARS[1:]
    population_growth = calculate_historic_growth(population)
    population_fiveyeargrowth = calculate_percent_change(population_fiveyearandcurrent[0], population_fiveyearandcurrent[1])
    housingunit_growth = calculate_historic_growth(housingunit)
    housingunit_fiveyeargrowth = calculate_percent_change(housingunit_fiveyearandcurrent[0], housingunit_fiveyearandcurrent[1])
    homeowner_fiveyeargrowth = calculate_percent_change(homeowner_fiveyearandcurrent[0], homeowner_fiveyearandcurrent[1])
    renter_fiveyeargrowth = calculate_percent_change(renter_fiveyearandcurrent[0], renter_fiveyearandcurrent[1])


    neighborhood_profile.demographics.populationtrends.data1 = population
    neighborhood_profile.demographics.populationtrends.labels1 = CENSUS_YEARS
    neighborhood_profile.demographics.populationtrends.data2 = population_growth
    neighborhood_profile.demographics.populationtrends.labels2 = growth_years
    neighborhood_profile.demographics.fiveyeargrowth.data.append(population_fiveyeargrowth)

    neighborhood_profile.housing.housingunitgrowth.data1 = housingunit
    neighborhood_profile.housing.housingunitgrowth.labels1 = CENSUS_YEARS
    neighborhood_profile.housing.housingunitgrowth.data2 = housingunit_growth
    neighborhood_profile.housing.housingunitgrowth.labels2 = growth_years

    neighborhood_profile.housing.housingquickfacts.value1 = homeowner_fiveyeargrowth
    neighborhood_profile.housing.housingquickfacts.value2 = renter_fiveyeargrowth
    neighborhood_profile.housing.housingquickfacts.value3 = housingunit_fiveyeargrowth


def calculate_historic_growth(data_array):
    return_array = []
    for i, val in enumerate(data_array):
        if i < 1:
            continue

        change = calculate_percent_change(data_array[i-1], val)

        return_array.append(change)

    return return_array



def calculate_percent_change(starting_data, ending_data):
    if starting_data == 0 or ending_data == 0:
        return 0

    percent_change = round((ending_data - starting_data) / starting_data * 100, 1)
    return percent_change