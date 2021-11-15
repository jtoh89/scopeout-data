import sys

from database import mongoclient
from models import neighborhoodprofile
from database import mongoclient
import json
from enums import GeoLevels
from enums import ProductionEnvironment
from utils.utils import calculate_percent_change

CENSUS_LATEST_YEAR = 2019
CENSUS_YEARS = [2012, 2013, 2014, 2015, 2016, 2017, 2018, CENSUS_LATEST_YEAR]

COLORS = ["green", "red", "yellow", "blue", "brown", "teal", "purple", "#65AFFF", "#4F6D7A", "#828489", "#D81E5B", "#FDF0D5", "#D4F2DB"]

def process_tracts():
    state_id = "01"

    neighborhood_profile = neighborhoodprofile.NeighborhoodProfile()

    # dict_test = json.loads(json.dumps(test, default=lambda o: o.__dict__))
    # dict_test = vars(test.convert_to_dict())


    # geo_data_df = get_geo_data(state_id=state_id)




    census_tract_data_filter = {
        'stateid': {'$eq': state_id},
        'geolevel': {'$eq': 'tract'},
    }

    census_tract_data = mongoclient.query_collection(database_name="censusdata1",
                                                     collection_name="CensusData",
                                                     collection_filter=census_tract_data_filter,
                                                     prod_env=ProductionEnvironment.CENSUS_DATA1)


    if len(census_tract_data) < 1:
        print('Did not find any county_data. Check which database state uses for censusdata')
        sys.exit()

    counties_to_get = []
    for i, record in census_tract_data.iterrows():
        countyfullcode = record.geoinfo['countyfullcode']

        if countyfullcode not in counties_to_get:
            counties_to_get.append(countyfullcode)


    census_county_data_filter = {
        'stateid': {'$eq': state_id},
        'geolevel': {'$eq': 'county'},
        'geoid': {'$in': counties_to_get},
    }

    county_data = mongoclient.query_collection(database_name="censusdata1",
                                               collection_name="CensusData",
                                               collection_filter=census_county_data_filter,
                                               prod_env=ProductionEnvironment.CENSUS_DATA1)


    census_cbsa_data_filter = {
        'stateid': {'$eq': state_id},
        'geolevel': {'$eq': 'cbsa'},
    }

    cbsa_data = mongoclient.query_collection(database_name="censusdata1",
                                               collection_name="CensusData",
                                               collection_filter=census_cbsa_data_filter,
                                               prod_env=ProductionEnvironment.CENSUS_DATA1)

    census_usa_data_filter = {
        'stateid': {'$eq': state_id},
        'geolevel': {'$eq': 'cbsa'},
    }

    usa_data = mongoclient.query_collection(database_name="censusdata1",
                                             collection_name="CensusData",
                                             collection_filter=census_usa_data_filter,
                                             prod_env=ProductionEnvironment.CENSUS_DATA1)

    for i, row in census_tract_data.iterrows():
        tractid = row.geoinfo['tractcode']
        data = row.data
        neighborhood_profile = calculate_history(row.data, neighborhood_profile)
        neighborhood_profile = calculate_chart_data(
            data_dict=row.data,
            neighborhood_profile=neighborhood_profile,
            county_data=county_data.data[0],
            # cbsa_data=cbsa_data,
            # usa_data=usa_data
        )


        print(neighborhood_profile)

    # tracts_collection_filter = {
    #     'fipsstatecode': {'$eq': state_id},
    # }
    #
    # tracts = mongoclient.query_collection(collection_name="EsriTracts",
    #                                       collection_filter=tracts_collection_filter,
    #                                       prod_env="prod")
    #

def calculate_chart_data(data_dict, neighborhood_profile, county_data
                         # cbsa_data,
                         # usa_data
                         ):

    dominant_housing_type = sorted(data_dict['Property Types']['All'].items(), key=lambda x: (x[1],x[0]), reverse=True)[0][0]
    neighborhood_profile.housing.housingquickfacts.value4 = dominant_housing_type


    neighborhood_profile.demographics.demographicquickfacts.value1 = str(data_dict['Poverty Rate']['Poverty Rate']) + '%'
    neighborhood_profile.demographics.demographicquickfacts.value2 = str(data_dict["% of Children"]["% of Children"]) + '%'
    neighborhood_profile.demographics.demographicquickfacts.value3 = str(data_dict['College Population']['College Population']) + '%'
    neighborhood_profile.demographics.demographicquickfacts.value4 = str(data_dict['Veteran']['Veteran']) + '%'

    neighborhood_profile.demographics.highesteducation.labels = list(data_dict['Highest Education'].keys())
    neighborhood_profile.demographics.highesteducation.data = list(data_dict['Highest Education'].values())
    neighborhood_profile.demographics.highesteducation.colors = COLORS[:len(data_dict['Highest Education'].values())]

    neighborhood_profile.demographics.race.labels = list(data_dict['Race/Ethnicity'].keys())
    neighborhood_profile.demographics.race.data = list(data_dict['Race/Ethnicity'].values())
    neighborhood_profile.demographics.race.colors = COLORS[:len(data_dict['Race/Ethnicity'].values())]

    neighborhood_profile.demographics.agegroups.labels = list(data_dict['Age Groups'].keys())
    neighborhood_profile.demographics.agegroups.data = list(data_dict['Age Groups'].values())
    neighborhood_profile.demographics.agegroups.colors = COLORS[:len(data_dict['Age Groups'].values())]

    neighborhood_profile.demographics.familytype.labels = list(data_dict['Family Type'].keys())
    neighborhood_profile.demographics.familytype.data = list(data_dict['Family Type'].values())
    neighborhood_profile.demographics.familytype.colors = COLORS[:len(data_dict['Family Type'].values())]

    neighborhood_profile.economy.householdincomerange.labels = list(data_dict['Household Income Range']['All'].keys())
    neighborhood_profile.economy.householdincomerange.data1 = list(data_dict['Household Income Range']['All'].values())
    neighborhood_profile.economy.householdincomerange.data2 = list(data_dict['Household Income Range']['Owners'].values())
    neighborhood_profile.economy.householdincomerange.data3 = list(data_dict['Household Income Range']['Renters'].values())


    neighborhood_profile.economy.medianhouseholdincome.labels.append('Neighborhood')
    neighborhood_profile.economy.medianhouseholdincome.data1.append(data_dict['Median Household Income']['All'][-1])
    neighborhood_profile.economy.medianhouseholdincome.data2.append(data_dict['Median Household Income']['Owners'][-1])
    neighborhood_profile.economy.medianhouseholdincome.data3.append(data_dict['Median Household Income']['Renters'][-1])
    neighborhood_profile.economy.medianhouseholdincome.colors = COLORS[:4]


    neighborhood_profile.economy.unemploymentrate.labels.append('Neighborhood')
    neighborhood_unemployment = data_dict['Unemployment Rate']['Unemployment Rate']
    # check unemployment adjustments

    neighborhood_adjustment_rate = neighborhood_unemployment * county_data['Unemployment Rate']['Unemployment Rate % Change']
    neighborhood_unemployment = neighborhood_unemployment + neighborhood_adjustment_rate
    countyunemployment = county_data['Unemployment Rate']['Unemployment Rate']
    keep adding comparison charts
    neighborhood_profile.economy.unemploymentrate.data = [neighborhood_unemployment, countyunemployment]
    # neighborhood_profile.economy.unemploymentrate.data.append(data_dict['Unemployment Rate']['Unemployment Rate'])
    neighborhood_profile.economy.unemploymentrate.colors = COLORS[:4]


    neighborhood_profile.economy.topemploymentindustries.data = calculate_top_5(data_dict['Employment Industries'])

    neighborhood_profile.economy.employmentindustries.labels = list(data_dict['Employment Industries'].keys())
    neighborhood_profile.economy.employmentindustries.data = list(data_dict['Employment Industries'].values())
    neighborhood_profile.economy.employmentindustries.colors = COLORS[:len(data_dict['Employment Industries'].values())]

    neighborhood_profile.economy.vehiclesowned.labels = list(data_dict['Vehicles Owned']['All'].keys())
    neighborhood_profile.economy.vehiclesowned.data1 = list(data_dict['Vehicles Owned']['All'].keys())
    neighborhood_profile.economy.vehiclesowned.data2 = list(data_dict['Vehicles Owned']['Owners'].keys())
    neighborhood_profile.economy.vehiclesowned.data3 = list(data_dict['Vehicles Owned']['Renters'].keys())
    neighborhood_profile.economy.vehiclesowned.colors = COLORS[:len(data_dict['Vehicles Owned']['All'].values())]

    neighborhood_profile.economy.commutetowork.labels = list(data_dict['Commute to Work'].keys())
    neighborhood_profile.economy.commutetowork.data = list(data_dict['Commute to Work'].values())

    neighborhood_profile.economy.meansoftransportation.labels = list(data_dict['Means of Transportation'].keys())
    neighborhood_profile.economy.meansoftransportation.data = list(data_dict['Means of Transportation'].values())
    neighborhood_profile.economy.meansoftransportation.colors = COLORS[:len(data_dict['Means of Transportation'].values())]

    neighborhood_profile.housing.occupancyrate.labels = list(data_dict['Occupancy rate'].keys())
    neighborhood_profile.housing.occupancyrate.data = list(data_dict['Occupancy rate'].values())
    neighborhood_profile.housing.occupancyrate.colors = COLORS[:len(data_dict['Occupancy rate'].values())]

    neighborhood_profile.housing.utilitiesincluded.labels = list(data_dict['Utilities in Rent'].keys())
    neighborhood_profile.housing.utilitiesincluded.data = list(data_dict['Utilities in Rent'].values())
    neighborhood_profile.housing.utilitiesincluded.colors = COLORS[:len(data_dict['Utilities in Rent'].values())]

    neighborhood_profile.housing.propertytypes.labels = list(data_dict['Property Types']['All'].keys())
    neighborhood_profile.housing.propertytypes.data1 = list(data_dict['Property Types']['All'].values())
    neighborhood_profile.housing.propertytypes.data2 = list(data_dict['Property Types']['Owners'].values())
    neighborhood_profile.housing.propertytypes.data3 = list(data_dict['Property Types']['Renters'].values())
    neighborhood_profile.housing.propertytypes.colors = COLORS[:len(data_dict['Property Types']['All'].values())]

    neighborhood_profile.housing.yearbuilt.labels = list(data_dict['Year Built'].keys())
    neighborhood_profile.housing.yearbuilt.data = list(data_dict['Year Built'].values())

    neighborhood_profile.housing.numberofbedrooms.labels = list(data_dict['Number of Bedrooms']['All'].keys())
    neighborhood_profile.housing.numberofbedrooms.data1 = list(data_dict['Number of Bedrooms']['All'].values())
    neighborhood_profile.housing.numberofbedrooms.data2 = list(data_dict['Number of Bedrooms']['Owners'].values())
    neighborhood_profile.housing.numberofbedrooms.data3 = list(data_dict['Number of Bedrooms']['Renters'].values())
    neighborhood_profile.housing.numberofbedrooms.colors = COLORS[:len(data_dict['Number of Bedrooms']['All'].values())]

    neighborhood_profile.housing.yearmovedin.labels = list(data_dict['Year Moved In']['All'].keys())
    neighborhood_profile.housing.yearmovedin.data1 = list(data_dict['Year Moved In']['All'].values())
    neighborhood_profile.housing.yearmovedin.data2 = list(data_dict['Year Moved In']['Owners'].values())
    neighborhood_profile.housing.yearmovedin.data3 = list(data_dict['Year Moved In']['Renters'].values())
    neighborhood_profile.housing.yearmovedin.colors = COLORS[:len(data_dict['Year Moved In']['All'].values())]

    neighborhood_profile.housing.incomehousingcost.labels = list(data_dict['% Income on Housing Costs']['All'].keys())
    neighborhood_profile.housing.incomehousingcost.data1 = list(data_dict['% Income on Housing Costs']['All'].values())
    neighborhood_profile.housing.incomehousingcost.data2 = list(data_dict['% Income on Housing Costs']['Owners'].values())
    neighborhood_profile.housing.incomehousingcost.data3 = list(data_dict['% Income on Housing Costs']['Renters'].values())
    neighborhood_profile.housing.incomehousingcost.colors = COLORS[:len(data_dict['% Income on Housing Costs']['All'].values())]


    return neighborhood_profile



def calculate_history(data_dict, neighborhood_profile):
    # population = []
    # housingunit = []

    population = data_dict['Population Growth']['Total Population']
    housingunit = data_dict['Housing Unit Growth']['Total Housing Units']
    # housingunit.append(data_dict['Housing Unit Growth']['Total Housing Units'])


    growth_years = CENSUS_YEARS[1:]
    population_growth = calculate_historic_growth(population)

    population_oneyeargrowth = calculate_percent_change(data_dict['Population Growth']['Total Population'][-2], data_dict['Population Growth']['Total Population'][-1])

    housingunit_growth = calculate_historic_growth(housingunit)
    housingunit_oneyeargrowth = calculate_percent_change(data_dict['Housing Unit Growth']['Total Housing Units'][-2], data_dict['Housing Unit Growth']['Total Housing Units'][-1])

    homeowner_oneyeargrowth = calculate_percent_change(data_dict['Homeowner Growth']['Owner'][-2], data_dict['Homeowner Growth']['Owner'][-1])
    renter_oneyeargrowth = calculate_percent_change(data_dict['Renter Growth']['Renter'][-2], data_dict['Renter Growth']['Renter'][-1])

    neighborhood_profile.demographics.populationtrends.data1 = population
    neighborhood_profile.demographics.populationtrends.labels1 = CENSUS_YEARS
    neighborhood_profile.demographics.populationtrends.data2 = population_growth
    neighborhood_profile.demographics.populationtrends.labels2 = growth_years

    neighborhood_profile.demographics.oneyeargrowth.data.append(population_oneyeargrowth)
    neighborhood_profile.demographics.oneyeargrowth.labels.append('Neighborhood')
    neighborhood_profile.demographics.oneyeargrowth.colors = COLORS[:4]


    top_industry_growth = calculate_top_industry_growth(data_dict['Employment Industry Growth'])
    neighborhood_profile.economy.topemploymentindustries.data = top_industry_growth

    neighborhood_profile.housing.housingunitgrowth.data1 = housingunit
    neighborhood_profile.housing.housingunitgrowth.labels1 = CENSUS_YEARS
    neighborhood_profile.housing.housingunitgrowth.data2 = housingunit_growth
    neighborhood_profile.housing.housingunitgrowth.labels2 = growth_years

    neighborhood_profile.housing.housingquickfacts.value1 = homeowner_oneyeargrowth
    neighborhood_profile.housing.housingquickfacts.value2 = renter_oneyeargrowth
    neighborhood_profile.housing.housingquickfacts.value3 = housingunit_oneyeargrowth
    neighborhood_profile.housing.housingquickfacts.value4 = housingunit_oneyeargrowth

    return neighborhood_profile

def calculate_top_industry_growth(industry_growth_dict):
    industry_1_year_growth = {}

    for k, v in industry_growth_dict.items():
        if k == 'years':
            continue

        prev_current = v[-2:]
        prev_year_val = prev_current[0]
        current_year_val = prev_current[1]

        if prev_year_val == 0 or current_year_val == 0:
            industry_1_year_growth[k] = 0
            continue

        growth = calculate_percent_change(prev_year_val, current_year_val)

        industry_1_year_growth[k] = growth

    sorted_industy_growth = sorted(industry_1_year_growth.items(), key=lambda x: (x[1],x[0]), reverse=True)[:5]

    industry_1_year_growth = {}
    for k, v in sorted_industy_growth:
        industry_1_year_growth[k] = str(v) + '%'

    return industry_1_year_growth




def calculate_historic_growth(data_array):
    return_array = []
    for i, val in enumerate(data_array):
        if i < 1:
            continue

        change = calculate_percent_change(data_array[i-1], val)

        return_array.append(change)

    return return_array


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


def calculate_top_5(data_dict):
    top_5 = sorted(data_dict.items(), key=lambda x: (x[1],x[0]), reverse=True)[0:5]

    top_5_data = []
    for i, item in enumerate(top_5):
        add_object = {
            'label': item[0],
            'value': str(item[1]) + '%'
        }
        top_5_data.append(add_object)

    return top_5_data

