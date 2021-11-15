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

GROWTH_YEAR_LABELS = CENSUS_YEARS[1:]
TRACT_LABEL_NAME = 'Neighborhood'

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

    counties_to_cbsa = mongoclient.query_collection(database_name="CensusDataInfo",
                                               collection_name="CountyToCbsa",
                                               collection_filter={'stateid': {'$eq': state_id}},
                                               prod_env=ProductionEnvironment.QA)

    county_cbsa_lookup = counties_to_cbsa[['countyfullcode', 'cbsacode', 'cbsaname']]
    all_cbsa = list(counties_to_cbsa['cbsacode'].drop_duplicates())

    census_cbsa_data_filter = {
        'geolevel': {'$eq': 'cbsa'},
        'geoid': {'$in': all_cbsa}
    }

    cbsa = mongoclient.query_collection(database_name="censusdata1",
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

    for i, tract in census_tract_data.iterrows():
        countyfullcode = tract.geoinfo['countyfullcode']
        county = county_data[county_data['geoid'] == countyfullcode].iloc[0]

        cbsainfo = county_cbsa_lookup[county_cbsa_lookup['countyfullcode'] == countyfullcode]
        cbsacode = cbsainfo['cbsacode'].iloc[0]
        cbsa = cbsa[cbsa['geoid'] == cbsacode].iloc[0]

        if len(cbsainfo) > 1:
            print('!!!ERROR - Found multiple cbsa for one county!!!')
            sys.exit()


        neighborhood_profile = set_demographic_section(tract, neighborhood_profile, cbsa, county, usa_data)
        neighborhood_profile = set_economy_section(tract, neighborhood_profile, cbsa, county, usa_data)
        neighborhood_profile = set_housing_section(tract, neighborhood_profile, cbsa, county, usa_data)


        print(neighborhood_profile)


def set_demographic_section(tract, neighborhood_profile, cbsa, county, usa):


    neighborhood_profile.demographics.demographicquickfacts.value1 = str(tract.data['Poverty Rate']['Poverty Rate']) + '%'
    neighborhood_profile.demographics.demographicquickfacts.value2 = str(tract.data["% of Children"]["% of Children"]) + '%'
    neighborhood_profile.demographics.demographicquickfacts.value3 = str(tract.data['College Population']['College Population']) + '%'
    neighborhood_profile.demographics.demographicquickfacts.value4 = str(tract.data['Veteran']['Veteran']) + '%'

    neighborhood_profile.demographics.highesteducation.labels = list(tract.data['Highest Education'].keys())
    neighborhood_profile.demographics.highesteducation.data = list(tract.data['Highest Education'].values())
    neighborhood_profile.demographics.highesteducation.colors = COLORS[:len(tract.data['Highest Education'].values())]

    neighborhood_profile.demographics.race.labels = list(tract.data['Race/Ethnicity'].keys())
    neighborhood_profile.demographics.race.data = list(tract.data['Race/Ethnicity'].values())
    neighborhood_profile.demographics.race.colors = COLORS[:len(tract.data['Race/Ethnicity'].values())]

    neighborhood_profile.demographics.agegroups.labels = list(tract.data['Age Groups'].keys())
    neighborhood_profile.demographics.agegroups.data = list(tract.data['Age Groups'].values())
    neighborhood_profile.demographics.agegroups.colors = COLORS[:len(tract.data['Age Groups'].values())]

    neighborhood_profile.demographics.familytype.labels = list(tract.data['Family Type'].keys())
    neighborhood_profile.demographics.familytype.data = list(tract.data['Family Type'].values())
    neighborhood_profile.demographics.familytype.colors = COLORS[:len(tract.data['Family Type'].values())]

    population = tract.data['Population Growth']['Total Population']
    population_growth = calculate_historic_growth(population)

    tract_population_oneyeargrowth = calculate_percent_change(tract.data['Population Growth']['Total Population'][-2], tract.data['Population Growth']['Total Population'][-1])
    cbsa_population_oneyeargrowth = calculate_percent_change(cbsa.data['Population Growth']['Total Population'][-2], tract.data['Population Growth']['Total Population'][-1])
    county_population_oneyeargrowth = calculate_percent_change(county.data['Population Growth']['Total Population'][-2], county.data['Population Growth']['Total Population'][-1])

    neighborhood_profile.demographics.populationtrends.data1 = population
    neighborhood_profile.demographics.populationtrends.labels1 = CENSUS_YEARS
    neighborhood_profile.demographics.populationtrends.data2 = population_growth
    neighborhood_profile.demographics.populationtrends.labels2 = GROWTH_YEAR_LABELS

    neighborhood_profile.demographics.oneyeargrowth.data.append(tract_population_oneyeargrowth,
                                                                cbsa_population_oneyeargrowth,
                                                                county_population_oneyeargrowth)

    county_name = county.geoinfo['countyname']
    cbsa_name = cbsa.geoinfo['cbsatitle']
    neighborhood_profile.demographics.oneyeargrowth.labels.append(TRACT_LABEL_NAME, county_name, cbsa_name)
    neighborhood_profile.demographics.oneyeargrowth.colors = COLORS[:4]

    return neighborhood_profile

def set_economy_section(tract, neighborhood_profile, cbsa, county, usa):
    neighborhood_profile.economy.householdincomerange.labels = list(tract.data['Household Income Range']['All'].keys())
    neighborhood_profile.economy.householdincomerange.data1 = list(tract.data['Household Income Range']['All'].values())
    neighborhood_profile.economy.householdincomerange.data2 = list(tract.data['Household Income Range']['Owners'].values())
    neighborhood_profile.economy.householdincomerange.data3 = list(tract.data['Household Income Range']['Renters'].values())

    neighborhood_profile.economy.medianhouseholdincome.labels.append('Neighborhood')
    neighborhood_profile.economy.medianhouseholdincome.data1.append(tract.data['Median Household Income']['All'][-1])
    neighborhood_profile.economy.medianhouseholdincome.data2.append(tract.data['Median Household Income']['Owners'][-1])
    neighborhood_profile.economy.medianhouseholdincome.data3.append(tract.data['Median Household Income']['Renters'][-1])
    neighborhood_profile.economy.medianhouseholdincome.colors = COLORS[:4]


    neighborhood_profile.economy.topemploymentindustries.data = calculate_top_5(tract.data['Employment Industries'])

    neighborhood_profile.economy.employmentindustries.labels = list(tract.data['Employment Industries'].keys())
    neighborhood_profile.economy.employmentindustries.data = list(tract.data['Employment Industries'].values())
    neighborhood_profile.economy.employmentindustries.colors = COLORS[:len(tract.data['Employment Industries'].values())]

    neighborhood_profile.economy.vehiclesowned.labels = list(tract.data['Vehicles Owned']['All'].keys())
    neighborhood_profile.economy.vehiclesowned.data1 = list(tract.data['Vehicles Owned']['All'].keys())
    neighborhood_profile.economy.vehiclesowned.data2 = list(tract.data['Vehicles Owned']['Owners'].keys())
    neighborhood_profile.economy.vehiclesowned.data3 = list(tract.data['Vehicles Owned']['Renters'].keys())
    neighborhood_profile.economy.vehiclesowned.colors = COLORS[:len(tract.data['Vehicles Owned']['All'].values())]


    neighborhood_profile.economy.commutetowork.labels = list(tract.data['Commute to Work'].keys())
    neighborhood_profile.economy.commutetowork.data = list(tract.data['Commute to Work'].values())

    neighborhood_profile.economy.meansoftransportation.labels = list(tract.data['Means of Transportation'].keys())
    neighborhood_profile.economy.meansoftransportation.data = list(tract.data['Means of Transportation'].values())
    neighborhood_profile.economy.meansoftransportation.colors = COLORS[:len(tract.data['Means of Transportation'].values())]



    top_industry_growth = calculate_top_industry_growth(tract.data['Employment Industry Growth'])
    neighborhood_profile.economy.topemploymentindustries.data = top_industry_growth

    neighborhood_profile.economy.unemploymentrate.labels.append('Neighborhood')
    neighborhood_unemployment = tract.data['Unemployment Rate']['Unemployment Rate']
    # check unemployment adjustments

    neighborhood_adjustment_rate = neighborhood_unemployment * county.data['Unemployment Rate']['Unemployment Rate % Change']
    neighborhood_unemployment = neighborhood_unemployment + neighborhood_adjustment_rate
    countyunemployment = county.data['Unemployment Rate']['Unemployment Rate']

    keep adding comparison charts


    neighborhood_profile.economy.unemploymentrate.data = [neighborhood_unemployment, countyunemployment]
    # neighborhood_profile.economy.unemploymentrate.data.append(tract.data['Unemployment Rate']['Unemployment Rate'])
    neighborhood_profile.economy.unemploymentrate.colors = COLORS[:4]

    return neighborhood_profile

def set_housing_section(tract, neighborhood_profile, cbsa, county, usa):
    dominant_housing_type = sorted(tract.data['Property Types']['All'].items(), key=lambda x: (x[1],x[0]), reverse=True)[0][0]
    neighborhood_profile.housing.housingquickfacts.value4 = dominant_housing_type

    neighborhood_profile.housing.occupancyrate.labels = list(tract.data['Occupancy rate'].keys())
    neighborhood_profile.housing.occupancyrate.data = list(tract.data['Occupancy rate'].values())
    neighborhood_profile.housing.occupancyrate.colors = COLORS[:len(tract.data['Occupancy rate'].values())]

    neighborhood_profile.housing.utilitiesincluded.labels = list(tract.data['Utilities in Rent'].keys())
    neighborhood_profile.housing.utilitiesincluded.data = list(tract.data['Utilities in Rent'].values())
    neighborhood_profile.housing.utilitiesincluded.colors = COLORS[:len(tract.data['Utilities in Rent'].values())]

    neighborhood_profile.housing.propertytypes.labels = list(tract.data['Property Types']['All'].keys())
    neighborhood_profile.housing.propertytypes.data1 = list(tract.data['Property Types']['All'].values())
    neighborhood_profile.housing.propertytypes.data2 = list(tract.data['Property Types']['Owners'].values())
    neighborhood_profile.housing.propertytypes.data3 = list(tract.data['Property Types']['Renters'].values())
    neighborhood_profile.housing.propertytypes.colors = COLORS[:len(tract.data['Property Types']['All'].values())]

    neighborhood_profile.housing.yearbuilt.labels = list(tract.data['Year Built'].keys())
    neighborhood_profile.housing.yearbuilt.data = list(tract.data['Year Built'].values())

    neighborhood_profile.housing.numberofbedrooms.labels = list(tract.data['Number of Bedrooms']['All'].keys())
    neighborhood_profile.housing.numberofbedrooms.data1 = list(tract.data['Number of Bedrooms']['All'].values())
    neighborhood_profile.housing.numberofbedrooms.data2 = list(tract.data['Number of Bedrooms']['Owners'].values())
    neighborhood_profile.housing.numberofbedrooms.data3 = list(tract.data['Number of Bedrooms']['Renters'].values())
    neighborhood_profile.housing.numberofbedrooms.colors = COLORS[:len(tract.data['Number of Bedrooms']['All'].values())]

    neighborhood_profile.housing.yearmovedin.labels = list(tract.data['Year Moved In']['All'].keys())
    neighborhood_profile.housing.yearmovedin.data1 = list(tract.data['Year Moved In']['All'].values())
    neighborhood_profile.housing.yearmovedin.data2 = list(tract.data['Year Moved In']['Owners'].values())
    neighborhood_profile.housing.yearmovedin.data3 = list(tract.data['Year Moved In']['Renters'].values())
    neighborhood_profile.housing.yearmovedin.colors = COLORS[:len(tract.data['Year Moved In']['All'].values())]

    neighborhood_profile.housing.incomehousingcost.labels = list(tract.data['% Income on Housing Costs']['All'].keys())
    neighborhood_profile.housing.incomehousingcost.data1 = list(tract.data['% Income on Housing Costs']['All'].values())
    neighborhood_profile.housing.incomehousingcost.data2 = list(tract.data['% Income on Housing Costs']['Owners'].values())
    neighborhood_profile.housing.incomehousingcost.data3 = list(tract.data['% Income on Housing Costs']['Renters'].values())
    neighborhood_profile.housing.incomehousingcost.colors = COLORS[:len(tract.data['% Income on Housing Costs']['All'].values())]

    housingunit = tract.data['Housing Unit Growth']['Total Housing Units']
    housingunit_growth = calculate_historic_growth(housingunit)
    housingunit_oneyeargrowth = calculate_percent_change(tract.data['Housing Unit Growth']['Total Housing Units'][-2], tract.data['Housing Unit Growth']['Total Housing Units'][-1])

    homeowner_oneyeargrowth = calculate_percent_change(tract.data['Homeowner Growth']['Owner'][-2], tract.data['Homeowner Growth']['Owner'][-1])
    renter_oneyeargrowth = calculate_percent_change(tract.data['Renter Growth']['Renter'][-2], tract.data['Renter Growth']['Renter'][-1])

    neighborhood_profile.housing.housingunitgrowth.data1 = housingunit
    neighborhood_profile.housing.housingunitgrowth.labels1 = CENSUS_YEARS
    neighborhood_profile.housing.housingunitgrowth.data2 = housingunit_growth
    neighborhood_profile.housing.housingunitgrowth.labels2 = GROWTH_YEAR_LABELS

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


def calculate_top_5(tract):
    top_5 = sorted(tract.items(), key=lambda x: (x[1],x[0]), reverse=True)[0:5]

    top_5_data = []
    for i, item in enumerate(top_5):
        add_object = {
            'label': item[0],
            'value': str(item[1]) + '%'
        }
        top_5_data.append(add_object)

    return top_5_data

