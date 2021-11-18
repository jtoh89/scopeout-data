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
US_Name = 'United States'

def process_tracts():
    state_id = "01"


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

    cbsa_data = mongoclient.query_collection(database_name="censusdata1",
                                               collection_name="CensusData",
                                               collection_filter=census_cbsa_data_filter,
                                               prod_env=ProductionEnvironment.CENSUS_DATA1)

    usa_data = mongoclient.query_collection(database_name="censusdata1",
                                             collection_name="CensusData",
                                             collection_filter={'geolevel': {'$eq': 'us'}},
                                             prod_env=ProductionEnvironment.CENSUS_DATA1)

    neighborhood_profile_list = []

    for i, tract_profile in census_tract_data.iterrows():
        neighborhood_profile = neighborhoodprofile.NeighborhoodProfile()

        # Set geoid and neighborhood shapes
        neighborhood_profile.geoid = tract_profile.geoid
        neighborhood_profile.geoshapecoordinates = get_neighborhood_map_shape(tract_profile.geoinfo)

        # County
        countyfullcode = tract_profile.geoinfo['countyfullcode']

        county_profile = county_data[county_data['geoid'] == countyfullcode]

        if len(county_profile) > 1:
            print('!!!ERROR - Check why there is more than 1 county record for tractid: {}!!!'.format(tract_profile.geoid))
            sys.exit()
        else:
            county_profile = county_profile.iloc[0]


        cbsainfo = county_cbsa_lookup[county_cbsa_lookup['countyfullcode'] == countyfullcode]

        if len(cbsainfo) == 1:
            cbsacode = cbsainfo['cbsacode'].iloc[0]
            cbsa_profile = cbsa_data[cbsa_data['geoid'] == cbsacode]

            if len(cbsa_profile) != 1:
                print('!!!WARNING - Why do we have missing cbsaid for cbsa data for cbsacode: {}!!!'.format(cbsacode))
                cbsa_profile = None
            else:
                cbsa_profile = cbsa_profile.iloc[0]
        elif len(cbsainfo) > 1:
            print('!!!ERROR - Check why there is more than 1 cbsa record for tractid: {}!!!'.format(tract_profile.geoid))
            sys.exit()
        else:
            cbsa_profile = None

        usa_profile = usa_data.iloc[0]

        neighborhood_profile = set_demographic_section(tract_profile, neighborhood_profile, cbsa_profile, county_profile, usa_profile)
        neighborhood_profile = set_economy_section(tract_profile, neighborhood_profile, cbsa_profile, county_profile, usa_profile)
        neighborhood_profile = set_housing_section(tract_profile, neighborhood_profile, cbsa_profile, county_profile, usa_profile)

        add_dict = neighborhood_profile_to_dict(neighborhood_profile, state_id)
        neighborhood_profile_list.append(add_dict)

    mongoclient.store_neighborhood_data(state_id, neighborhood_profile_list)

def get_neighborhood_map_shape(geoinfo):
    coordinate_list = []
    for coordinate in geoinfo['esristandardgeofeatures']['geometry']['rings'][0]:
        coordinate_list.append({'lng': coordinate[0], 'lat': coordinate[1]})

    return coordinate_list

def neighborhood_profile_to_dict(neighborhood_profile, state_id):
    neighborhood_profile = neighborhood_profile.convert_to_dict()
    neighborhood_profile.stateid = state_id
    return neighborhood_profile.__dict__


def set_demographic_section(tract_profile, neighborhood_profile, cbsa_profile, county_profile, usa_profile):
    neighborhood_profile.demographics.demographicquickfacts.value1 = str(tract_profile.data['Poverty Rate']['Poverty Rate']) + '%'
    neighborhood_profile.demographics.demographicquickfacts.value2 = str(tract_profile.data["% of Children"]["% of Children"]) + '%'
    neighborhood_profile.demographics.demographicquickfacts.value3 = str(tract_profile.data['College Population']['College Population']) + '%'
    neighborhood_profile.demographics.demographicquickfacts.value4 = str(tract_profile.data['Veteran']['Veteran']) + '%'

    neighborhood_profile.demographics.highesteducation.labels = list(tract_profile.data['Highest Education'].keys())
    neighborhood_profile.demographics.highesteducation.data = list(tract_profile.data['Highest Education'].values())
    neighborhood_profile.demographics.highesteducation.colors = COLORS[:len(tract_profile.data['Highest Education'].values())]

    neighborhood_profile.demographics.race.labels = list(tract_profile.data['Race/Ethnicity'].keys())
    neighborhood_profile.demographics.race.data = list(tract_profile.data['Race/Ethnicity'].values())
    neighborhood_profile.demographics.race.colors = COLORS[:len(tract_profile.data['Race/Ethnicity'].values())]

    neighborhood_profile.demographics.agegroups.labels = list(tract_profile.data['Age Groups'].keys())
    neighborhood_profile.demographics.agegroups.data = list(tract_profile.data['Age Groups'].values())
    neighborhood_profile.demographics.agegroups.colors = COLORS[:len(tract_profile.data['Age Groups'].values())]

    neighborhood_profile.demographics.familytype.labels = list(tract_profile.data['Family Type'].keys())
    neighborhood_profile.demographics.familytype.data = list(tract_profile.data['Family Type'].values())
    neighborhood_profile.demographics.familytype.colors = COLORS[:len(tract_profile.data['Family Type'].values())]

    population = tract_profile.data['Population Growth']['Total Population']
    population_growth = calculate_historic_growth(population)

    neighborhood_profile.demographics.populationtrends.data1 = population
    neighborhood_profile.demographics.populationtrends.labels1 = CENSUS_YEARS
    neighborhood_profile.demographics.populationtrends.data2 = population_growth
    neighborhood_profile.demographics.populationtrends.labels2 = GROWTH_YEAR_LABELS

    county_name = county_profile.geoinfo['countyname']

    tract_population_oneyeargrowth = calculate_percent_change(tract_profile.data['Population Growth']['Total Population'][-2], tract_profile.data['Population Growth']['Total Population'][-1])
    county_population_oneyeargrowth = calculate_percent_change(county_profile.data['Population Growth']['Total Population'][-2], county_profile.data['Population Growth']['Total Population'][-1],decimal_places=2)
    us_population_oneyeargrowth = calculate_percent_change(usa_profile.data['Population Growth']['Total Population'][-2], usa_profile.data['Population Growth']['Total Population'][-1])

    if cbsa_profile is not None:
        cbsa_name = cbsa_profile.geoinfo['cbsatitle']
        cbsa_population_oneyeargrowth = calculate_percent_change(cbsa_profile.data['Population Growth']['Total Population'][-2], cbsa_profile.data['Population Growth']['Total Population'][-1])
    else:
        cbsa_name = "N/A"
        cbsa_population_oneyeargrowth = 0

    neighborhood_profile.demographics.oneyeargrowth.data = [tract_population_oneyeargrowth,
                                                            county_population_oneyeargrowth,
                                                                cbsa_population_oneyeargrowth,
                                                                us_population_oneyeargrowth]

    neighborhood_profile.demographics.oneyeargrowth.labels = [TRACT_LABEL_NAME, county_name, cbsa_name, US_Name]
    neighborhood_profile.demographics.oneyeargrowth.colors = COLORS[:4]

    return neighborhood_profile

def set_economy_section(tract_profile, neighborhood_profile, cbsa_profile, county_profile, usa_profile):
    pre_labels = list(tract_profile.data['Household Income Range']['All'].keys())

    household_income_range_relabel = {
        'Less than 25,000':'Less than $25,000',
       '25,000-49,999':'$25,000-$49,999',
       '50,000-74,999':'$50,000-$74,999',
       '75,000-99,999':'$75,000-$99,999',
       '100,000-149,999':'$100,000-$149,999',
       '150,000 or more':'$150,000 or more'
    }

    new_labels = [household_income_range_relabel.get(item,item) for item in pre_labels]
    neighborhood_profile.economy.householdincomerange.labels = new_labels
    neighborhood_profile.economy.householdincomerange.data1 = list(tract_profile.data['Household Income Range']['All'].values())
    neighborhood_profile.economy.householdincomerange.data2 = list(tract_profile.data['Household Income Range']['Owners'].values())
    neighborhood_profile.economy.householdincomerange.data3 = list(tract_profile.data['Household Income Range']['Renters'].values())

    neighborhood_profile.economy.leadingemploymentindustries.data = calculate_top_5(tract_profile.data['Employment Industries'])

    neighborhood_profile.economy.employmentindustries.labels = list(tract_profile.data['Employment Industries'].keys())
    neighborhood_profile.economy.employmentindustries.data = list(tract_profile.data['Employment Industries'].values())
    neighborhood_profile.economy.employmentindustries.colors = COLORS[:len(tract_profile.data['Employment Industries'].values())]

    neighborhood_profile.economy.vehiclesowned.labels = list(tract_profile.data['Vehicles Owned']['All'].keys())
    neighborhood_profile.economy.vehiclesowned.data1 = list(tract_profile.data['Vehicles Owned']['All'].values())
    neighborhood_profile.economy.vehiclesowned.data2 = list(tract_profile.data['Vehicles Owned']['Owners'].values())
    neighborhood_profile.economy.vehiclesowned.data3 = list(tract_profile.data['Vehicles Owned']['Renters'].values())
    neighborhood_profile.economy.vehiclesowned.colors = COLORS[:len(tract_profile.data['Vehicles Owned']['All'].values())]


    neighborhood_profile.economy.commutetowork.labels = list(tract_profile.data['Commute to Work'].keys())
    neighborhood_profile.economy.commutetowork.data = list(tract_profile.data['Commute to Work'].values())

    neighborhood_profile.economy.meansoftransportation.labels = list(tract_profile.data['Means of Transportation'].keys())
    neighborhood_profile.economy.meansoftransportation.data = list(tract_profile.data['Means of Transportation'].values())
    neighborhood_profile.economy.meansoftransportation.colors = COLORS[:len(tract_profile.data['Means of Transportation'].values())]

    # top_industry_growth = calculate_top_industry_growth(tract_profile.data['Employment Industry Growth'])
    # neighborhood_profile.economy.leadingemploymentindustries.data = top_industry_growth

    county_name = county_profile.geoinfo['countyname']

    if cbsa_profile is not None:
        cbsa_name = cbsa_profile.geoinfo['cbsatitle']
        cbsa_medianhouseholdincome_all = cbsa_profile.data['Median Household Income']['All'][-1]
        cbsa_medianhouseholdincome_owner = cbsa_profile.data['Median Household Income']['Owners'][-1]
        cbsa_medianhouseholdincome_renter = cbsa_profile.data['Median Household Income']['Renters'][-1]
        cbsa_unemployment = cbsa_profile.data['Unemployment Rate']['Unemployment Rate']
    else:
        cbsa_name = "N/A"
        cbsa_medianhouseholdincome_all = 0
        cbsa_medianhouseholdincome_owner = 0
        cbsa_medianhouseholdincome_renter = 0
        cbsa_unemployment = 0

    neighborhood_profile.economy.medianhouseholdincome.data1.append(tract_profile.data['Median Household Income']['All'][-1])

    neighborhood_profile.economy.medianhouseholdincome.data1 = [
        tract_profile.data['Median Household Income']['All'][-1],
        county_profile.data['Median Household Income']['All'][-1],
        cbsa_medianhouseholdincome_all,
        usa_profile.data['Median Household Income']['All'][-1]
    ]

    neighborhood_profile.economy.medianhouseholdincome.data2 = [
        tract_profile.data['Median Household Income']['Owners'][-1],
        county_profile.data['Median Household Income']['Owners'][-1],
        cbsa_medianhouseholdincome_owner,
        usa_profile.data['Median Household Income']['Owners'][-1]
    ]

    neighborhood_profile.economy.medianhouseholdincome.data3 = [
        tract_profile.data['Median Household Income']['Renters'][-1],
        county_profile.data['Median Household Income']['Renters'][-1],
        cbsa_medianhouseholdincome_renter,
        usa_profile.data['Median Household Income']['Renters'][-1]
    ]

    neighborhood_profile.economy.medianhouseholdincome.labels = [TRACT_LABEL_NAME, county_name, cbsa_name, US_Name]
    neighborhood_profile.economy.medianhouseholdincome.colors = COLORS[:4]

    neighborhood_profile.economy.unemploymentrate.labels= [TRACT_LABEL_NAME, county_name, cbsa_name, US_Name]
    neighborhood_unemployment = tract_profile.data['Unemployment Rate']['Unemployment Rate']

    neighborhood_adjustment_rate = neighborhood_unemployment * county_profile.data['Unemployment Rate']['Unemployment Rate % Change']
    neighborhood_unemployment = neighborhood_unemployment + neighborhood_adjustment_rate

    neighborhood_profile.economy.unemploymentrate.data = [
        round(neighborhood_unemployment, 1),
        county_profile.data['Unemployment Rate']['Unemployment Rate'],
        cbsa_unemployment,
        usa_profile.data['Unemployment Rate']['Unemployment Rate'],
    ]

    neighborhood_profile.economy.unemploymentrate.colors = COLORS[:4]

    return neighborhood_profile

def set_housing_section(tract_profile, neighborhood_profile, cbsa_profile, county_profile, usa_profile):
    housingunit = tract_profile.data['Housing Unit Growth']['Total Housing Units']
    housingunit_growth = calculate_historic_growth(housingunit)
    housingunit_oneyeargrowth = calculate_percent_change(tract_profile.data['Housing Unit Growth']['Total Housing Units'][-2], tract_profile.data['Housing Unit Growth']['Total Housing Units'][-1])

    homeowner_oneyeargrowth = calculate_percent_change(tract_profile.data['Homeowner Growth']['Owner'][-2], tract_profile.data['Homeowner Growth']['Owner'][-1])
    renter_oneyeargrowth = calculate_percent_change(tract_profile.data['Renter Growth']['Renter'][-2], tract_profile.data['Renter Growth']['Renter'][-1])

    neighborhood_profile.housing.housingunitgrowth.data1 = housingunit
    neighborhood_profile.housing.housingunitgrowth.labels1 = CENSUS_YEARS
    neighborhood_profile.housing.housingunitgrowth.data2 = housingunit_growth
    neighborhood_profile.housing.housingunitgrowth.labels2 = GROWTH_YEAR_LABELS

    dominant_housing_type = sorted(tract_profile.data['Property Types']['All'].items(), key=lambda x: (x[1],x[0]), reverse=True)[0][0]

    neighborhood_profile.housing.housingquickfacts.value1 = str(homeowner_oneyeargrowth) + '%'
    neighborhood_profile.housing.housingquickfacts.value2 = str(renter_oneyeargrowth) + '%'
    neighborhood_profile.housing.housingquickfacts.value3 = str(housingunit_oneyeargrowth) + '%'
    neighborhood_profile.housing.housingquickfacts.value4 = dominant_housing_type


    neighborhood_profile.housing.occupancyrate.labels = list(tract_profile.data['Occupancy rate'].keys())
    neighborhood_profile.housing.occupancyrate.data = list(tract_profile.data['Occupancy rate'].values())
    neighborhood_profile.housing.occupancyrate.colors = COLORS[:len(tract_profile.data['Occupancy rate'].values())]

    neighborhood_profile.housing.utilitiesincluded.labels = list(tract_profile.data['Utilities in Rent'].keys())
    neighborhood_profile.housing.utilitiesincluded.data = list(tract_profile.data['Utilities in Rent'].values())
    neighborhood_profile.housing.utilitiesincluded.colors = COLORS[:len(tract_profile.data['Utilities in Rent'].values())]

    neighborhood_profile.housing.propertytypes.labels = list(tract_profile.data['Property Types']['All'].keys())
    neighborhood_profile.housing.propertytypes.data1 = list(tract_profile.data['Property Types']['All'].values())
    neighborhood_profile.housing.propertytypes.data2 = list(tract_profile.data['Property Types']['Owners'].values())
    neighborhood_profile.housing.propertytypes.data3 = list(tract_profile.data['Property Types']['Renters'].values())
    neighborhood_profile.housing.propertytypes.colors = COLORS[:len(tract_profile.data['Property Types']['All'].values())]

    neighborhood_profile.housing.yearbuilt.labels = list(tract_profile.data['Year Built'].keys())
    neighborhood_profile.housing.yearbuilt.data = list(tract_profile.data['Year Built'].values())

    neighborhood_profile.housing.numberofbedrooms.labels = list(tract_profile.data['Number of Bedrooms']['All'].keys())
    neighborhood_profile.housing.numberofbedrooms.data1 = list(tract_profile.data['Number of Bedrooms']['All'].values())
    neighborhood_profile.housing.numberofbedrooms.data2 = list(tract_profile.data['Number of Bedrooms']['Owners'].values())
    neighborhood_profile.housing.numberofbedrooms.data3 = list(tract_profile.data['Number of Bedrooms']['Renters'].values())
    neighborhood_profile.housing.numberofbedrooms.colors = COLORS[:len(tract_profile.data['Number of Bedrooms']['All'].values())]

    neighborhood_profile.housing.yearmovedin.labels = list(tract_profile.data['Year Moved In']['All'].keys())
    neighborhood_profile.housing.yearmovedin.data1 = list(tract_profile.data['Year Moved In']['All'].values())
    neighborhood_profile.housing.yearmovedin.data2 = list(tract_profile.data['Year Moved In']['Owners'].values())
    neighborhood_profile.housing.yearmovedin.data3 = list(tract_profile.data['Year Moved In']['Renters'].values())
    neighborhood_profile.housing.yearmovedin.colors = COLORS[:len(tract_profile.data['Year Moved In']['All'].values())]

    neighborhood_profile.housing.incomehousingcost.labels = list(tract_profile.data['% Income on Housing Costs']['All'].keys())
    neighborhood_profile.housing.incomehousingcost.data1 = list(tract_profile.data['% Income on Housing Costs']['All'].values())
    neighborhood_profile.housing.incomehousingcost.data2 = list(tract_profile.data['% Income on Housing Costs']['Owners'].values())
    neighborhood_profile.housing.incomehousingcost.data3 = list(tract_profile.data['% Income on Housing Costs']['Renters'].values())
    neighborhood_profile.housing.incomehousingcost.colors = COLORS[:len(tract_profile.data['% Income on Housing Costs']['All'].values())]



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

