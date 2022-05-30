import sys
from models.shortneighborhoorprofile import shortneighborhoodprofile
from database import mongoclient
from enums import GeoLevels
from enums import ProductionEnvironment
from utils.utils import calculate_percent_change, zero_to_null, list_0_to_None
from utils.production import get_county_cbsa_lookup
from census.censusdata import STATES1, STATES2
from globals import CENSUS_YEARS
from scopeoutdata import helpers


COLORS = ["green", "red", "yellow", "blue", "brown", "teal", "purple", "#65AFFF", "#4F6D7A", "#828489", "#D81E5B", "#FDF0D5", "#D4F2DB"]

CENSUS_YEARS_SHORT = CENSUS_YEARS[-6:]
GROWTH_YEAR_LABELS = CENSUS_YEARS_SHORT[1:]
TRACT_LABEL_NAME = 'Neighborhood'
US_Name = 'United States'

def create_short_neighborhood_profiles():
    '''
    Function stores neighborhood profiles for app
    :return:
    '''

    collection_find_finished_runs = {
        'category': 'shortneighborhoodprofiles',
        'geo_level': GeoLevels.TRACT.value,
    }
    finished_runs = mongoclient.get_finished_runs(collection_find_finished_runs)

    for stategroupindex, stategroup in enumerate([STATES1, STATES2]):
        prod_env = ProductionEnvironment.CENSUS_DATA1
        if stategroupindex == 1:
            prod_env = ProductionEnvironment.CENSUS_DATA2

        for stateid in stategroup:
            if len(finished_runs) > 0:
                match_found = finished_runs[finished_runs['state_id'] == stateid]['category'].values

                if match_found:
                    print('Skipping state {}. Neighborhood profiles already exists.'.format(stateid))
                    continue

            all_dict = helpers.get_all_geo_data_for_neighborhoods(stateid, prod_env)
            census_tract_data = all_dict['census_tract_data']
            county_data = all_dict['county_data']
            county_market_profiles = all_dict['county_market_profiles']
            county_cbsa_lookup = all_dict['county_cbsa_lookup']
            cbsa_data = all_dict['cbsa_data']
            usa_data = all_dict['usa_data']

            neighborhood_profile_list = []

            for i, tract_profile in census_tract_data.iterrows():
                neighborhood_profile = shortneighborhoodprofile.ShortNeighborhoodProfile()

                # Set geoid and neighborhood shapes
                neighborhood_profile.geoid = tract_profile.geoid
                neighborhood_profile.countyfullcode = False
                neighborhood_profile.countyname = ''
                neighborhood_profile.cbsacode = False
                neighborhood_profile.cbsaname = ''

                # County
                countyfullcode = tract_profile.geoinfo['countyfullcode']
                county_profile = county_data[county_data['geoid'] == countyfullcode]

                if len(county_profile) > 1:
                    print('!!!ERROR - Check why there is more than 1 county record for tractid: {}!!!'.format(tract_profile.geoid))
                    sys.exit()
                elif len(county_profile) == 0:
                    print('!!!ERROR - Check why there is there no county record for tractid: {}!!!'.format(tract_profile.geoid))
                    sys.exit()
                else:
                    county_profile = county_profile.iloc[0]
                    neighborhood_profile.countyfullcode = county_profile.geoid
                    neighborhood_profile.countyname = county_profile.geoinfo['countyname']

                cbsainfo = county_cbsa_lookup[county_cbsa_lookup['countyfullcode'] == countyfullcode]

                if len(cbsainfo) == 1:
                    cbsacode = cbsainfo['cbsacode'].iloc[0]
                    cbsa_profile = cbsa_data[cbsa_data['geoid'] == cbsacode]

                    if len(cbsa_profile) != 1:
                        print('!!!WARNING - Why do we have missing cbsaid for cbsa zipcodegeojson for cbsacode: {}!!!'.format(cbsacode))
                        cbsa_profile = None
                    else:
                        cbsa_profile = cbsa_profile.iloc[0]
                        neighborhood_profile.cbsacode = cbsa_profile.geoid
                        neighborhood_profile.cbsaname = cbsa_profile.geoinfo['cbsaname']

                elif len(cbsainfo) > 1:
                    print('!!!ERROR - Check why there is more than 1 cbsa record for tractid: {}!!!'.format(tract_profile.geoid))
                    sys.exit()
                else:
                    cbsa_profile = None

                usa_profile = usa_data.iloc[0]

                neighborhood_profile = set_demographic_section(tract_profile, neighborhood_profile, cbsa_profile, county_profile, usa_profile)
                neighborhood_profile = set_economy_section(tract_profile, neighborhood_profile, cbsa_profile, county_profile, usa_profile)
                neighborhood_profile = set_housing_section(tract_profile, neighborhood_profile, cbsa_profile, county_profile, usa_profile)

                add_dict = neighborhood_profile_to_dict(neighborhood_profile, stateid)
                neighborhood_profile_list.append(add_dict)

            success = mongoclient.store_neighborhood_data(stateid, neighborhood_profile_list, None, "shortneighborhoodprofiles")

            if success:
                collection_add_finished_run = {
                    'category': 'shortneighborhoodprofiles',
                    'geo_level': GeoLevels.TRACT.value,
                    'state_id': stateid,
                }

                print('Successfully stored neighborhood profile for stateid: {}'.format(stateid))
                mongoclient.add_finished_run(collection_add_finished_run)

def process_property_types(neighborhood_profile_object, market_profile, propertytype):
    if propertytype in market_profile.keys():
        neighborhood_profile_object.labels = market_profile[propertytype]['labels']
        neighborhood_profile_object.data1 = market_profile[propertytype]['data1']
        neighborhood_profile_object.data1Name = market_profile[propertytype]['data1Name']
        neighborhood_profile_object.data2 = market_profile[propertytype]['data2']
        neighborhood_profile_object.data2Name = market_profile[propertytype]['data2Name']
        neighborhood_profile_object.data3 = market_profile[propertytype]['data3']
        neighborhood_profile_object.data3Name = market_profile[propertytype]['data3Name']

def set_market_trends_section(tract_profile, neighborhood_profile, county_market_profiles):
    county_market_profiles = county_market_profiles[county_market_profiles['countyfullcode'] == tract_profile.countyfullcode].iloc[0]

    if len(county_market_profiles) < 1:
        print('No market trends')
        return

    if county_market_profiles.realestatedata:
        if 'median_sale_price' in county_market_profiles.realestatedata.keys():
            process_property_types(neighborhood_profile.marketprofile.mediansaleprice.all, county_market_profiles.realestatedata['median_sale_price'], 'allresidential')
            process_property_types(neighborhood_profile.marketprofile.mediansaleprice.singlefamily, county_market_profiles.realestatedata['median_sale_price'], 'singlefamily')
            process_property_types(neighborhood_profile.marketprofile.mediansaleprice.multifamily, county_market_profiles.realestatedata['median_sale_price'], 'multifamily')

        if 'median_ppsf' in county_market_profiles.realestatedata.keys():
            process_property_types(neighborhood_profile.marketprofile.medianppsf.all, county_market_profiles.realestatedata['median_ppsf'], 'allresidential')
            process_property_types(neighborhood_profile.marketprofile.medianppsf.singlefamily, county_market_profiles.realestatedata['median_ppsf'], 'singlefamily')
            process_property_types(neighborhood_profile.marketprofile.medianppsf.multifamily, county_market_profiles.realestatedata['median_ppsf'], 'multifamily')

        if 'months_of_supply' in county_market_profiles.realestatedata.keys():
            process_property_types(neighborhood_profile.marketprofile.monthsofsupply.all, county_market_profiles.realestatedata['months_of_supply'], 'allresidential')
            process_property_types(neighborhood_profile.marketprofile.monthsofsupply.singlefamily, county_market_profiles.realestatedata['months_of_supply'], 'singlefamily')
            process_property_types(neighborhood_profile.marketprofile.monthsofsupply.multifamily, county_market_profiles.realestatedata['months_of_supply'], 'multifamily')

        if 'median_dom' in county_market_profiles.realestatedata.keys():
            process_property_types(neighborhood_profile.marketprofile.mediandom.all, county_market_profiles.realestatedata['median_dom'], 'allresidential')
            process_property_types(neighborhood_profile.marketprofile.mediandom.singlefamily, county_market_profiles.realestatedata['median_dom'], 'singlefamily')
            process_property_types(neighborhood_profile.marketprofile.mediandom.multifamily, county_market_profiles.realestatedata['median_dom'], 'multifamily')

        if 'price_drops' in county_market_profiles.realestatedata.keys():
            process_property_types(neighborhood_profile.marketprofile.pricedrops.all, county_market_profiles.realestatedata['price_drops'], 'allresidential')
            process_property_types(neighborhood_profile.marketprofile.pricedrops.singlefamily, county_market_profiles.realestatedata['price_drops'], 'singlefamily')
            process_property_types(neighborhood_profile.marketprofile.pricedrops.multifamily, county_market_profiles.realestatedata['price_drops'], 'multifamily')
    else:
        neighborhood_profile.marketprofile.mediansaleprice.hasData = False
        neighborhood_profile.marketprofile.medianppsf.hasData = False
        neighborhood_profile.marketprofile.monthsofsupply.hasData = False
        neighborhood_profile.marketprofile.mediandom.hasData = False
        neighborhood_profile.marketprofile.pricedrops.hasData = False

    if county_market_profiles.rentaldata:
        neighborhood_profile.marketprofile.rentaltrends = county_market_profiles.rentaldata
    else:
        neighborhood_profile.marketprofile.rentaltrends.hasData = False

    if county_market_profiles.unemploymentrate:
        neighborhood_profile.marketprofile.unemploymentrate = county_market_profiles.unemploymentrate
    else:
        neighborhood_profile.marketprofile.unemploymentrate.hasData = False

    return neighborhood_profile


def set_demographic_section(tract_profile, neighborhood_profile, cbsa_profile, county_profile, usa_profile):
    if 'Population Growth' not in tract_profile.data.keys():
        population = []
        population_growth = []
        neighborhood_profile.demographics.oneyeargrowth.hasData = False
    else:
        population = tract_profile.data['Population Growth']['Total Population']

    neighborhood_profile.demographics.race.labels = list(tract_profile.data['Race/Ethnicity'].keys())
    neighborhood_profile.demographics.race.data = list(tract_profile.data['Race/Ethnicity'].values())
    neighborhood_profile.demographics.race.colors = COLORS[:len(tract_profile.data['Race/Ethnicity'].values())]

    neighborhood_profile.demographics.agegroups.labels = list(tract_profile.data['Age Groups'].keys())
    neighborhood_profile.demographics.agegroups.data = list(tract_profile.data['Age Groups'].values())
    neighborhood_profile.demographics.agegroups.colors = COLORS[:len(tract_profile.data['Age Groups'].values())]

    neighborhood_profile.demographics.populationhistorical.data = population
    neighborhood_profile.demographics.populationhistorical.labels = CENSUS_YEARS

    return neighborhood_profile

def set_economy_section(tract_profile, neighborhood_profile, cbsa_profile, county_profile, usa_profile):
    county_name = county_profile.geoinfo['countyname']

    #region Median Household Income
    if cbsa_profile is not None:
        cbsa_name = cbsa_profile.geoinfo['cbsaname']
        cbsa_medianhouseholdincome_all = zero_to_null(cbsa_profile.data['Median Household Income']['All'][-1])
        cbsa_medianhouseholdincome_owner = zero_to_null(cbsa_profile.data['Median Household Income']['Owners'][-1])
        cbsa_medianhouseholdincome_renter = zero_to_null(cbsa_profile.data['Median Household Income']['Renters'][-1])
        cbsa_unemployment = cbsa_profile.data['Unemployment Rate']['Unemployment Rate']
    else:
        cbsa_name = "N/A"
        cbsa_medianhouseholdincome_all = 0
        cbsa_medianhouseholdincome_owner = 0
        cbsa_medianhouseholdincome_renter = 0
        cbsa_unemployment = 0
    neighborhood_profile.economy.medianhouseholdincome.data1.append(tract_profile.data['Median Household Income']['All'][-1])

    neighborhood_profile.economy.medianhouseholdincome.data1 = [
        zero_to_null(tract_profile.data['Median Household Income']['All'][-1]),
        zero_to_null(county_profile.data['Median Household Income']['All'][-1]),
        cbsa_medianhouseholdincome_all,
        zero_to_null(usa_profile.data['Median Household Income']['All'][-1])
    ]

    neighborhood_profile.economy.medianhouseholdincome.data2 = [
        zero_to_null(tract_profile.data['Median Household Income']['Owners'][-1]),
        zero_to_null(county_profile.data['Median Household Income']['Owners'][-1]),
        cbsa_medianhouseholdincome_owner,
        zero_to_null(usa_profile.data['Median Household Income']['Owners'][-1])
    ]

    neighborhood_profile.economy.medianhouseholdincome.data3 = [
        zero_to_null(tract_profile.data['Median Household Income']['Renters'][-1]),
        zero_to_null(county_profile.data['Median Household Income']['Renters'][-1]),
        cbsa_medianhouseholdincome_renter,
        zero_to_null(usa_profile.data['Median Household Income']['Renters'][-1])
    ]

    neighborhood_profile.economy.medianhouseholdincome.labels = [TRACT_LABEL_NAME, county_name, cbsa_name, US_Name]
    neighborhood_profile.economy.medianhouseholdincome.colors = COLORS[:4]
    #endregion

    #region Median Household Income Historical
    neighborhood_profile.economy.medianhouseholdincomehistorical.labels = CENSUS_YEARS
    neighborhood_profile.economy.medianhouseholdincomehistorical.data1 = list_0_to_None(tract_profile.data['Median Household Income']['All'])
    neighborhood_profile.economy.medianhouseholdincomehistorical.data2 = list_0_to_None(tract_profile.data['Median Household Income']['Owners'])
    neighborhood_profile.economy.medianhouseholdincomehistorical.data3 = list_0_to_None(tract_profile.data['Median Household Income']['Renters'])

    if len(tract_profile.data['Median Household Income']['All']) == 1:
        neighborhood_profile.economy.medianhouseholdincomehistorical.hasData = False

    #endregion

    #region Unemployment Rate
    neighborhood_profile.economy.unemploymentrate.labels = [TRACT_LABEL_NAME, county_name, cbsa_name, US_Name]
    neighborhood_unemployment = tract_profile.data['Unemployment Rate']['Unemployment Rate']

    if 'Unemployment Rate % Change' in county_profile.data['Unemployment Rate'].keys():
        neighborhood_adjustment_rate = neighborhood_unemployment * county_profile.data['Unemployment Rate']['Unemployment Rate % Change']
    elif cbsa_profile is not None and 'Unemployment Rate % Change' in cbsa_profile.data['Unemployment Rate'].keys():
        neighborhood_adjustment_rate = neighborhood_unemployment * cbsa_profile.data['Unemployment Rate']['Unemployment Rate % Change']
    else:
        neighborhood_adjustment_rate = 0

    neighborhood_unemployment = neighborhood_unemployment + neighborhood_adjustment_rate

    neighborhood_profile.economy.unemploymentrate.data = [
        round(neighborhood_unemployment, 1),
        county_profile.data['Unemployment Rate']['Unemployment Rate'],
        cbsa_unemployment,
        usa_profile.data['Unemployment Rate']['Unemployment Rate'],
    ]

    neighborhood_profile.economy.unemploymentrate.colors = COLORS[:4]
    #endregion


    return neighborhood_profile

def set_housing_section(tract_profile, neighborhood_profile, cbsa_profile, county_profile, usa_profile):
    neighborhood_profile.housing.occupancyrate.labels = list(tract_profile.data['Occupancy rate'].keys())
    neighborhood_profile.housing.occupancyrate.data = list(tract_profile.data['Occupancy rate'].values())
    neighborhood_profile.housing.occupancyrate.colors = COLORS[:len(tract_profile.data['Occupancy rate'].values())]

    neighborhood_profile.housing.propertytypes.labels = list(tract_profile.data['Property Types']['All'].keys())
    neighborhood_profile.housing.propertytypes.data1 = list(tract_profile.data['Property Types']['All'].values())
    neighborhood_profile.housing.propertytypes.data2 = list(tract_profile.data['Property Types']['Owners'].values())
    neighborhood_profile.housing.propertytypes.data3 = list(tract_profile.data['Property Types']['Renters'].values())
    neighborhood_profile.housing.propertytypes.colors = COLORS[:len(tract_profile.data['Property Types']['All'].values())]


    return neighborhood_profile


def neighborhood_profile_to_dict(neighborhood_profile, state_id):
    neighborhood_profile = neighborhood_profile.convert_to_dict()
    neighborhood_profile.stateid = state_id
    return neighborhood_profile.__dict__




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

