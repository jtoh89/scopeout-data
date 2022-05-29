import sys
from database import mongoclient
from enums import ProductionEnvironment, GeoLevels, Collections_Historical_Profiles, Collections_Profiles
from utils.utils import set_na_to_false_from_dict, list_float_to_percent, two_list_to_dict
from utils.production import create_url_slug, calculate_percent_change
from models import cbsamarketprofile
from globals import SCOPEOUT_COLOR, RED_COLOR, BLUE_COLOR, BORDER_COLOR
from lookups import SCOPEOUT_MARKET_LIST

def generate_cbsa_market_profiles(prod_env, geoid_field):
    # us_historical_profile = mongoclient.query_collection(database_name="MarketProfiles",
    #                                              collection_name=Collections_Historical_Profiles.USA.value,
    #                                              collection_filter={'geolevel': GeoLevels.USA.value},
    #                                              prod_env=ProductionEnvironment.MARKET_PROFILES)
    # us_historical_profile = us_historical_profile.to_dict()

    scopeout_markets = mongoclient.query_collection(database_name="ScopeOut",
                                                    collection_name="ScopeOutMarkets",
                                                    collection_filter={},
                                                    prod_env=ProductionEnvironment.GEO_ONLY)

    all_scopeout_markets = list(scopeout_markets['cbsacode'])

    cbsa_historical_profiles = mongoclient.query_collection(database_name="MarketProfiles",
                                                 collection_name=Collections_Historical_Profiles.CBSA.value,
                                                 collection_filter={'geolevel': GeoLevels.CBSA.value},
                                                 prod_env=ProductionEnvironment.MARKET_PROFILES)


    census_cbsa_data = mongoclient.query_collection(database_name="CensusData1",
                                                 collection_name="CensusData",
                                                 collection_filter={'geolevel': GeoLevels.CBSA.value},
                                                 prod_env=ProductionEnvironment.CENSUS_DATA1)


    cbsa_market_profile_list = []

    for i, row in cbsa_historical_profiles.iterrows():
        cbsa_profile = row.to_dict()

        if row[geoid_field] not in all_scopeout_markets:
            continue

        set_na_to_false_from_dict(cbsa_profile)
        cbsa_market_profile = cbsamarketprofile.CbsaMarketProfile()

        cbsa_market_profile.cbsacode = row[geoid_field]
        cbsa_market_profile.cbsaname = row['geoname']
        cbsa_market_profile.urlslug = create_url_slug(marketname=row['geoname'], cbsacode=cbsa_market_profile.cbsacode)

        if row['cbsacode'] not in list(scopeout_markets['cbsacode']):
            print("!!! Why is cbsacode not found in scopeout markets? !!!!")
            sys.exit()

        census_cbsa_data_match = census_cbsa_data[census_cbsa_data['geoid'] == row[geoid_field]]

        if len(census_cbsa_data_match) > 0:
            census_cbsa_data_match = census_cbsa_data_match.data.iloc[0]
        else:
            census_cbsa_data_match = False

        if cbsa_profile['realestatetrends']:
            # Median Sale Price
            cbsa_market_profile.mediansaleprice.labels = cbsa_profile['realestatetrends']['dates']
            cbsa_market_profile.mediansaleprice.data = cbsa_profile['realestatetrends']['mediansaleprice']

            # Median Sale Price MoM
            cbsa_market_profile.mediansalepricemom.labels = cbsa_profile['realestatetrends']['dates']
            cbsa_market_profile.mediansalepricemom.data = list_float_to_percent(cbsa_profile['realestatetrends']['mediansalepricemom'])

            # Median PPSF
            cbsa_market_profile.medianppsf.data = cbsa_profile['realestatetrends']['medianppsf']
            cbsa_market_profile.medianppsf.labels = cbsa_profile['realestatetrends']['dates']

            # Months of Supply
            cbsa_market_profile.monthsofsupply.data = cbsa_profile['realestatetrends']['monthsofsupply']
            cbsa_market_profile.monthsofsupply.labels = cbsa_profile['realestatetrends']['dates']

            # Days on Market
            cbsa_market_profile.mediandom.data = cbsa_profile['realestatetrends']['mediandom']
            cbsa_market_profile.mediandom.labels = cbsa_profile['realestatetrends']['dates']

            # Price Drops
            cbsa_market_profile.pricedrops.data = list_float_to_percent(cbsa_profile['realestatetrends']['pricedrops'])
            cbsa_market_profile.pricedrops.labels = cbsa_profile['realestatetrends']['dates']

        if cbsa_profile['rentaltrends']:
            cbsa_market_profile.rentaltrends.dataName = "Median Rent"
            cbsa_market_profile.rentaltrends.data = cbsa_profile['rentaltrends']['median_rent']
            cbsa_market_profile.rentaltrends.labels = cbsa_profile['rentaltrends']['dates']

        if 'buildingpermits' in cbsa_profile.keys() and cbsa_profile['buildingpermits']:
            cbsa_market_profile.buildingpermits.dataName = "Total Units Permitted"
            cbsa_market_profile.buildingpermits.labels = cbsa_profile['buildingpermits']['dates']
            cbsa_market_profile.buildingpermits.data = cbsa_profile['buildingpermits']['total']

        if 'historicunemploymentrate' in cbsa_profile.keys() and cbsa_profile['historicunemploymentrate']:
            cbsa_market_profile.unemploymentrate.dataName = "Unemployment Rate"
            cbsa_market_profile.unemploymentrate.labels = cbsa_profile['historicunemploymentrate']['dates']
            cbsa_market_profile.unemploymentrate.data = cbsa_profile['historicunemploymentrate']['unemploymentrate']

        if census_cbsa_data_match:
            # Total Population
            cbsa_market_profile.totalpopulationgrowth.dataName = "Total Population"
            cbsa_market_profile.totalpopulationgrowth.labels = census_cbsa_data_match['Population Growth']['years']
            cbsa_market_profile.totalpopulationgrowth.data = census_cbsa_data_match['Population Growth']['Total Population']

            # Median Household Income
            cbsa_market_profile.medianhouseholdincome.dataName = "Median Household Income"
            cbsa_market_profile.medianhouseholdincome.labels = census_cbsa_data_match['Median Household Income']['years']
            cbsa_market_profile.medianhouseholdincome.data = census_cbsa_data_match['Median Household Income']['All']

            # Housing unit vs Household growth change
            housing_unit_growth_dict = two_list_to_dict(key_list=census_cbsa_data_match['Housing Unit Growth']['years'],
                                                        value_list=census_cbsa_data_match['Housing Unit Growth']['Total Housing Units'])

            household_growth_dict = two_list_to_dict(key_list=census_cbsa_data_match['Total Households']['years'],
                                                        value_list=census_cbsa_data_match['Total Households']['Total Households'])

            if housing_unit_growth_dict.keys() == household_growth_dict.keys():
                housing_units_add = []
                households_add = []

                years = list(housing_unit_growth_dict.keys())
                for i2, year in enumerate(years):
                    # skip year 2012, weird anomoly data where huge difference in households/housing units
                    if i2 == 0 or year == '2013':
                        continue
                    prev_year = years[i2-1]

                    change = calculate_percent_change(housing_unit_growth_dict[prev_year], housing_unit_growth_dict[year])

                    if change > 3 or change < -3:
                        print("{}. year: {}. change: {}".format(cbsa_market_profile.cbsaname, year, change))

                    housing_units_add.append(housing_unit_growth_dict[year]-housing_unit_growth_dict[prev_year])
                    households_add.append(household_growth_dict[year]-household_growth_dict[prev_year])

                housing_unit_added = {
                    "label": "Housing Units",
                    "fill": True,
                    "borderColor": BORDER_COLOR,
                    "data": housing_units_add,
                    "backgroundColor": BLUE_COLOR,
                }

                households_added = {
                    "label": "Households",
                    "fill": True,
                    "data": households_add,
                    "borderColor": BORDER_COLOR,
                    "backgroundColor": SCOPEOUT_COLOR,
                }

                cbsa_market_profile.housingunitsvshouseholdschange.colors = "Total Housing Units"
                cbsa_market_profile.housingunitsvshouseholdschange.labels = years[2:]
                cbsa_market_profile.housingunitsvshouseholdschange.datasets = [housing_unit_added, households_added]
            else:
                print("!!! historical years not same between housing units and houshold growth !!!")
                sys.exit()

        cbsa_market_profile.convert_to_dict()
        cbsa_market_profile_list.append(cbsa_market_profile.__dict__)


    client = mongoclient.connect_to_client(prod_env=prod_env)
    dbname = 'MarketProfiles'
    db = client[dbname]
    collection = db[Collections_Profiles.CBSA.value]

    collection_filter = {}

    success = mongoclient.batch_inserts_with_list(cbsa_market_profile_list, collection, collection_filter, geoid_field)

    if success:
        print("Successfully stored batch into Mongo. Rows inserted: ", len(cbsa_market_profile_list))
        return success
