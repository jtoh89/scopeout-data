import sys
from database import mongoclient
from models import marketmap
from models import tractmarketmaps
from models import geojson as modelGeoJson
from enums import ProductionEnvironment
from utils.utils import list_length_okay, number_to_string
from utils.production import create_url_slug, calculate_percentiles_from_list, assign_color, COLOR_LEVEL_NA, assign_legend_details
from aws.s3 import upload_s3
import numpy as np


TEST_CBSAID = "31080"


def generate_tract_maps():
    scopeout_markets = mongoclient.query_collection(database_name="ScopeOut",
                                                    collection_name="ScopeOutMarkets",
                                                    collection_filter={},
                                                    prod_env=ProductionEnvironment.GEO_ONLY)

    for cbsacode in list(scopeout_markets['cbsacode']):
    # for cbsacode in list(["28140"]):
        cbsa_geo_dict = mongoclient.query_collection(database_name="Geographies",
                                                        collection_name="Cbsa",
                                                        collection_filter={'cbsacode': {'$eq': cbsacode}},
                                                        prod_env=ProductionEnvironment.GEO_ONLY).iloc[0].to_dict()

        cbsa_tracts_geo_df = mongoclient.query_collection(database_name="ScopeOut",
                                                          collection_name="GeojsonTractsBySOMarkets",
                                                          # collection_filter={'countyfullcode': {'$in': list(counties_to_cbsa["countyfullcode"])}},
                                                          collection_filter={"cbsacode": cbsacode},
                                                          prod_env=ProductionEnvironment.GEO_ONLY)

        tracts_data_df = mongoclient.query_collection(database_name="ShortProfiles",
                                                      collection_name="shortneighborhoodprofiles",
                                                      collection_filter={"cbsacode": cbsacode},
                                                      prod_env=ProductionEnvironment.PROD)


        marketname = scopeout_markets[scopeout_markets['cbsacode'] == cbsacode]["cbsaname"].iloc[0]
        tract_map = tractmarketmaps.TractMarketMap()
        tract_map.cbsacode = cbsacode
        tract_map.cbsaname = marketname
        tract_map.coordinates = {
            'lon_x':cbsa_geo_dict['lon_x'],
            'lat_y': cbsa_geo_dict['lat_y']
        }
        tract_map.urlslug = create_url_slug(marketname=marketname, cbsacode=cbsacode)

        print("Start TractsMarketMaps for ", marketname)

        tract_data_percentiles_dict = calculate_percentiles_from_all_tracts(tracts_data_df)

        #region Median Household Income
        assign_legend_details(tract_map.medianhouseholdincomelegend, tract_data_percentiles_dict["median_household_income_percentiles"], 'dollar', 'ascending')
        assign_legend_details(tract_map.unemploymentratelegend, tract_data_percentiles_dict["unemployment_rate_percentiles"], 'percent', 'descending')
        assign_legend_details(tract_map.owneroccupancyratelegend, tract_data_percentiles_dict["owner_occupancy_rate_percentiles"], 'percent', 'ascending')
        #endregion

        for i, row in cbsa_tracts_geo_df.iterrows():
            for tract in row.geojson['features']:
                geo_json_feature = modelGeoJson.GeoJsonFeature()

                geo_json_feature.geometry = tract['geometry']

                geo_json_properties = tractmarketmaps.TractMarketGeoJsonProperties()

                geo_json_feature.id = tract['id']
                geo_json_properties.geoid = tract['id']

                tract_data = tracts_data_df[tracts_data_df['geoid'] == tract['id']]

                if len(tract_data) == 0:
                    # geo_json_feature.properties = geo_json_properties.__dict__
                    # tract_map.medianhouseholdincomecolors.extend([tract['id'], COLOR_LEVEL_NA])
                    # tract_map.unemploymentratecolors.extend([tract['id'], COLOR_LEVEL_NA])
                    # tract_map.owneroccupancyratecolors.extend([tract['id'], COLOR_LEVEL_NA])
                    #
                    # tract_map.geojson.features.append(geo_json_feature.__dict__)
                    continue

                tract_data = tract_data.iloc[0]

                if tract_data.demographics['populationhistorical']['data'][-1] == 0:
                    # print("Skipping geoid with no population: ", tract['id'])
                    continue

                medianhouseholdincome = tract_data.economy['medianhouseholdincome']['data1'][0]
                unemploymentrate = tract_data.economy['unemploymentrate']['data'][0]
                owneroccupancyrate = tract_data.housing['occupancyrate']['data'][0]

                if medianhouseholdincome != medianhouseholdincome or unemploymentrate != unemploymentrate:
                    print('!!! Found NA value for tract!!!')
                    sys.exit()

                geo_json_properties.medianhouseholdincome = medianhouseholdincome
                tract_medianhouseholdincome_color = assign_color(medianhouseholdincome, tract_data_percentiles_dict["median_household_income_percentiles"],  'ascending')
                tract_map.medianhouseholdincomecolors.extend([tract['id'], tract_medianhouseholdincome_color])

                geo_json_properties.unemploymentrate = unemploymentrate
                tract_unemploymentrate_color = assign_color(unemploymentrate, tract_data_percentiles_dict["unemployment_rate_percentiles"], 'descending')
                tract_map.unemploymentratecolors.extend([tract['id'], tract_unemploymentrate_color])

                geo_json_properties.owneroccupancyrate = owneroccupancyrate
                tract_owneroccupancyrate_color = assign_color(owneroccupancyrate, tract_data_percentiles_dict["owner_occupancy_rate_percentiles"], 'ascending')
                tract_map.owneroccupancyratecolors.extend([tract['id'], tract_owneroccupancyrate_color])

                geo_json_feature.properties = geo_json_properties.__dict__

                tract_map.geojson.features.append(geo_json_feature.__dict__)

        tract_map.medianhouseholdincomecolors.append(COLOR_LEVEL_NA)
        tract_map.unemploymentratecolors.append(COLOR_LEVEL_NA)
        tract_map.owneroccupancyratecolors.append(COLOR_LEVEL_NA)
        tract_map.convert_to_dict()

        print("Insert TractsMarketMaps for ", marketname)
        tract_map_dict = tract_map.__dict__

        upload_s3(cbsacode, tract_map_dict)

        # mongoclient.insert_list_mongo(list_data=[tract_map.__dict__],
        #                               dbname='ScopeOutMaps',
        #                               collection_name='TractsMarketMaps',
        #                               prod_env=ProductionEnvironment.MARKET_MAPS,
        #                               collection_update_existing={"cbsacode": cbsacode})






def calculate_percentiles_from_all_tracts(tracts_data_df):
    median_household_income_list = []
    unemployment_rate_list = []
    owner_occupant_rate_list = []

    for i, tract in tracts_data_df.iterrows():
        tract_median_household_income = tract['economy']['medianhouseholdincome']['data1'][0]
        median_household_income_list.append(tract_median_household_income)

        tract_median_unemployment_rate = tract['economy']['unemploymentrate']['data'][0]
        unemployment_rate_list.append(tract_median_unemployment_rate)

        tract_owner_occupant_rate = tract['housing']['occupancyrate']['data'][0]
        owner_occupant_rate_list.append(tract_owner_occupant_rate)

    median_household_income_percentiles = calculate_percentiles_from_list(median_household_income_list)
    owner_occupancy_rate_percentiles = calculate_percentiles_from_list(owner_occupant_rate_list)
    unemployment_rate_percentiles = {
        "percentile_20": 2,
        "percentile_40": 4,
        "percentile_60": 6,
        "percentile_80": 8
    }

    return {
        "median_household_income_percentiles": median_household_income_percentiles,
        "unemployment_rate_percentiles": unemployment_rate_percentiles,
        "owner_occupancy_rate_percentiles": owner_occupancy_rate_percentiles,
    }

