import sys

from database import mongoclient
from models import marketmap
from models import tractmarketmaps
from enums import ProductionEnvironment
from utils.utils import list_length_okay, create_url_slug, calculate_percentiles_from_list, number_to_string
import numpy as np

TEST_CBSAID = "31080"

COLOR_LEVEL_NA = "#999999"
COLOR_LEVEL_1 = "#ff0000"
COLOR_LEVEL_2 = "#ff7f01"
COLOR_LEVEL_3 = "#ffff01"
COLOR_LEVEL_4 = "#004c00"
COLOR_LEVEL_5 = "#00ff01"

def generate_tract_maps():
    scopeout_markets = mongoclient.query_collection(database_name="ScopeOut",
                                                    collection_name="ScopeOutMarkets",
                                                    collection_filter={},
                                                    prod_env=ProductionEnvironment.GEO_ONLY)

    for cbsacode in [TEST_CBSAID]:
        # counties_to_cbsa = mongoclient.query_collection(database_name="Geographies",
        #                                                 collection_name="CountyByCbsa",
        #                                                 collection_filter={'cbsacode': {'$eq': cbsacode}},
        #                                                 prod_env=ProductionEnvironment.GEO_ONLY)

        cbsa_tracts_geo_df = mongoclient.query_collection(database_name="ScopeOut",
                                                          collection_name="EsriTractsBySOMarkets",
                                                          # collection_filter={'countyfullcode': {'$in': list(counties_to_cbsa["countyfullcode"])}},
                                                          collection_filter={"cbsacode": cbsacode},
                                                          prod_env=ProductionEnvironment.GEO_ONLY)

        tracts_data_df = mongoclient.query_collection(database_name="scopeout",
                                                      collection_name="neighborhoodprofiles",
                                                      collection_filter={"cbsacode": cbsacode},
                                                      prod_env=ProductionEnvironment.PRODUCTION)

        tracts_data2_df = mongoclient.query_collection(database_name="scopeout",
                                                       collection_name="neighborhoodprofiles",
                                                       collection_filter={"cbsacode": cbsacode},
                                                       prod_env=ProductionEnvironment.PRODUCTION2)
        tracts_data_df.append(tracts_data2_df)


        marketname = scopeout_markets[scopeout_markets['cbsacode'] == cbsacode]["cbsaname"].iloc[0]
        tract_map = tractmarketmaps.TractMarketMap()
        tract_map.cbsacode = cbsacode
        tract_map.urlslug = create_url_slug(marketname=marketname, cbsacode=cbsacode)

        tract_data_percentiles_dict = calculate_percentiles_from_all_tracts(tracts_data_df)

        assign_legend_details(tract_map.medianhouseholdincomelegend, tract_data_percentiles_dict["median_household_income_percentiles"], 'dollar', 'ascending')
        assign_legend_details(tract_map.unemploymentratelegend, tract_data_percentiles_dict["unemployment_rate_percentiles"], 'percent', 'descending')
        assign_legend_details(tract_map.owneroccupancyratelegend, tract_data_percentiles_dict["owner_occupancy_rate_percentiles"], 'percent', 'ascending')

        for i, tract in cbsa_tracts_geo_df.iterrows():
            geo_json_feature = tractmarketmaps.GeoJsonFeature()

            geo_json_geometry = tractmarketmaps.GeoJsonGeometry()
            geo_json_geometry.coordinates = [tract["rings"]]

            geo_json_properties = tractmarketmaps.GeoJsonProperties()

            geo_json_feature.id = tract.tractcode
            geo_json_properties.geoid = tract.tractcode

            tract_data = tracts_data_df[tracts_data_df['geoid'] == tract.tractcode]

            if len(tract_data) == 0:
                print('!!! WARNING - Could not find matching tract for tractid: ', tract.tractcode)
                geo_json_feature.geometry = geo_json_geometry.__dict__
                geo_json_feature.properties = geo_json_properties.__dict__
                tract_map.medianhouseholdincomecolors.extend([tract.tractcode, COLOR_LEVEL_NA])
                tract_map.unemploymentratecolors.extend([tract.tractcode, COLOR_LEVEL_NA])
                tract_map.owneroccupancyratecolors.extend([tract.tractcode, COLOR_LEVEL_NA])

                tract_map.geojson.features.append(geo_json_feature.__dict__)
                continue

            tract_data = tract_data.iloc[0]

            medianhouseholdincome = tract_data.economy['medianhouseholdincome']['data1'][0]
            unemploymentrate = tract_data.economy['unemploymentrate']['data'][0]
            owneroccupancyrate = tract_data.housing['occupancyrate']['data'][0]

            if medianhouseholdincome != medianhouseholdincome or unemploymentrate != unemploymentrate:
                print('!!! Found NA value for tract!!!')
                sys.exit()

            geo_json_properties.medianhouseholdincome = medianhouseholdincome
            tract_medianhouseholdincome_color = assign_color(medianhouseholdincome, tract_data_percentiles_dict["median_household_income_percentiles"],  'ascending')
            tract_map.medianhouseholdincomecolors.extend([tract.tractcode, tract_medianhouseholdincome_color])

            geo_json_properties.unemploymentrate = unemploymentrate
            tract_unemploymentrate_color = assign_color(unemploymentrate, tract_data_percentiles_dict["unemployment_rate_percentiles"], 'descending')
            tract_map.unemploymentratecolors.extend([tract.tractcode, tract_unemploymentrate_color])

            geo_json_properties.owneroccupancyrate = owneroccupancyrate
            tract_owneroccupancyrate_color = assign_color(owneroccupancyrate, tract_data_percentiles_dict["owner_occupancy_rate_percentiles"], 'ascending')
            tract_map.owneroccupancyratecolors.extend([tract.tractcode, tract_owneroccupancyrate_color])

            geo_json_feature.geometry = geo_json_geometry.__dict__
            geo_json_feature.properties = geo_json_properties.__dict__

            tract_map.geojson.features.append(geo_json_feature.__dict__)

        tract_map.medianhouseholdincomecolors.append(COLOR_LEVEL_NA)
        tract_map.unemploymentratecolors.append(COLOR_LEVEL_NA)
        tract_map.owneroccupancyratecolors.append(COLOR_LEVEL_NA)
        tract_map.convert_to_dict()

        mongoclient.insert_list_mongo(list_data=[tract_map.__dict__],
                                      dbname='ScopeOutMaps',
                                      collection_name='TractsMarketMapsV2',
                                      prod_env=ProductionEnvironment.MARKET_MAPS,
                                      collection_update_existing={"cbsacode": cbsacode})


def assign_legend_details(legend_details, percentiles_dict, data_type, order):
    if data_type == "dollar":
        legend_details.level1description = "Under " + number_to_string(data_type, percentiles_dict['percentile_20'])
        legend_details.level2description = number_to_string(data_type, percentiles_dict['percentile_20']) + " to " + number_to_string(data_type, percentiles_dict['percentile_40'])
        legend_details.level3description = number_to_string(data_type, percentiles_dict['percentile_40']) + " to " + number_to_string(data_type, percentiles_dict['percentile_60'])
        legend_details.level4description = number_to_string(data_type, percentiles_dict['percentile_60']) + " to " + number_to_string(data_type, percentiles_dict['percentile_80'])
        legend_details.level5description = number_to_string(data_type, percentiles_dict['percentile_80']) + " or More"
    elif data_type == "percent":
        legend_details.level1description = "Under " + number_to_string(data_type, percentiles_dict['percentile_20'])
        legend_details.level2description = number_to_string(data_type, percentiles_dict['percentile_20']) + " to " + number_to_string(data_type, percentiles_dict['percentile_40'])
        legend_details.level3description = number_to_string(data_type, percentiles_dict['percentile_40']) + " to " + number_to_string(data_type, percentiles_dict['percentile_60'])
        legend_details.level4description = number_to_string(data_type, percentiles_dict['percentile_60']) + " to " + number_to_string(data_type, percentiles_dict['percentile_80'])
        legend_details.level5description = number_to_string(data_type, percentiles_dict['percentile_80']) + " or More"
    else:
        legend_details.level1description = "Under " + str(percentiles_dict['percentile_20'])
        legend_details.level2description = str(percentiles_dict['percentile_20']) + " to " + str(percentiles_dict['percentile_40'])
        legend_details.level3description = str(percentiles_dict['percentile_40']) + " to " + str(percentiles_dict['percentile_60'])
        legend_details.level4description = str(percentiles_dict['percentile_60']) + " to " + str(percentiles_dict['percentile_80'])
        legend_details.level5description = str(percentiles_dict['percentile_80']) + " or More"

    if order == "ascending":
        legend_details.level1color = COLOR_LEVEL_1
        legend_details.level2color = COLOR_LEVEL_2
        legend_details.level3color = COLOR_LEVEL_3
        legend_details.level4color = COLOR_LEVEL_4
        legend_details.level5color = COLOR_LEVEL_5
    elif order == "descending":
        legend_details.level1color = COLOR_LEVEL_5
        legend_details.level2color = COLOR_LEVEL_4
        legend_details.level3color = COLOR_LEVEL_3
        legend_details.level4color = COLOR_LEVEL_2
        legend_details.level5color = COLOR_LEVEL_1



def assign_color(value, percentiles_dict, order):
    if value != value:
        return COLOR_LEVEL_NA

    if order == "ascending":
        if value < percentiles_dict['percentile_20']:
            return COLOR_LEVEL_1
        elif value < percentiles_dict['percentile_40']:
            return COLOR_LEVEL_2
        elif value < percentiles_dict['percentile_60']:
            return COLOR_LEVEL_3
        elif value < percentiles_dict['percentile_80']:
            return COLOR_LEVEL_4
        else:
            return COLOR_LEVEL_5
    elif order == "descending":
        if value < percentiles_dict['percentile_20']:
            return COLOR_LEVEL_5
        if value < percentiles_dict['percentile_40']:
            return COLOR_LEVEL_4
        if value < percentiles_dict['percentile_60']:
            return COLOR_LEVEL_3
        if value < percentiles_dict['percentile_80']:
            return COLOR_LEVEL_2
        else:
            return COLOR_LEVEL_1



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
    unemployment_rate_percentiles = calculate_percentiles_from_list(unemployment_rate_list)
    owner_occupancy_rate_percentiles = calculate_percentiles_from_list(owner_occupant_rate_list)

    return {
        "median_household_income_percentiles": median_household_income_percentiles,
        "unemployment_rate_percentiles": unemployment_rate_percentiles,
        "owner_occupancy_rate_percentiles": owner_occupancy_rate_percentiles,
    }


def generate_zipcode_maps():
    scopeout_markets = mongoclient.query_collection(database_name="ScopeOut",
                                                    collection_name="ScopeOutMarkets",
                                                    collection_filter={},
                                                    prod_env=ProductionEnvironment.GEO_ONLY)

    cbsa_zip_df = mongoclient.query_collection(database_name="Geographies",
                                               collection_name="EsriZipcodes",
                                               collection_filter={},
                                               prod_env=ProductionEnvironment.GEO_ONLY)

    insert_list = []
    for i, cbsa_data in cbsa_zip_df.iterrows():
        marketname = scopeout_markets[scopeout_markets['cbsacode'] == cbsa_data.cbsacode]["cbsaname"].iloc[0]
        marketmap_data = marketmap.MarketMap()
        marketmap_data.cbsacode = cbsa_data.cbsacode
        marketmap_data.urlslug = create_url_slug(marketname=marketname, cbsacode=cbsa_data.cbsacode)

        for zipcode_data in cbsa_data.zipcodes:
            geometry = []

            # if zipcode_data['zipcode'] not in ["92705", "90275", "92683"]:
            #     continue

            if not list_length_okay(zipcode_data['geometry'], 1):
                print('zipcode geo length >1: ', zipcode_data['zipcode'])
                for zipcode_subgeo in zipcode_data['geometry']:
                    add_list = []
                    for zipcode_subgeo_latlng in zipcode_subgeo:
                        add_list.append({
                            "lng": zipcode_subgeo_latlng[0],
                            "lat": zipcode_subgeo_latlng[1],
                        })

                    geometry.append(add_list)
            else:
                add_list = []
                for latlng in zipcode_data['geometry'][0]:
                    add_list.append({
                        "lng": latlng[0],
                        "lat": latlng[1],
                    })

                geometry = [add_list]

            marketmap_data.zipprofiles.append({
                'zipcode': zipcode_data['zipcode'],
                'geometry': geometry,
                'data': {
                    'medianhouseholdincome': 80000
                }
            })


        insert_list.append(marketmap_data.__dict__)


    mongoclient.insert_list_mongo(list_data=insert_list,
                                  dbname='ScopeOutMaps',
                                  collection_name='MarketMaps',
                                  prod_env=ProductionEnvironment.MARKET_MAPS,
                                  collection_update_existing={})