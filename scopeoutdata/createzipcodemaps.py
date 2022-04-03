import sys
from database import mongoclient
from models import marketmap
from models import tractmarketmaps, zipcodemarketmap
from models import geojson as modelGeoJson
from enums import ProductionEnvironment
from utils.utils import list_length_okay, create_url_slug, calculate_percentiles_from_list, number_to_string, assign_color, COLOR_LEVEL_NA, assign_legend_details
import numpy as np

TEST_CBSAID = "31080"


def generate_zipcode_maps():
    zip_code_data = mongoclient.query_collection(database_name="MarketTrends",
                                                    collection_name="redfinzipcodedata",
                                                    collection_filter={},
                                                    prod_env=ProductionEnvironment.MARKET_TRENDS)

    zip_code_geojson = mongoclient.query_collection(database_name="ScopeOut",
                                                 collection_name="GeojsonZipcodesBySOMarkets",
                                                 collection_filter={},
                                                 prod_env=ProductionEnvironment.GEO_ONLY)


    for i, scopeout_market in zip_code_geojson.iterrows():
        cbsacode = scopeout_market.cbsacode
        cbsaname = scopeout_market.geojson['name']

        if cbsacode != "31080":
            continue

        all_zip_median_sale_price = []
        all_zip_dom = []
        all_zip_ppsf = []


        missing_zipcode_count = 0

        # create zipcode market map
        zipcode_market_map = zipcodemarketmap.ZipcodeMarketMap()
        zipcode_market_map.cbsacode = cbsacode
        zipcode_market_map.urlslug = create_url_slug(cbsacode, cbsaname)
        # iterate through geojson features to add property and build metric lists
        for zip_geojson_feature in scopeout_market.geojson['features']:
            matching_zip_data = zip_code_data[zip_code_data['zipcode'] == zip_geojson_feature['id']]

            zipcode_geojson_property = zipcodemarketmap.ZipcodeGeoJsonProperties()

            if len(matching_zip_data) == 0:
                print('No matching zipcode found')
                zipcode_geojson_property.mediansaleprice = None
                zipcode_geojson_property.dom = None
                zipcode_geojson_property.ppsf = None
                zipcode_geojson_property.geoid = zip_geojson_feature['id']
                zip_geojson_feature['properties'] = zipcode_geojson_property
                missing_zipcode_count += 1
                continue
            else:
                matching_zip_data = matching_zip_data.iloc[0]

            median_sale_price = matching_zip_data.median_sale_price
            dom = matching_zip_data.median_dom
            ppsf = matching_zip_data.median_ppsf

            # build list of all metrics
            all_zip_median_sale_price.append(median_sale_price)
            all_zip_dom.append(dom)
            all_zip_ppsf.append(ppsf)

            # assign item to geojson property
            zipcode_geojson_property.geoid = zip_geojson_feature['id']
            zipcode_geojson_property.mediansaleprice = median_sale_price
            zipcode_geojson_property.dom = dom
            zipcode_geojson_property.ppsf = ppsf

            # assign zipcode geojson property
            zip_geojson_feature['properties'] = zipcode_geojson_property

        print("Zipcode coverage for {} is {}".format(cbsaname, ((len(scopeout_market.geojson['features']) - missing_zipcode_count) / len(scopeout_market.geojson['features']))))


        # calculate percentiles
        median_sale_price_percentiles = calculate_percentiles_from_list(all_zip_median_sale_price)
        dom_percentiles = calculate_percentiles_from_list(all_zip_dom)
        ppsf_percentiles = calculate_percentiles_from_list(all_zip_ppsf)


        #iterate again to assign colors
        for zip_geojson_feature in scopeout_market.geojson['features']:
            zipcode = zip_geojson_feature['id']

            median_sale_price = zip_geojson_feature['properties'].mediansaleprice
            dom = zip_geojson_feature['properties'].dom
            ppsf = zip_geojson_feature['properties'].ppsf


            median_sale_price_color = assign_color(median_sale_price, median_sale_price_percentiles, 'ascending')
            zipcode_market_map.mediansalepricecolors.extend([zipcode, median_sale_price_color])

            dom_color = assign_color(dom, dom_percentiles, 'ascending')
            zipcode_market_map.domcolors.extend([zipcode, dom_color])

            ppsf_color = assign_color(ppsf, ppsf_percentiles, 'ascending')
            zipcode_market_map.ppsfcolors.extend([zipcode, ppsf_color])

            zip_geojson_feature['properties'].mediansaleprice = number_to_string('dollar', median_sale_price)

            if dom:
                zip_geojson_feature['properties'].dom = int(dom)
            else:
                zip_geojson_feature['properties'].dom = None

            if ppsf:
                zip_geojson_feature['properties'].ppsf = number_to_string('dollar', ppsf)
            else:
                zip_geojson_feature['properties'].ppsf = None


            zip_geojson_feature['properties'] = zip_geojson_feature['properties'].__dict__

        zipcode_market_map.geojson = scopeout_market.geojson

        assign_legend_details(zipcode_market_map.mediansalepricelegend, median_sale_price_percentiles, 'dollar', 'ascending')
        assign_legend_details(zipcode_market_map.domlegend, dom_percentiles, 'number', 'ascending')
        assign_legend_details(zipcode_market_map.ppsflegend, ppsf_percentiles, 'dollar', 'ascending')

        zipcode_market_map.mediansalepricecolors.append(COLOR_LEVEL_NA)
        zipcode_market_map.domcolors.append(COLOR_LEVEL_NA)
        zipcode_market_map.ppsfcolors.append(COLOR_LEVEL_NA)



        zipcode_market_map.convert_to_dict()


        mongoclient.insert_list_mongo(list_data=[zipcode_market_map.__dict__],
                                      dbname='ScopeOutMaps',
                                      collection_name='ZipcodeMarketMaps',
                                      prod_env=ProductionEnvironment.MARKET_MAPS,
                                      collection_update_existing={"cbsacode": cbsacode})


def calculate_percentiles_from_all_tracts(tracts_data_df):
    median_household_income_list = []
    unemployment_rate_list = []
    owner_occupant_rate_list = []

    for i, tract in tracts_data_df.iterrows():
        tract_median_household_income = tract['economy']['medianhouseholdincome']['data'][0]
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

