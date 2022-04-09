import sys
from database import mongoclient
from models import marketmap
from models import tractmarketmaps, zipcodemarketmap
from models import geojson as modelGeoJson
from enums import ProductionEnvironment
from utils.utils import list_length_okay, create_url_slug, calculate_percentiles_from_list, number_to_string, assign_color, COLOR_LEVEL_NA, assign_legend_details
import numpy as np

def create_short_zipcode_profiles():
    zip_code_data = mongoclient.query_collection(database_name="MarketTrends",
                                                 collection_name="redfinzipcodedata",
                                                 collection_filter={},
                                                 prod_env=ProductionEnvironment.MARKET_TRENDS)

    zipcodes_by_scopeout_markets = mongoclient.query_collection(database_name="ScopeOut",
                                                    collection_name="EsriZipcodesBySOMarkets",
                                                    collection_filter={},
                                                    prod_env=ProductionEnvironment.GEO_ONLY)

    for i, row in zipcodes_by_scopeout_markets.iterrows():
        cbsacode = row.cbsacode

        cbsa_market = mongoclient.query_collection(database_name="MarketTrends",
                                                    collection_name="cbsamarketprofiles",
                                                    collection_filter={"cbsacode":cbsacode},
                                                    prod_env=ProductionEnvironment.MARKET_TRENDS)

        cbsa_market = mongoclient.query_cocllection(database_name="MarketTrends",
                                                   collection_name="cbsamarketprofiles",
                                                   collection_filter={"cbsacode":cbsacode},
                                                   prod_env=ProductionEnvironment.MARKET_TRENDS)
        print('')