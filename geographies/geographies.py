import os
import sys
from database import mongoclient
import pandas as pd
from enums import ProductionEnvironment
from database import mongoclient
import json
from shapely.geometry import Polygon, mapping
from utils.utils import create_url_slug

def dump_zipcode_spatial_by_scopeout_markets():
    currpath = os.path.dirname(os.path.abspath(__file__))
    rootpath = os.path.dirname(os.path.abspath(currpath))

    scopeout_markets = mongoclient.query_collection(database_name="ScopeOut",
                                                         collection_name="ScopeOutMarkets",
                                                         collection_filter={},
                                                         prod_env=ProductionEnvironment.GEO_ONLY)

    for i, cbsa in scopeout_markets.iterrows():
        zipcode_list = []
        cbsacode = cbsa['cbsacode']


        with open(rootpath + '/files/test_zip.json') as f:
            data = json.load(f)
            for zipcode_data in data['features']:
                geometry = []
                zipcode = zipcode_data['properties']['ZIP_CODE']

                top_level_coordinates = zipcode_data['geometry']['coordinates']

                for i, poly_tuple_list in enumerate(top_level_coordinates):

                    geo_list = []

                    parentPolygon = ""
                    for i2, poly_tuple_ll in enumerate(poly_tuple_list):
                        if parentPolygon == "":
                            parentPolygon = Polygon(poly_tuple_ll)
                            continue
                        else:
                            parentPolygon = parentPolygon.difference(Polygon(poly_tuple_ll))

                    poly_mapped = mapping(parentPolygon)
                    for i, poly_tuple_list in enumerate(poly_mapped['coordinates']):
                        for poly_tuple in poly_tuple_list:
                            geo_list.append({
                                "lng": poly_tuple[0],
                                "lat": poly_tuple[1],
                            })
                    geometry.append(geo_list)

                zipcode_list.append(
                    {
                        "zipcode": zipcode,
                        "geometry": geometry
                    }
                )

        cbsa_ziplist = {
            "cbsacode": cbsacode,
            "urlslug": create_url_slug(cbsacode, cbsa['cbsaname']),
            "zipprofiles": zipcode_list
        }

        mongoclient.insert_list_mongo(list_data=[cbsa_ziplist],
                             dbname='Geographies',
                             collection_name='ZipcodeSpatialData',
                              prod_env=ProductionEnvironment.GEO_ONLY,
                              collection_update_existing={"cbsacode": cbsacode})


def dump_testziplist():
    with open('./files/test_zip.json', 'r') as f:
        data = json.load(f)

        insert_list = []
        for zipcode_data in data['features']:
            geometry = []
            zipcode = zipcode_data['properties']['ZIP_CODE']
            insert_list.append({"zipcode": zipcode})


        mongoclient.insert_list_mongo(list_data=insert_list,
                                      dbname='Geographies',
                                      collection_name='TestZipList',
                                      prod_env=ProductionEnvironment.GEO_ONLY)

def dump_all_geographies():
    """
    Stores data to State, County, Cbsa tables
    """
    currpath = os.path.dirname(os.path.abspath(__file__))
    rootpath = os.path.dirname(os.path.abspath(currpath))

    state_dict = {}
    county_dict = {}

    for geo_level in ['state', 'county']:
        if geo_level == 'state' or geo_level == 'county':
            file_dir = '/files/statecountyfips.csv'
            geo_df = pd.read_csv(rootpath + file_dir)

            geo_df = geo_df.rename(columns={'State Code (FIPS)':'stateid',
                                            'County Code (FIPS)': 'countyid',
                                            'Area Name (including legal/statistical area description)':'areaname',
                                            'Abbreviation':'abbreviation'})

            geo_df['stateid'] = geo_df['stateid'].apply(lambda x: str(x).zfill(2))
            geo_df['countyid'] = geo_df['countyid'].apply(lambda x: str(x).zfill(3))

            geo_df = geo_df[geo_df['stateid'] != '72']

            for i, row in geo_df.iterrows():
                if geo_level == 'state' and row['Summary Level'] == 40:
                    state_dict[row['stateid']] = {
                        'fipsstatecode': row['stateid'],
                        'statename': row['areaname'],
                        'stateabbreviation': row['abbreviation'],
                    }
                elif geo_level == 'county' and row['Summary Level'] == 50:
                    countyfullcode = row['stateid'] + row['countyid']

                    county_dict[countyfullcode] = {
                        'countyfullcode': countyfullcode,
                        'fipscountycode': row['countyid'],
                        'countyname': row['areaname'],
                        'stateinfo': state_dict[row['stateid']],
                    }

    file_dir = '/files/cbsafips.csv'
    cbsa_df = pd.read_csv(rootpath + file_dir)

    cbsa_dict = {}

    for i, row in cbsa_df.iterrows():

        if row['cbsacode'] == row['cbsacode']:
            cbsacode = str(int(row['cbsacode']))
            stateid = str(int(row['fipsstatecode'])).zfill(2)

            if stateid == '72':
                continue

            fipscountycode = str(int(row['fipscountycode'])).zfill(3)
            countyfullcode = stateid + fipscountycode

            if countyfullcode in county_dict.keys():
                county_data = county_dict[countyfullcode]
            else:
                print('Skipping county: ', row['countycountyequivalent'])
                continue

            if cbsacode in cbsa_dict.keys():
                cbsa_dict[cbsacode]['counties'].append(county_data)
            else:
                cbsa_dict[cbsacode] = {
                    'cbsacode': cbsacode,
                    'cbsaname': row['cbsatitle'].replace('--','-'),
                    'counties': [county_data]
                }

    state_list = []
    county_list = []
    cbsa_list = []

    for _, v in state_dict.items():
        state_list.append(v)

    mongoclient.insert_list_mongo(list_data=state_list,
                                  dbname='Geographies',
                                  collection_name='State',
                                  prod_env=ProductionEnvironment.GEO_ONLY)

    for _, v in county_dict.items():
        county_list.append(v)

    mongoclient.insert_list_mongo(list_data=county_list,
                                  dbname='Geographies',
                                  collection_name='County',
                                  prod_env=ProductionEnvironment.GEO_ONLY)

    for _, v in cbsa_dict.items():
        cbsa_list.append(v)

    mongoclient.insert_list_mongo(list_data=cbsa_list,
                                  dbname='Geographies',
                                  collection_name='Cbsa',
                                  prod_env=ProductionEnvironment.GEO_ONLY)



    print('done')

def dump_county_by_cbsa():
    '''
    Stores data to CountyByCbsa table. Function creates lookup for counties to cbsa ids
    '''
    cbsa_data = mongoclient.query_collection(database_name="Geographies",
                                 collection_name="Cbsa",
                                 collection_filter={},
                                 prod_env=ProductionEnvironment.GEO_ONLY)

    counties_to_cbsa = []
    for i, cbsa in cbsa_data.iterrows():
        cbsaid = cbsa['cbsacode']
        cbsaname = cbsa['cbsaname']
        for county in cbsa['counties']:
            stateid = county['stateinfo']['fipsstatecode']
            counties_to_cbsa.append({
                'countyfullcode': county['countyfullcode'],
                'cbsacode': cbsaid,
                'cbsaname': cbsaname,
                'stateid': stateid
            })

    mongoclient.insert_list_mongo(list_data=counties_to_cbsa,
                      dbname='Geographies',
                      collection_name='CountyByCbsa2',
                      prod_env=ProductionEnvironment.GEO_ONLY)

def dump_zillow_cbsa_mapping():
    '''
    Function dumps mappings for zillow metro ids to cbsa ids
    :return:
    '''
    currpath = os.path.dirname(os.path.abspath(__file__))
    rootpath = os.path.dirname(os.path.abspath(currpath))

    final_df = pd.DataFrame()

    # get this csv from google sheet
    file_dir = '/files/Zillow CBSA ID Map.csv'
    df = pd.read_csv(rootpath + file_dir)
    df = df[df['cbsamissinginzillow'] != 'Yes']
    df[['zillowmsaid','cbsacode']] = df[['zillowmsaid','cbsacode']].fillna(0).astype(int)

    zillow_cbsa_mapping_list = []

    length_zillow = len(df['zillowmsaid'])
    length_zillow_unique = len(df['zillowmsaid'].drop_duplicates())
    length_cbsa = len(df['cbsacode'])
    length_cbsa_unique = len(df['cbsacode'].drop_duplicates())

    if length_zillow != length_zillow_unique and length_cbsa != length_cbsa_unique:
        print('Why is there a mismatch in number ')
        sys.exit()

    for i, row in df.iterrows():
        zillow_cbsa_mapping_list.append({
            'zillowmsaid': row.zillowmsaid,
            'zillowmsaname': row.zillowmetroname,
            'cbsacode': row.cbsacode,
            'cbsaname': row.cbsaname
        })


    mongoclient.insert_list_mongo(list_data=zillow_cbsa_mapping_list,
                                  dbname='Geographies',
                                  collection_name='ZillowCbsaMapping',
                                  prod_env=ProductionEnvironment.GEO_ONLY)

# Get zip files from: https://www.huduser.gov/portal/datasets/usps_crosswalk.html
def DEPRECATED_dump_zipcode():
    '''
    Function stores all USPS zipcodes from various years.
    :return:
    '''
    currpath = os.path.dirname(os.path.abspath(__file__))
    rootpath = os.path.dirname(os.path.abspath(currpath))

    final_df = pd.DataFrame()

    ZIP_FILE_YEARS = ['10','15','19','20']
    for year in ZIP_FILE_YEARS:
        file_dir = '/files/zipcodes/ZIP_COUNTY_1220{}.xlsx'.format(year)
        print('Reading zip file: {}'.format(file_dir))
        df = pd.read_excel(rootpath + file_dir)

        df = df.rename(columns={'ZIP':'zipcode','COUNTY':'countyfullcode'})
        df['zipcode'] = df['zipcode'].apply(lambda x: str(x).zfill(5))
        df['countyfullcode'] = df['countyfullcode'].apply(lambda x: str(x).zfill(5))
        df['stateid'] = df['countyfullcode'].str[:2]
        df = df[df['stateid'].isin(['60','66','69','72','78']) == False ]
        df = df[['zipcode','countyfullcode']]
        df['zipcountyid'] = df['zipcode'] + df['countyfullcode']

        if len(final_df) < 1:
            final_df = df
        else:
            final_zipcode_list = list(final_df['zipcountyid'])
            for i, row in df.iterrows():
              current_zipcode = row.zipcode
              current_countyfullcode = row.countyfullcode
              zipcountyid = current_zipcode + current_countyfullcode

              if zipcountyid not in final_zipcode_list:
                  final_df = final_df.append({'zipcountyid': zipcountyid,
                                              'zipcode':current_zipcode,
                                              'countyfullcode':current_countyfullcode},
                                              ignore_index=True)

    counties_to_cbsa = mongoclient.query_collection(database_name="Geographies",
                                                    collection_name="CountyByCbsa",
                                                    collection_filter={},
                                                    prod_env=ProductionEnvironment.GEO_ONLY)

    insert_list = []
    for i, zipcounty in final_df.iterrows():
        match = counties_to_cbsa[counties_to_cbsa['countyfullcode'] == zipcounty.countyfullcode]

        if len(match) > 1:
            print('found multiple matches: countyfullcode: {}'.format(zipcounty.countyfullcode))
        elif len(match) > 0:
            insert_list.append({
                'zipcode': zipcounty.zipcode,
                'countyfullcode': zipcounty.countyfullcode,
                'cbsacode': match.cbsacode.iloc[0]
            })
        else:
            insert_list.append({
                'zipcode': zipcounty.zipcode,
                'countyfullcode': zipcounty.countyfullcode
            })



    mongoclient.insert_list_mongo(list_data=insert_list,
                                  dbname='Geographies',
                                  collection_name='ZipCountyCbsa',
                                  prod_env=ProductionEnvironment.GEO_ONLY)