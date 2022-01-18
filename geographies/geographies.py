import os
import sys
from database import mongoclient
import pandas as pd
from enums import ProductionEnvironment


def dump_all_geographies():
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

    # mongoclient.insert_list_mongo(list_data=state_list,
    #                               dbname='Geographies',
    #                               collection_name='State',
    #                               prod_env=ProductionEnvironment.GEO_ONLY)

    for _, v in county_dict.items():
        county_list.append(v)

    # mongoclient.insert_list_mongo(list_data=county_list,
    #                               dbname='Geographies',
    #                               collection_name='County',
    #                               prod_env=ProductionEnvironment.GEO_ONLY)

    for _, v in cbsa_dict.items():
        cbsa_list.append(v)

    mongoclient.insert_list_mongo(list_data=cbsa_list,
                                  dbname='Geographies',
                                  collection_name='Cbsa',
                                  prod_env=ProductionEnvironment.GEO_ONLY)



    print('done')


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
                                  collection_name='Zillow_Cbsa_Mapping',
                                  prod_env=ProductionEnvironment.GEO_ONLY)



ZIP_FILE_YEARS = ['10','15','19','20']
# ZIP_FILE_YEARS = ['20']

# Get zip files from: https://www.huduser.gov/portal/datasets/usps_crosswalk.html
def dump_zipcode():
    '''
    Function stores all USPS zipcodes from various years.
    :return:
    '''
    currpath = os.path.dirname(os.path.abspath(__file__))
    rootpath = os.path.dirname(os.path.abspath(currpath))

    final_df = pd.DataFrame()

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
                                                    collection_name="CountyToCbsa",
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