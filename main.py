from census import censusdata, censusgroupslookups
from database import mongoclient

states = [
    '01','02','04','05','06','08','09','10','11','12','13','15','16','17','18','19','20','21','22','23','24',
    '25','26','27','28','29','30','31','32','33','34','35','36','37','38','39','40','41','42','44','45','46',
    '47','48','49','50','51','53','54','55','56'
]

lookups = censusgroupslookups.get_census_lookup()

all_categories = lookups['Category'].drop_duplicates()

for stateid in states:
    print('Starting import for stateid: ', stateid)
    for i, category in all_categories.items():
        variables_df = lookups[lookups['Category'] == category]

        censusdata.get_census_data(geo_level='tracts', state_id=stateid, variables_df=variables_df)






