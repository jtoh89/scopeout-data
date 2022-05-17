import geopandas# import shapefile
import shapely.geometry

# Get shapefiles from https://www2.census.gov/geo/tiger/TIGER2020/TRACT/

df = geopandas.read_file('tractsgeojson/tl_2020_06_tract/tl_2020_06_tract.shp')
df = df.to_crs("EPSG:4326")

for i, row in df.iterrows():
    if row['GEOID'] == '06059075516':
        shapely_polygon = row.geometry
        test = geopandas.GeoSeries([shapely_polygon]).__geo_interface__
        print("")

    if row['GEOID'] == '06059075515':
        print("")

print("done")



