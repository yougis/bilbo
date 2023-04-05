import os.path

import ee
import geemap as gee
from gee_raster import extract

# instanciation de ee
service_account = 'ee-oeil@surfor.iam.gserviceaccount.com'
credentials = ee.ServiceAccountCredentials(service_account, '../gee_raster/surfor-8383e43c3aa7.json')
ee.Initialize(credentials)

# création de l'input : geodataframe avec N polygone et N points
zone1 = ee.Geometry.Polygon(
    [[[166.40, -22.22], [166.40, -22.15], [166.42, -22.13], [166.45, -22.15], [166.45, -22.22]]])
zone2 = ee.Geometry.Polygon(
    [[[166.50, -22.10], [166.50, -22.15], [166.52, -22.12], [166.54, -22.15], [166.54, -22.10]]])
features = [
    ee.Feature(zone1, {'name': 'zone1'}),
    ee.Feature(zone2, {'name': 'zone2'}),
]
gdf = gee.ee_to_geopandas(ee.FeatureCollection(features))
file_path = 'input_spec_example.yml'
output_gdf = extract.extract_gee_data(file_path, gdf)
print(output_gdf)
