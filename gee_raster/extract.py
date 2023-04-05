import os

import yaml
from yaml.loader import SafeLoader
import geopandas as gpd
import geemap.foliumap as geemap
import geemap as gee
import math
import affine
import rasterstats as rs
import ee


def extract_gee_data(specs: dict, input_gdf):
    service_account = 'ee-oeil@surfor.iam.gserviceaccount.com'
    credentials = ee.ServiceAccountCredentials(service_account,  os.path.dirname(__file__) + '/surfor-8383e43c3aa7.json')
    ee.Initialize(credentials)

    # vérification du contenu de l'entrée
    if len(input_gdf) == 0:
        raise 'Aucune donnée à traiter'
    elif input_gdf.geom_type[0] == 'Polygon':
        return _extract_gee_data_polygon(specs, input_gdf)
    elif input_gdf.geom_type[0] == 'Point':
        return _extract_gee_data_point(specs, input_gdf)
    else:
        raise 'Les géométries peuvent seulement être de type Polygon ou Point mais pas {geom_type}'.format(
            geom_type=input_gdf.geom_type[0])


def _extract_gee_data_polygon(specs: dict, input_gdf):
    # image de référence
    image = ee.Image(specs['confRaster']['uri_image']).select(specs['keepList'])
    scale = image.getInfo()['bands'][0]['crs_transform'][0]
    default_value = specs['confRaster']['defaultValue']
    projection = specs['epsg']

    # indiquer le crs si pas déjà fait
    if input_gdf.crs is None:
        input_gdf = input_gdf.set_crs(crs=projection)

    output_features = []
    # traitement sur les polygones un à un pour éviter de dépasser la limite de GEE de 262144 pixels
    for feature in input_gdf.iterfeatures():
        # récupération du raster correspondant au polygon
        image_clipped = gee.ee_to_numpy(image, region=ee.FeatureCollection([feature]), default_value=default_value)
        image_clipped_reshaped = None
        if image_clipped is not None:
            image_clipped_reshaped = image_clipped.reshape(image_clipped.shape[0],
                                                           (image_clipped.shape[1] * image_clipped.shape[2]))

        # récupération de l'affine
        sample_gdf = gpd.GeoDataFrame(feature)
        xmin, _, _, ymax = sample_gdf.total_bounds
        transform = affine.Affine.from_gdal(_round_down(xmin, scale), scale, 0, _round_up(ymax, scale), 0, -scale)

        # récupération des stats de la zone
        if image_clipped_reshaped is not None:
            stats = rs.zonal_stats(sample_gdf, image_clipped_reshaped, stats=['min', 'max', 'mean', 'median', 'std'],
                                   affine=transform, nodata=default_value, geojson_out=True, raster_out=True,
                                   all_touched=True)
            output_features.append(stats[0])
    return gpd.GeoDataFrame.from_features(output_features, crs=projection)


def _extract_gee_data_point(specs: dict, input_gdf):
    pass


def _round_up(x, increment):
    return math.ceil(x / increment) * increment


def _round_down(x, increment):
    return math.floor(x / increment) * increment
