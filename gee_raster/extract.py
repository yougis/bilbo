import os
import geopandas as gpd
import geemap as gee
import math
import affine
import rasterstats as rs
import ee
import numpy as np

ee_crs = "EPSG:4326"


def extract_gee_data(specs: dict, input_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    service_account = 'ee-oeil@surfor.iam.gserviceaccount.com'
    credentials = ee.ServiceAccountCredentials(service_account, os.path.dirname(__file__) + '/surfor-8383e43c3aa7.json')
    ee.Initialize(credentials)

    # modifier le CRS du gdf si ce n'est pas celui de Google Earth Engine (EPSG:4326)
    if input_gdf.crs is not None and input_gdf.crs != ee_crs:
        print("Le CRS de la donnée d'entrée n'est pas {ee_crs}, conversion en cours...".format(ee_crs=ee_crs))
        input_gdf = input_gdf.to_crs(ee_crs)

    # récupération de l'image de référence
    uri_image = specs['confRaster']['uri_image']
    bands = specs['keepList']
    if len(bands) != 1:
        raise ValueError("Pour l'instant l'attribut keepList ne peut contenir qu'un seul élément mais en contient {nb}"
                         "".format(nb=len(bands)))

    # création de l'image de référence
    image = ee.Image(uri_image).select(bands)

    # vérification du contenu de l'entrée
    if len(input_gdf) == 0:
        raise 'Aucune donnée à traiter'
    elif input_gdf.geom_type[0] == 'Polygon':
        return _extract_gee_data_polygon(specs, input_gdf, image)
    elif input_gdf.geom_type[0] == 'Point':
        return _extract_gee_data_point(specs, input_gdf, image)
    else:
        raise ValueError(
            "Les géométries peuvent seulement être de type 'Polygon' ou 'Point' mais pas '{geom_type}'".format(
                geom_type=input_gdf.geom_type[0]))


def _extract_gee_data_polygon(specs: dict, input_gdf: gpd.GeoDataFrame, image: ee.Image) -> gpd.GeoDataFrame:
    # récupération des specs utiles
    mask_ranges = specs['confRaster']['masque']
    output_value = specs['confRaster']['outputValue']
    bands = specs['keepList']
    default_value = specs['confRaster']['defaultValue']
    scale = image.getInfo()['bands'][0]['crs_transform'][0]

    # application des masques
    image = _apply_mask_on_image(mask_ranges, output_value, image)

    nb_input_features = len(input_gdf.index)
    output_features = []
    # traitement sur les polygones un à un pour éviter de dépasser la limite de GEE de 262144 pixels
    for count, feature in enumerate(input_gdf.iterfeatures()):
        print("Analyse de la feature n°{nb} / {total}".format(nb=count + 1, total=nb_input_features))
        # récupération du raster correspondant au polygon
        sample = image.sampleRectangle(region=feature.get('geometry'), defaultValue=default_value)
        sample_first_band = sample.get(bands[0])
        image_clipped = np.array(sample_first_band.getInfo())

        # récupération de l'affine
        sample_gdf = gpd.GeoDataFrame.from_features([feature])
        xmin, _, _, ymax = sample_gdf.total_bounds
        transform = affine.Affine.from_gdal(_round_down(xmin, scale), scale, 0, _round_up(ymax, scale), 0, -scale)

        # récupération des stats de la zone
        feature_with_stats = rs.zonal_stats(sample_gdf, image_clipped,
                                            stats=['min', 'max', 'mean', 'median', 'std'],
                                            affine=transform, nodata=default_value, geojson_out=True,
                                            raster_out=True,
                                            all_touched=True)
        if len(feature_with_stats) > 0:
            output_features.append(feature_with_stats[0])
    return gpd.GeoDataFrame.from_features(output_features)


def _apply_mask_on_image(mask_ranges: list, output_value: str, image: ee.Image) -> ee.Image:
    if mask_ranges is None:
        return image
    if output_value == 'continue':
        if len(mask_ranges) != 2:
            raise ValueError("Le masque appliqué devrait contenir uniquement 2 valeurs, le min (inclut) et le max ("
                             "exclut) de la plage de valeur")
        min = mask_ranges[0]
        max = mask_ranges[1]
        print("Utilisation d'un filtre sur les données [{min},{max}[".format(min=min, max=max))
        return image.updateMask(image.gte(min).And(image.lt(max)))
    elif output_value == 'classifie':
        if len(mask_ranges) < 2:
            raise ValueError("Le masque appliqué devrait contenir au moins 2 valeurs, mais en contient {nb}".format(
                nb=len(mask_ranges)))
        original_image = image
        clauses = []
        for idx, value in enumerate(mask_ranges):
            if idx == 0:
                continue
            left_limit = mask_ranges[idx - 1]
            right_limit = value
            if idx == len(mask_ranges) - 1:  # dans le cas de la dernière valeur, on l'inclut dans l'interval
                image = image.where(original_image.gte(left_limit).And(original_image.lte(right_limit)), idx)
                clauses.append('[{left},{right}] = {value}'.format(left=left_limit, right=right_limit, value=idx))
            else:
                image = image.where(original_image.gte(left_limit).And(original_image.lt(right_limit)), idx)
                clauses.append('[{left},{right}[ = {value}'.format(left=left_limit, right=right_limit, value=idx))
        print("Classification des données selon les intervals suivants : {clauses}".format(clauses=' | '.join(clauses)))
        return image
    else:
        raise ValueError("Le type de données de sortie n'est pas correcte, devrait être 'continue' ou 'classifie' "
                         "mais pas '{output_value}'".format(output_value=output_value))


def _extract_gee_data_point(specs: dict, input_gdf: gpd.GeoDataFrame, image: ee.Image) -> gpd.GeoDataFrame:
    # récupération des specs utils
    projection = specs['epsg']

    # récupération des valeurs pour chaque point
    feature_collection = gee.geopandas_to_ee(input_gdf.set_crs(crs=projection))
    output_features = image.sampleRegions(feature_collection)
    return gpd.GeoDataFrame.from_features(output_features.getInfo(), crs=projection)


def _round_up(x, increment):
    return math.ceil(x / increment) * increment


def _round_down(x, increment):
    return math.floor(x / increment) * increment
