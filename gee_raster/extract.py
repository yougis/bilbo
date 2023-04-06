import os
import geopandas as gpd
import geemap as gee
import math
import affine
import rasterstats as rs
import ee
import numpy as np


def extract_gee_data(specs: dict, input_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    service_account = 'ee-oeil@surfor.iam.gserviceaccount.com'
    credentials = ee.ServiceAccountCredentials(service_account, os.path.dirname(__file__) + '/surfor-8383e43c3aa7.json')
    ee.Initialize(credentials)

    # vérification du contenu de l'entrée
    if len(input_gdf) == 0:
        raise 'Aucune donnée à traiter'
    elif input_gdf.geom_type[0] == 'Polygon':
        return _extract_gee_data_polygon(specs, input_gdf)
    elif input_gdf.geom_type[0] == 'Point':
        return _extract_gee_data_point(specs, input_gdf)
    else:
        raise ValueError(
            "Les géométries peuvent seulement être de type 'Polygon' ou 'Point' mais pas '{geom_type}'".format(
                geom_type=input_gdf.geom_type[0]))


def _extract_gee_data_polygon(specs: dict, input_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    # récupération des specs utiles
    masques = specs['confRaster']['masque']
    output_value = specs['confRaster']['outputValue']
    uri_image = specs['confRaster']['uri_image']
    bands = specs['keepList']
    if len(bands) != 1:
        raise ValueError("Pour l'instant l'attribut keepList ne peut contenir qu'un seul élément mais en contient {nb}"
                         "".format(nb=len(bands)))
    default_value = specs['confRaster']['defaultValue']
    projection = specs['epsg']

    # création de l'image de référence
    image = ee.Image(uri_image).select(bands)
    scale = image.getInfo()['bands'][0]['crs_transform'][0]

    # application des masques
    image = _apply_mask_on_image(masques, output_value, image)

    # indiquer le crs si pas déjà fait
    if input_gdf.crs is None:
        input_gdf = input_gdf.set_crs(crs=projection)

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
    return gpd.GeoDataFrame.from_features(output_features, crs=projection)


def _apply_mask_on_image(masques: list, output_value: str, image: ee.Image) -> ee.Image:
    if masques is None:
        return image
    if output_value == 'continue':
        if len(masques) > 0:
            masque_type = list(masques[0].keys())[0]
            masque_value = list(masques[0].values())[0]
            if masque_type == 'gt':
                return image.updateMask(image.gt(masque_value))
            elif masque_type == 'gte':
                return image.updateMask(image.gte(masque_value))
            elif masque_type == 'lt':
                return image.updateMask(image.lt(masque_value))
            elif masque_type == 'lte':
                return image.updateMask(image.lte(masque_value))
            elif masque_type == 'eq':
                return image.updateMask(image.eq(masque_value))
            elif masque_type == 'neq':
                return image.updateMask(image.neq(masque_value))
            else:
                raise ValueError(
                    "Le type de masque n'est pas correcte, devrait être 'gt', 'gte', eq', 'neq', 'lt ou 'lte' mais "
                    "pas '{masque_type}'".format(
                        masque_type=masque_type))
        else:
            return image
    elif output_value == 'classifie':
        original_image = image
        for masque_idx, masque in enumerate(masques):
            if len(masque.keys()) != 1 or len(masque.values()) != 1:
                raise ValueError(
                    "Le masque '{masque}' n'est pas correctement configuré, devrait être sous le format 'gt: 30'".format(
                        masque=masque))
            masque_type = list(masque.keys())[0]
            masque_value = list(masque.values())[0]
            if masque_type == 'gt':
                if masque_idx == 0:
                    image = original_image.gt(masque_value)
                else:
                    image = image.add(original_image.gt(masque_value))
            elif masque_type == 'gte':
                if masque_idx == 0:
                    image = original_image.gte(masque_value)
                else:
                    image = image.add(original_image.gte(masque_value))
            elif masque_type == 'lt':
                if masque_idx == 0:
                    image = original_image.lt(masque_value)
                else:
                    image = image.add(original_image.lt(masque_value))
            elif masque_type == 'lte':
                if masque_idx == 0:
                    image = original_image.lte(masque_value)
                else:
                    image = image.add(original_image.lte(masque_value))
            elif masque_type == 'eq':
                if masque_idx == 0:
                    image = original_image.eq(masque_value)
                else:
                    image = image.add(original_image.eq(masque_value))
            elif masque_type == 'neq':
                if masque_idx == 0:
                    image = original_image.neq(masque_value)
                else:
                    image = image.add(original_image.neq(masque_value))
            else:
                raise ValueError(
                    "Le type de masque n'est pas correcte, devrait être 'gt', 'gte', eq', 'neq', 'lt ou 'lte' mais "
                    "pas '{masque_type}'".format(
                        masque_type=masque_type))
        return image
    else:
        raise ValueError("Le type de données de sortie n'est pas correcte, devrait être 'continue' ou 'classifie' "
                         "mais pas '{output_value}'".format(output_value=output_value))


def _extract_gee_data_point(specs: dict, input_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    pass


def _round_up(x, increment):
    return math.ceil(x / increment) * increment


def _round_down(x, increment):
    return math.floor(x / increment) * increment
