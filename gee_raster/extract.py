import os
import geopandas as gpd
import geemap as gee
import math
import affine
import rasterstats as rs
import ee
import numpy as np

ee_crs = "EPSG:4326"
band_name_key = 'confRaster.bandName'
mask_key = 'confRaster.masque'
output_value_key = 'confRaster.outputValue'
default_value_key = 'confRaster.defaultValue'
uri_image_key = 'confRaster.uri_image'
spec_err = "Le champ '{field}' n'est pas défini dans les specifications"


def extract_gee_data(specs: dict, input_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    service_account = 'ee-oeil@surfor.iam.gserviceaccount.com'
    credentials = ee.ServiceAccountCredentials(service_account, os.path.dirname(__file__) + '/surfor-8383e43c3aa7.json')
    ee.Initialize(credentials)

    # modifier le CRS du gdf si ce n'est pas celui de Google Earth Engine (EPSG:4326)
    if input_gdf.crs is None:
        raise ValueError("Le CRS n'est pas défini pour la donnée d'entrée.")
    elif input_gdf.crs != ee_crs:
        print("Le CRS de la donnée d'entrée n'est pas {ee_crs}, conversion en cours...".format(ee_crs=ee_crs))
        input_gdf = input_gdf.to_crs(ee_crs)

    # vérification du contenu de l'entrée
    _gee_extractor = GeeExtractor(specs, input_gdf)
    if len(input_gdf) == 0:
        raise ValueError("Aucune donnée à traiter, vérifiez la valeur de l'input")
    elif input_gdf.geom_type[0] == 'Polygon':
        return _gee_extractor.extract_gee_data_polygon()
    elif input_gdf.geom_type[0] == 'Point':
        return _gee_extractor.extract_gee_data_point()
    else:
        raise ValueError(
            "Les géométries peuvent seulement être de type 'Polygon' ou 'Point' mais pas '{geom_type}'".format(
                geom_type=input_gdf.geom_type[0]))


class GeeExtractor:
    """Class permettant d'extraire les données de Google Earth Engine"""

    def __init__(self, specs: dict, input_gdf: gpd.GeoDataFrame):
        self.specs = specs
        self.input_gdf = input_gdf

    def extract_gee_data_polygon(self) -> gpd.GeoDataFrame:
        # récupération des specs
        uri_image = self._get_spec_value(uri_image_key)
        band_name = self._get_spec_value(band_name_key)

        # création de l'image de référence
        image = ee.Image(uri_image).select(band_name)

        # récupération des specs utiles
        output_value = self._get_spec_value(output_value_key)
        default_value = self._get_spec_value(default_value_key)
        image_info = image.getInfo()
        bands = image_info.get('bands')
        if bands is None or len(bands) == 0:
            raise ValueError("Impossible de récupérer la liste des bandes à partir de l'image EE : {ee_image}".format(
                ee_image=image_info))
        else:
            band = bands[0]
            crs_transform = band.get('crs_transform')
            if crs_transform is None or len(crs_transform) == 0:
                raise ValueError(
                    "Impossible de récupérer la liste des transformations CRS à partir de la bande : {band}"
                    .format(band=band))
            scale = crs_transform[0]

        # application des masques
        image = self._apply_mask_on_image(output_value, image)

        nb_input_features = len(self.input_gdf.index)
        output_features = []
        # traitement sur les polygones un à un pour éviter de dépasser la limite de GEE de 262144 pixels
        for count, feature in enumerate(self.input_gdf.iterfeatures()):
            print("Analyse de la feature n°{nb} / {total}".format(nb=count + 1, total=nb_input_features))
            # récupération du raster correspondant au polygon
            sample = image.sampleRectangle(region=feature.get('geometry'), defaultValue=default_value)
            sample_first_band = sample.get(band_name)
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

    def extract_gee_data_point(self) -> gpd.GeoDataFrame:
        # récupération des specs
        uri_image = self._get_spec_value(uri_image_key)
        band_name = self._get_spec_value(band_name_key)

        # création de l'image de référence
        image = ee.Image(uri_image).select(band_name)

        # récupération des valeurs pour chaque point
        feature_collection = gee.geopandas_to_ee(self.input_gdf)
        output_features = image.sampleRegions(feature_collection)
        return gpd.GeoDataFrame.from_features(output_features.getInfo())

    def _get_spec_value(self, spec_key: str, raise_err_if_not_found=True):
        keys = spec_key.split('.')
        spec_value = self.specs
        for key in keys:
            spec_value = spec_value.get(key)
            if spec_value is None and raise_err_if_not_found:
                raise ValueError(spec_err.format(field=key))
        return spec_value

    def _apply_mask_on_image(self, output_value: str, image: ee.Image) -> ee.Image:
        mask_ranges = self._get_spec_value(mask_key, raise_err_if_not_found=False)
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
            print("Classification des données selon les intervals suivants : {clauses}".format(
                clauses=' | '.join(clauses)))
            return image
        else:
            raise ValueError("Le type de données de sortie n'est pas correcte, devrait être 'continue' ou 'classifie' "
                             "mais pas '{output_value}'".format(output_value=output_value))


def _round_up(x, increment):
    return math.ceil(x / increment) * increment


def _round_down(x, increment):
    return math.floor(x / increment) * increment
