import os
import geopandas as gpd
import geemap
import math
import affine
from rasterstats import zonal_stats
import ee
import numpy as np

_ee_crs = "EPSG:4326"
_band_name_key = 'confRaster.bandName'
_mask_key = 'confRaster.masque'
_output_value_key = 'confRaster.outputValue'
_default_value_key = 'confRaster.defaultValue'
_uri_image_key = 'confRaster.uri_image'
_spec_err = "Le champ '{field}' n'est pas défini dans les specifications"
_service_account = 'ee-oeil@surfor.iam.gserviceaccount.com'
_credentials = ee.ServiceAccountCredentials(_service_account, os.path.dirname(__file__) + '/gee_credentials.json')
ee.Initialize(_credentials)


def extract_data(specs: dict, input_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Cette méthode permet de récupérer les données d'un catalogue Google Earth Engine pour des géométries données, soit des polygones, soit des points.
    :param specs: Contient toutes les spécifications, certaines valeurs sont obligatoires : bandName, uri_image, outputValue, defaultValue.
    :type specs: dict
    :param input_gdf: Contient l'ensemble des géométries à étudier, soit des polygones, soit des points. L'attribut CRS doit être spécifié.
    :type input_gdf: gpd.GeoDataFrame
    :returns: GeoDataFrame fourni en entrée avec des nouvelles propriétés, soit mini_raster_array, mini_raster_affine, min and max pour les polygones, soit la valeur de la bande spécifiée pour les points.
    :rtype: gpd.GeoDataFrame
    """


    # modifier le CRS du gdf si ce n'est pas celui de Google Earth Engine (EPSG:4326)
    if input_gdf.crs is None:
        raise ValueError("Le CRS n'est pas défini pour la donnée d'entrée.")
    elif input_gdf.crs != _ee_crs:
        print("Le CRS de la donnée d'entrée n'est pas {ee_crs}, conversion en cours...".format(ee_crs=_ee_crs))
        input_gdf = input_gdf.to_crs(_ee_crs)

    # vérification du contenu de l'entrée
    _gee_extractor = _GeeExtractor(specs, input_gdf)
    geom_type = _checkGeomType(input_gdf)
    if input_gdf.shape[0] == 0 or len(input_gdf) == 0 or len(input_gdf.geom_type) == 0 or geom_type is None:
        raise ValueError("Aucune donnée à traiter, vérifiez la valeur de l'input")
    elif geom_type == 'Polygon':
        return _gee_extractor.extract_data_polygon()
    elif geom_type == 'Point':
        return _gee_extractor.extract_data_point()
    else:
        raise ValueError(
            "Les géométries peuvent seulement être de type 'Polygon' ou 'Point' mais pas '{geom_type}'".format(
                geom_type=geom_type))


class _GeeExtractor:
    """Classe permettant d'extraire les données de Google Earth Engine, soit à partir de polygones, soit de points."""

    def __init__(self, specs: dict, input_gdf: gpd.GeoDataFrame):
        self.specs = specs
        self.input_gdf = input_gdf

    def extract_data_polygon(self) -> gpd.GeoDataFrame:
        """Génère le raster array à partir du catalogue GEE pour chaque polygone fourni en entrée."""
        # récupération des specs
        uri_image = self._get_spec_value(_uri_image_key)
        band_name = self._get_spec_value(_band_name_key)

        # création de l'image de référence
        image = ee.Image(uri_image).select(band_name)

        # récupération des specs utiles
        output_value = self._get_spec_value(_output_value_key)
        default_value = self._get_spec_value(_default_value_key)
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
            sample_gdf = gpd.GeoDataFrame.from_features([feature], crs=_ee_crs)
            xmin, _, _, ymax = sample_gdf.total_bounds
            transform = affine.Affine.from_gdal(_round_down(xmin, scale), scale, 0, _round_up(ymax, scale), 0, -scale)

            # récupération des stats de la zone
            feature_with_stats = zonal_stats(sample_gdf, image_clipped,
                                                stats=['min', 'max','nodata'],
                                                affine=transform, nodata=default_value, geojson_out=True,
                                                raster_out=True,
                                                all_touched=True)
            if len(feature_with_stats) > 0:
                output_features.append(feature_with_stats[0])
        return gpd.GeoDataFrame.from_features(output_features, crs=_ee_crs)

    def extract_data_point(self) -> gpd.GeoDataFrame:
        """Récupère la valeur du pixel à partir du catalogue GEE pour chaque point fourni en entrée"""
        # récupération des specs
        uri_image = self._get_spec_value(_uri_image_key)
        band_name = self._get_spec_value(_band_name_key)

        # création de l'image de référence
        image = ee.Image(uri_image).select(band_name)

        # récupération des valeurs pour chaque point
        feature_collection = geemap.geopandas_to_ee(self.input_gdf)
        output_features = image.sampleRegions(feature_collection)
        return gpd.GeoDataFrame.from_features(output_features.getInfo(), crs=_ee_crs)

    def _get_spec_value(self, spec_key: str, raise_err_if_not_found=True):
        """Récupère la valeur d'une spécification depuis le dictionnaire fourni en entrée. 
        La clé peut contenir des '.' pour les sous dictionnaires, exemple : 'confRaster.uri_image'.
        Une exception est levée si la clé n'est pas trouvée dans le dictionnaire et si raise_err_if_not_found est à True
         (défaut : True)."""
        keys = spec_key.split('.')
        spec_value = self.specs
        for key in keys:
            spec_value = spec_value.get(key)
            if spec_value is None and raise_err_if_not_found:
                raise ValueError(_spec_err.format(field=key))
        return spec_value

    def _apply_mask_on_image(self, output_value: str, image: ee.Image) -> ee.Image:
        """Applique un masque à l'image GEE à partir des spécifications."""
        mask_ranges = self._get_spec_value(_mask_key, raise_err_if_not_found=False)
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
            min = mask_ranges[0]
            max = mask_ranges[len(mask_ranges) - 1]
            # on filtre pour retirer les valeurs inférieures au début du range et les valeurs supérieures à la fin du range
            print("Les valeurs inférieures ou égales à {min} ou supérieures à {max} ne seront pas considérées.".format(min=min, max=max))
            image = image.updateMask(image.gte(min).And(image.lt(max)))
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

def _checkGeomType(gdf): 
    points = gdf.loc[(gdf.geom_type =='Point') | (gdf.geom_type =='MultiPoint' )]
    polygons = gdf.loc[(gdf.geom_type =='Polygon') | (gdf.geom_type =='MultiPolygon')] 
    lines = gdf.loc[(gdf.geom_type =='LineString') | (gdf.geom_type =='MultiLineString')] 
    if points.shape[0]>0: 
        print("Geometry type : Point or MultiPoint") 
        return 'Point' 
    if polygons.shape[0]>0: 
        print("Geometry type : Polygon or MultiPolygon") 
        return 'Polygon' 
    if lines.shape[0]>0: 
        print("Geometry type : Line or MultiLineString") 
        return 'Line' 
    print("Geom type UNKNOWN")