from geopandas import GeoDataFrame
from shapely.geometry import Polygon,MultiPolygon
from pandas import  concat as pd_concat
from dask.dataframe import concat as dd_concat
import numpy as np

import logging
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%d/%m/%Y %H:%M:%S', level=logging.DEBUG)

logging.info("Utils - Geometry Imported")


def checkGeomType(gdf: GeoDataFrame) -> str:
    """
    Check the geometry type of a GeoDataFrame and return the corresponding type.

    Parameters:
    gdf (GeoDataFrame): The GeoDataFrame to check.

    Returns:
    str: The geometry type ('Point', 'Polygon', 'Line') if found, otherwise 'Geom type UNKNOWN'.
    """
    
    logging.info("checkGeomType")
    points = gdf.loc[(gdf.geometry.geometry.type=='Point') | (gdf.geometry.geometry.type=='MultiPoint' )]
    polygons = gdf.loc[(gdf.geometry.geometry.type=='Polygon') | (gdf.geometry.geometry.type=='MultiPolygon')]
    lines = gdf.loc[(gdf.geometry.geometry.type=='LineString') | (gdf.geometry.geometry.type=='MultiLineString')]
    
    if points.shape[0]>0:
        return 'Point'
    if polygons.shape[0]>0:
        return 'Polygon'
    if lines.shape[0]>0:
        return 'Line'
    
    logging.info("Geom type UNKNOWN")

# Spliting Geometry
    
def splitGeom(gdf_to_split, gdf_from_raster, fp2):
    """
    Splits the geometry of a GeoDataFrame based on the overlay with another GeoDataFrame.

    Args:
        gdf_to_split (GeoDataFrame): The GeoDataFrame to be split.
        gdf_from_raster (GeoDataFrame): The GeoDataFrame used for overlay.
        fp2: A value used for some computation.

    Returns:
        GeoDataFrame: The resulting GeoDataFrame after the split.

    Raises:
        Exception: If an error occurs during the overlay operation.

    Deprecated:
        This function is deprecated and should not be used anymore.

    """
    gdf_to_split = GeoDataFrame([gdf_to_split], crs="EPSG:3163")
    gdf_to_split.geometry.plot(cmap='tab10')
    try:
        res_identity = gdf_to_split.overlay(gdf_from_raster, how='identity')
    except Exception as e:
        logging.info(f"gdf_to_split overlay error: {e}")
    fp2.value += 1
    return res_identity



def splitGeomByAnother(gdf_to_split, by_geom, overlayHow="intersection", keep_geom_type=True, epsg="EPSG:3163"):
    '''
    Split a GeoDataFrame by another geometry and return a new GeoDataFrame with the intersected geometries.

    Parameters:
    gdf_to_split (GeoDataFrame): The GeoDataFrame to be split.
    by_geom (GeoDataFrame or geometry): The geometry or GeoDataFrame used to split the gdf_to_split.
    overlayHow (str, optional): The overlay operation to perform. Defaults to "intersection".
    keep_geom_type (bool, optional): Whether to keep the geometry type of the resulting GeoDataFrame. Defaults to True.
    epsg (str, optional): The EPSG code of the coordinate reference system. Defaults to "EPSG:3163".

    Returns:
    GeoDataFrame: A new GeoDataFrame with the intersected geometries and copied attributes.
    '''
    try:
        if not isinstance(gdf_to_split, GeoDataFrame):
            logging.info(f"convert gdf_to_split: {type(gdf_to_split)}")
            gdf_to_split = GeoDataFrame([gdf_to_split], crs=epsg)

        gdf_to_split = gdf_to_split.explode(index_parts=True, ignore_index=True).reset_index()
        result_intersection = gdf_to_split.overlay(by_geom, how=overlayHow, keep_geom_type=keep_geom_type)
        return result_intersection

    except Exception as e:
        logging.info(f"Unexpected error splitGeomByAnother: {e}")
        cols = by_geom.columns.tolist() + gdf_to_split.columns.tolist()
        cols = list(dict.fromkeys(cols))  # suppresion des doublons
        logging.info(f"more details: {cols}")
        errors = gdf_to_split

        return errors


def splitGeomByDimSpatial(gdf_to_split, by_geoms):
    """
    Splits a GeoDataFrame by another GeoDataFrame based on spatial dimensions.

    Args:
        gdf_to_split (GeoDataFrame): The GeoDataFrame to be split.
        by_geoms (GeoDataFrame): The GeoDataFrame containing the geometries used for splitting.

    Returns:
        GeoDataFrame: The resulting GeoDataFrame after the split.

    """
    res = splitGeomByAnother(gdf_to_split, by_geoms[['id_spatial', 'geometry']])
    return res


# Dask processing
    
def daskSplitGeomByAnother(gdf_to_split, iterables: tuple):
    '''
    Split a GeoDataFrame by intersecting it with another set of geometries.

    Parameters:
    - gdf_to_split (GeoDataFrame): The GeoDataFrame to be split.
    - iterables (tuple): A tuple containing the set of geometries to intersect with and the overlay method.

    Returns:
    - result (GeoDataFrame): A GeoDataFrame containing the split geometries with copied attributes.

    Notes:
    - The `gdf_to_split` parameter is the unit to be analyzed.
    - The `iterables` parameter should contain the set of geometries to intersect with (`by_geoms`) and the overlay method (`overlayHow`).
    - If `keep_geom_type` is set to True, the resulting geometries will have the same geometry type as the original geometries.
    - If no intersections are found, an empty GeoDataFrame with the same columns as `gdf_to_split` will be returned.
    - If `gdf_to_split` is empty, an empty GeoDataFrame with the same columns as `gdf_to_split` will be returned.
    - If an error occurs during the process, an error message will be printed.

    '''
    logging.info(f"daskSplitGeomByAnother ...")
    logging.debug(f"daskSplitGeomByAnother gdf type {type(gdf_to_split)}")
    logging.debug(f"daskSplitGeomByAnother gdf {gdf_to_split}")

    logging.debug(f"daskSplitGeomByAnother iterables {iterables}")

    by_geoms, overlayHow = iterables
    result_intersection = []
    by_geoms.set_index('id_spatial', inplace = True, drop=False)
    listId_Spatial = list(dict.fromkeys(by_geoms.id_spatial.to_list()))

    try:
        logging.debug(f"daskSplitGeomByAnother gdf_to_split.shape[0]>0 =  {gdf_to_split.shape[0]>0}")
        if gdf_to_split.shape[0]>0:
            for id_spatial in listId_Spatial:
                by_geom = by_geoms.loc[[id_spatial]]
                if gdf_to_split.shape[0] > 0:
                    intersection = gdf_to_split.overlay(by_geom, how=overlayHow, keep_geom_type=True)
                    logging.info(f"len intersection: {len(intersection)}")
                    if intersection.shape[0] > 0:
                        result_intersection.append(intersection)
                    else:
                        pass
            if len(result_intersection) > 0:
                
                result = pd_concat(result_intersection)
                logging.info(f"End all idSpatial {result}")
                return result
            else:
                logging.warning(f"NO result_intersection: {result_intersection}")
                cols = gdf_to_split.columns.tolist()
                cols.insert(len(cols)-1,"id_spatial")
                result = GeoDataFrame(data=None, columns=cols)
                return result
        else:
            logging.debug(f"No data to intersect: {gdf_to_split.shape[0]}")
            cols = gdf_to_split.columns.tolist()
            cols.insert(len(cols)-1,"id_spatial")
            result = GeoDataFrame(data=None, columns=cols)
            return result
    except Exception as e:
        logging.critical(f"Unexpected error daskSplitGeomByAnother: {e}")


    
def overlapsGeom(ddf_to_split, by_geom):
    '''
    ddf_to_split : unité analysée
    by_geom: indicateur ou dimension spatiale
    
    On créer des géometries découpées par intersection
    keep_geom_type est à TRUE sinon on a des warning sur des géometries qui sont supprimées 
    
    return: un gdf découpé par les géometries des entités by_geom avec copies des attributs 
    
    '''
    logging.info("OverlapsGeom  ... ")
    overlapsList = []
    try:
       
        for i in range(by_geom.shape[0]):
            
            byG = by_geom.geometry.simplify(1000).iloc[i]
            id_spatial = by_geom.id_spatial.iloc[i]
            logging.info(f"DASK OverlapsGeom id_spatial : {id_spatial}")
            set_operation = ddf_to_split.intersects(byG)
            logging.info(f"set_operation intersects: {set_operation}")

            ddf_intersects = ddf_to_split[set_operation]
            
            if ddf_intersects.shape[0] == 0:
                pass
            else:
                overlaps = ddf_intersects.overlaps(byG)
                logging.info(f"set_operation overlaps: {overlaps}")
                ddf_overlaps = ddf_intersects[overlaps]

                if ddf_overlaps.shape[0] > 0:
                    ddf_overlaps['overlaps'] = True
                    ddf_overlaps['id_spatial'] = id_spatial
                    overlapsList.append(ddf_overlaps)

                ddf_within = ddf_intersects[~overlaps]

                if ddf_within.shape[0] > 0:
                    ddf_within['overlaps'] = False
                    ddf_within['id_spatial'] = id_spatial
                    overlapsList.append(ddf_within)

        if len(overlapsList) > 0:
            result = dd_concat(overlapsList)
            logging.info(f"END result: {result}")
            
            return result
        else:
            ddf_to_split['id_spatial']="0"
            return ddf_to_split
    except Exception as e:
        logging.error("Unexpected error daskGeomWhoNeedToBeCut: {e}")
        logging.error(f"ddf_to_split: {ddf_to_split}")
        logging.error(f"by_geom: {by_geom}")

    ddf_to_split['id_spatial']="0"
    return ddf_to_split


def daskOverlapsGeom(ddf_to_split, by_geom):
    '''
    Perform spatial partitioning and intersection operations on a Dask DataFrame.

    Args:
        ddf_to_split (Dask DataFrame): The Dask DataFrame to be split.
        by_geom (GeoDataFrame): The spatial entities used for splitting.

    Returns:
        Dask DataFrame: A new Dask DataFrame that is split based on the spatial entities.

    Raises:
        Exception: If an unexpected error occurs during the operation.

    Notes:
        - The function calculates the spatial partitions of the input Dask DataFrame.
        - It iterates over each spatial entity in the `by_geom` GeoDataFrame.
        - For each entity, it performs intersection and overlap operations on the Dask DataFrame.
        - The resulting Dask DataFrames are appended to a list.
        - If any overlaps are found, the list is concatenated into a single Dask DataFrame and returned.
        - If no overlaps are found, the original Dask DataFrame is returned.
    '''
    logging.info("DASK OverlapsGeom... ")
    overlapsList = []
    ddf_to_split.calculate_spatial_partitions()
    try:
        for i in range(by_geom.shape[0]):
            byG = by_geom.geometry.simplify(1000).iloc[i]
            id_spatial = by_geom.id_spatial.iloc[i]
            logging.info(f"DASK OverlapsGeom id_spatial : {id_spatial}")
            set_operation = ddf_to_split.intersects(byG)
            logging.info(f"set_operation intersects: {set_operation}")
            ddf_intersects = ddf_to_split[set_operation]
            overlaps = ddf_intersects.overlaps(byG)
            logging.info(f"set_operation overlaps: {overlaps}")
            ddf_overlaps = ddf_intersects[overlaps]
            ddf_overlaps['overlaps'] = True
            ddf_overlaps['id_spatial'] = id_spatial
            overlapsList.append(ddf_overlaps)
            ddf_within = ddf_intersects[~overlaps]
            ddf_within['overlaps'] = False
            ddf_within['id_spatial'] = id_spatial
            overlapsList.append(ddf_within)

        if len(overlapsList) > 0:
            result = dd_concat(overlapsList)
            logging.info(f"END result: {result}")
            return result
        else:
            return ddf_to_split
    except Exception as e:
        logging.info(f"Unexpected error daskGeomWhoNeedToBeCut: {e}")
        logging.info(f"ddf_to_split: {ddf_to_split}")
        logging.info(f"by_geom: {by_geom}")

    return ddf_to_split

def cleanOverlaps(df, dissolveby):
    """
    Cleans overlaps in a DataFrame's geometry column.

    Args:
        df (pandas.DataFrame): The DataFrame containing the geometry column.
        dissolveby (list): A list of column names to dissolve the DataFrame by.

    Returns:
        pandas.DataFrame: The cleaned DataFrame with overlaps removed.

    """
    logging.info("cleanOverlaps")
    df.reset_index(drop=True, inplace=True)
    df = df.explode(index_parts=False, ignore_index=True)
    df["geometry"] = [feature if type(feature) == Polygon or type(feature) == MultiPolygon else np.nan for feature in df["geometry"]]
    df.dropna(subset=['geometry'], inplace=True)
    df = df.loc[df.geometry.area >= 20]
    dissolveby = list(dict.fromkeys(dissolveby))
    
    df = df.dissolve(dissolveby).explode(index_parts=False).reset_index()
    
    logging.info(f"Nettoyage terminé. Résultat : {df}")      

    return df
