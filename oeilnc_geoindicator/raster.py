import logging
#from warnings import deprecated
from oeilnc_config import settings

from os import getenv
import os
import yaml
from intake import open_catalog
import rasterio
import threading
from rasterstats import zonal_stats
import concurrent.futures
from geopandas import GeoDataFrame
from shapely.geometry import shape

from oeilnc_utils.raster import block_shapes, changeTypeMaskArrayToUint8, changeTypeMaskArrayToUint16
from oeilnc_utils.connection import fixOsPath, fixpath
from oeilnc_utils.geometry import checkGeomType, splitGeomByAnother
from oeilnc_config.settings import getPaths, getDbConnection, getDaskClient

from oeilnc_geoindicator import gee
import numpy as np
import pandas as pd
from dask.distributed import get_worker

logging.info("GeoIndicator - Raster Imported")



def getStatMultiThread(gdf,width=512, height=512,raster="full_masked.tif"):

    num_workers=12
    geoprocess=[]   
    gdf_filtered_list=[]
    out_grids=[]
    
    with rasterio.open(raster) as src:
        blocks = list(block_shapes(src,width,height))
        print(src.profile)
        count = 0
        read_lock = threading.Lock()
        def process(window):
            with read_lock:
                xmin, ymin, xmax, ymax = src.window_bounds(window)
                bounds = [xmin,ymin,xmax,ymax]
                gdf_filtered = gdf.cx[xmin:xmax,ymin:ymax]
                if gdf_filtered.shape[0] > 0:
                    img = src.read(1, window=window)
                    transform = src.window_transform(window)
                    result = zonal_stats(gdf_filtered,img, affine=transform, nodata=99,geojson_out=True,raster_out=True, all_touched=True)
                    geoprocess.append(result)
                    
                    
            return geoprocess

        with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
            executor.map(process, blocks)
            executor.shutdown()
        
    return geoprocess


# def createRasterMasked(rasterDataset, features, identifiant):

#     # Deprecated
#     out_img, out_transform = rasterio.mask(data, shapes=getFeatures(features), crop=True,filled=False)
#     out_meta = data.meta.copy()
#     out_meta.update({"driver": "GTiff",
#                      "height": out_img.shape[1],
#                      "width": out_img.shape[2],
#                      "transform": out_transform,
#                      "nodata":0,
#                      "dtype": rasterio.ubyte})

#     with rasterio.open(f"tmp/{identifiant}_masked.tif", "w", **out_meta) as dest:
#         dest.write(out_img)



def createRasterIndicateur(data,uri_image, spec_raster_indicateur, epsg="EPSG:3163"):
    windows_width = spec_raster_indicateur.get('windows_width',2048)
    windows_height = spec_raster_indicateur.get('windows_height',2048)
    indicateur_raster = GeoDataFrame.from_features([item for i in getStatMultiThread(data,windows_width,windows_height,uri_image) for item in i],crs=epsg)
    
    if indicateur_raster.shape[0] > 0:
        try:
            indicateur_raster.sindex
        except Exception as e:
            print("Indexing error:", e)
    return indicateur_raster


def polygonizeRaster(indexRef, rowId, img, crs, transform):
    """
    Polygonizes a raster image.

    Args:
        indexRef (int): The reference index.
        rowId (int): The row ID.
        img (numpy.ndarray): The raster image.
        crs (rasterio.crs.CRS): The coordinate reference system.
        transform (affine.Affine): The affine transformation.

    Returns:
        dict: A dictionary containing the index reference, row ID, mini raster values, and geometries.
    """
    print(rowId, "  polygonizeRaster", transform)
    try:
        geometries = []
        colvalues = []

        for (geom, colval) in rasterio.features.shapes(img, mask=None, transform=transform):
            polygon = shape(geom)
            geometries.append(polygon)
            colvalues.append(colval)
        return {indexRef: rowId, "mini_raster_value": colvalues, "geometry": geometries}
    except Exception as e:
        print("Unexpected error:", e)
   
def polygonizeRasterThreader(gdf_to_split, individuStatSpec):
    """
    Threaded function to polygonize raster data.

    Args:
        gdf_to_split (GeoDataFrame): The GeoDataFrame to split.
        individuStatSpec (dict): The individual statistical specifications.

    Returns:
        list: A list of results from polygonizing the raster data.
    """
    print("""polygonizeRasterThreader""")
    indexRef = individuStatSpec.get('indexRef', None)
    num_workers = 12
    result = []
    n = 1000  # chunk row size
    read_lock = threading.Lock()

    def process(gdf_to_split):
        crs = gdf_to_split.crs
        print("read_lock polygonizeRasterThreader")
        try:
            result.append(gdf_to_split.apply(lambda row: polygonizeRaster(indexRef, getattr(row, indexRef), row.mini_raster_array, crs, row.mini_raster_affine), axis='columns', result_type='expand'))
            # print("next polygonizeRasterThreader")
        except Exception as e:
            print("Error during execution polygonizeRaster:", e)
        return result

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
        # print("thread",gdf_to_split.shape)
        list_df = [gdf_to_split[i:i+n] for i in range(0, gdf_to_split.shape[0], n)]
        executor.map(process, list_df)

    return result


def indicateur_from_raster(data, iterables):
    """
    Extracts indicators from a raster based on the geometries of individual data points or polygons.

    Args:
        data (GeoDataFrame): The input data containing geometries.
        iterables (tuple): A tuple containing indicator specifications, individual statistics specifications,
                           EPSG code, and metamodel.

    Returns:
        GeoDataFrame: The final indicator data containing the extracted values from the raster.

    Raises:
        ValueError: If the geometry type of the input data is not supported.

    """

    paths = getPaths()


    data_catalog_dir = paths.get('data_catalog_dir')
    commun_path  = paths.get('commun_path')

    indicateurSpec, individuStatSpec, epsg , metamodel= iterables
    spec_raster_indicateur = indicateurSpec.get('confRaster',{})
    uri_image = f"{spec_raster_indicateur.get('uri_image',None)}"
    if os.name == 'nt':
        replace = commun_path
        print('fix path:', commun_path)
        uri_image = fixpath(uri_image,replace)
         #print('New uri_image for Windows',uri_image)

    indexRef = individuStatSpec.get('indexRef',None)
    keepList = individuStatSpec.get('keepList',[]) + indicateurSpec.get('keepList',[])[1:]

    # par defaut la valeur issue du raster sera stocké dans un champ nommé comme le premier de la Keeplist des spec 
    if len(indicateurSpec.get('keepList',[]>0)):   
        raster_classe = indicateurSpec.get('keepList',[])[0]
    else:
        raster_classe = "classe"
        
    # on voit si on traite une donnée temporelle, auquelle cas un attribut doit être créé pour intégrer la valeur de date ou d'année correspondante.
    # la proprieté confTemporal de indicateurSpec permet de définir comment l'attribut est nommé (afin d'être raccord au modèle si la table existe déjà), 
    # indique si la valeur est fixe ou si elle sera tirée de la valeur du pixel auquel cas le champs raster classe devient colName
    spec_temporal_indicateur = indicateurSpec.get('confTemporal',{})
    fromPixel = spec_temporal_indicateur.get('valuefromPixel',False)
    colDate = spec_temporal_indicateur.get('colName',raster_classe)

    if fromPixel:
        raster_classe = colDate
    else:
        data[colDate] = spec_temporal_indicateur.get('value',np.nan)

    emptyDataframe =  GeoDataFrame(columns = metamodel, crs=data.crs)    
    # on va chercher les indicateurs dans le raster à partir des géometries des données individuStat
    # Methode variable selon le type de géometry en entrée :
    #print(checkGeomType(data))
    if checkGeomType(data) == 'Point':
        
        with rasterio.open(uri_image) as src:
            coord_list = [(x,y) for x,y in zip(data['geometry'].x , data['geometry'].y)]
            data[raster_classe] = [x for x in src.sample(coord_list)]
            data['id_split'] = 'None'
            data= data[metamodel]
        return data
    elif  checkGeomType(data) == 'Polygon':

        if spec_raster_indicateur.get('api',None) == 'GEE':
            logging.info(get_worker().name , "->  Google Earth Engine API !")
            try:
                try:
                    gee.initialize()
                except Exception as e:
                    print("gee.initialize execption :", e)
                gdf_to_split = gee.extract_data(indicateurSpec,data)
        
            except Exception as e:
                logging.info(get_worker().name , "-> Unexpected error in gee.extract_data:", e)
                print("trying to reimport for Initialze again")
                from oeilnc_geoindicator import gee as gee_
                gdf_to_split = gee_.extract_data(indicateurSpec,data)
        else:
            gdf_to_split = createRasterIndicateur(data,uri_image, spec_raster_indicateur, epsg)

        # on regarde
        if gdf_to_split.shape[0] > 0:
            if gdf_to_split['max'].max()< 255 :
                gdf_to_split["mini_raster_array"] = gdf_to_split["mini_raster_array"].apply(changeTypeMaskArrayToUint8)
            else:
                gdf_to_split["mini_raster_array"] = gdf_to_split["mini_raster_array"].apply(changeTypeMaskArrayToUint16)


            min_diff_max= "min != max"
            min_eg_max= "min == max"
            without_nodata= "nodata < 1"
            with_nodata = "nodata > 0"
            gdf_to_split_filtered = gdf_to_split.query(f"{min_diff_max} or {with_nodata}")
            #print('gdf_to_split_filtered : ', gdf_to_split_filtered)
            gdf_to_concat = gdf_to_split.query(f"{min_eg_max} and {without_nodata}")

            if gdf_to_split_filtered.shape[0] == 0:
                gdf_to_split_filtered = emptyDataframe
                result_minirastersplit = emptyDataframe

            else:
                # on polygonize uniquement les mini rasters qui intersectent des géometries 
                # on pourrait le faire pour ceux ayant des min max différents (+ les doublons avec des valeurs non similaires) mais on le fait pas ici
                # eg . gdf_to_split = gdf_to_split[['id','min','max']].drop_duplicates(subset=['id','min','max'],keep=False)
                try:
                    explode_raster = pd.concat([ item for item in polygonizeRasterThreader(gdf_to_split_filtered,individuStatSpec)]).set_index([indexRef]).apply(pd.Series.explode).reset_index()
                    print('finish polygonizeRasterThreader ')
                    explode_raster = GeoDataFrame(explode_raster, geometry='geometry',crs=epsg)
                    explode_raster.sindex
                except Exception as e:
                    print("Unexpected error in explode_raster:", e)
                    explode_raster= emptyDataframe
                    #return gpd.GeoDataFrame()

                try:
                    keepListAttribut = [indexRef,'geometry','min','max']+keepList
                    result_minirastersplit = pd.concat([ item for item in splitGeomThreader(gdf_to_split_filtered[keepListAttribut],explode_raster,spec_raster_indicateur,individuStatSpec, epsg)])
                    print("finish splitGeomThreader result_minirastersplit")
                    result_minirastersplit['id_split'] = result_minirastersplit.apply(lambda row :  str(getattr(row, indexRef + "_1")) + "_" +  str(row.name), axis=1)
                    result_minirastersplit[indexRef] = getattr(result_minirastersplit, indexRef + "_1")
                    result_minirastersplit[raster_classe]=result_minirastersplit['mini_raster_value']
                    if result_minirastersplit.crs is None:
                        result_minirastersplit.set_crs(epsg, inplace=True)
                        result_minirastersplit.to_crs(data.crs, inplace=True)
                    else:
                        result_minirastersplit.to_crs(data.crs, inplace=True)

                except Exception as e:
                    print("Unexpected error in indicateur_from_raster:", e)
                    result_minirastersplit=emptyDataframe
                    result_minirastersplit.to_crs(data.crs, inplace=True)

            try:
                if gdf_to_concat.shape[0] > 0 :
                    print("Size gdf_to_concat :",gdf_to_concat.shape )  
                    gdf_to_concat[raster_classe] = gdf_to_concat['min']
                    gdf_to_concat['id_split'] = gdf_to_concat[indexRef]
                    if gdf_to_concat.crs is None:
                        gdf_to_concat.set_crs(epsg, inplace=True)
                        gdf_to_concat.to_crs(data.crs, inplace=True)
                    else:
                        gdf_to_concat.to_crs(data.crs, inplace=True)
                else :
                    gdf_to_concat = emptyDataframe
                    gdf_to_concat.to_crs(data.crs, inplace=True)

                #print("result_minirastersplit[[metamodel]]",result_minirastersplit[metamodel].columns)
                #print("gdf_to_concat[metamodel]",gdf_to_concat[metamodel].columns)
                final_indicateur = pd.concat([result_minirastersplit[metamodel],gdf_to_concat[metamodel]])


            except Exception as e:
                print("Unexpected error in indicateur_from_raster - concatenate step:", e)
                final_indicateur = emptyDataframe

            return final_indicateur
        else:
            print(f"Pas de géometrie à créer")
            return emptyDataframe
    else:
        print(f"le type de géometrie {checkGeomType(data)} en entrée n'est pas pris en charge pour l'instant")
    
    
    # returning empty dataframe for dask model
   # column_names = listAttribut
    return emptyDataframe


def splitGeomThreader(gdf_to_split, explode_raster, spec_raster_indicateur, individuStatSpec, epsg):
    """
    Split the geometries in a GeoDataFrame using another raster GeoDataFrame.

    Args:
        gdf_to_split (GeoDataFrame): The GeoDataFrame containing the geometries to be split.
        explode_raster (GeoDataFrame): The raster GeoDataFrame used for splitting the geometries.
        spec_raster_indicateur (dict): A dictionary specifying the raster indicator.
        individuStatSpec (dict): A dictionary specifying the individual statistics.
        epsg (int): The EPSG code for the coordinate reference system.

    Returns:
        list: A list of GeoDataFrames containing the split geometries.
    """
    
    print("""splitGeomThreader""")

    indexRef = individuStatSpec.get('indexRef', None)
    num_workers = 12
    result = []
    n = 100  # chunk row size
    idx = 0

    overlayHow = spec_raster_indicateur.get("overlayHow", None)

    def process(gdf_to_split):
        crs = gdf_to_split.crs
        try:
            for index, row in gdf_to_split.iterrows():
                splitGeoms = splitGeomByAnother(row, explode_raster[getattr(explode_raster, indexRef).isin([row[indexRef]])], overlayHow=overlayHow, keep_geom_type=False, epsg=epsg)
                splitGeoms.index.name = 'uid'
                splitGeoms.reset_index(level=0, inplace=True)
                result.append(splitGeoms)
        except Exception as e:
            print("Unexpected error in splitGeomThreader:", e)

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
        list_df = [gdf_to_split[i:i+n] for i in range(0, gdf_to_split.shape[0], n)]
        executor.map(process, list_df)
        executor.shutdown()

    return result

#@deprecated("don't use")
def BboxIndicateur(yamlFile, typData="vecteur"):
    """
    Calculate the bounding box of the indicator based on the given YAML file.

    Parameters:
    - yamlFile (str): The name of the YAML file.
    - typData (str): The type of data, either "raster" or "vecteur". Default is "vecteur".

    Returns:
    - bbox_ind (list): The bounding box coordinates [xmin, ymin, xmax, ymax].
    
    """
    
    paths = getPaths()

    data_config_dir = paths.get('data_config_dir')

    data_catalog_dir = paths.get('data_catalog_dir')

    if typData == "raster":
        with open(f'{data_config_dir}{yamlFile}.yaml', 'r') as file:        
            individuStatSpec = yaml.load(file, Loader=yaml.Loader)
            confRaster = individuStatSpec.get('confRaster',None)
            width = confRaster.get('windows_width', None)
            height = confRaster.get('windows_height', None)
            raster = confRaster.get('uri_image',None)
            with rasterio.open(raster) as src:
                    blocks = list(block_shapes(src,width,height))
                    print(src.profile)
                    count = 0
                    read_lock = threading.Lock()
                    def process(window):
                            with read_lock:
                                    xmin, ymin, xmax, ymax = src.window_bounds(window)
                                    bbox_ind = [xmin,ymin,xmax,ymax]
            print("BBOX raster:",bbox_ind)
    elif typData == "vecteur":
        with open(f'{data_config_dir}{yamlFile}.yaml', 'r') as file:        
            individuStatSpec = yaml.load(file, Loader=yaml.Loader)
            dataName = individuStatSpec.get('dataName',None)
            catalogUri = individuStatSpec.get('catalogUri',None)
            indic = getattr(open_catalog(f'{data_catalog_dir}{catalogUri}'),dataName)
            xmin, ymin, xmax, ymax = indic.read().geometry.total_bounds
            bbox_ind = [xmin, ymin, xmax, ymax]
            print("BBOX vecteur:",bbox_ind)
    return bbox_ind    
