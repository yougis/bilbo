from oeilnc_utils.connection import fixOsPath, getSqlWhereClauseBbox
from dask_geopandas import from_geopandas as ddg_from_geopandas
from intake import Catalog
from geopandas import GeoDataFrame
from dask.distributed import Client
from os import getenv
import logging
from pandas import concat as pd_concat

from oeilnc_config import settings
from oeilnc_utils import connection
from oeilnc_utils.geometry import splitGeomByAnother, cleanOverlaps, daskSplitGeomByAnother

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%d/%m/%Y %H:%M:%S', level=logging.INFO)

logging.info("GeoIndicator - Distribution Imported")


def parallelize_DaskDataFrame(df, func, paramsTuples, nbchuncks=20):
    """
    Parallelizes the execution of a function on a Dask DataFrame.

    Args:
        df (Dask DataFrame): The input Dask DataFrame.
        func (function): The function to be applied on each partition of the DataFrame.
        paramsTuples (iterable): The iterable containing the parameters for the function.
        nbchuncks (int, optional): The number of partitions to create. Defaults to 20.

    Returns:
        Dask DataFrame: The result DataFrame after applying the function on each partition.
    """
    data = ddg_from_geopandas(df, npartitions=nbchuncks)
    data.calculate_spatial_partitions()
    df2 = data.map_partitions(func, iterables=paramsTuples).compute()
    return df2


def parallelize_DaskDataFrame_From_Intake_Source(intakeSource: Catalog.entry, func, paramsTuples, conf_parquet_file, metaModelList=None,  nbchuncks=20):
    """
    Parallelizes the processing of a Dask DataFrame created from an Intake data source.

    Args:
        intakeSource (intake.catalog.CatalogEntry): The Intake data source.
        func (function): The function to be applied to each partition of the Dask DataFrame.
        paramsTuples (tuple): The parameters to be passed to the function.
        conf_parquet_file (tuple): A tuple containing the table name and the extension table name.
        metaModelList (list, optional): The list of column names for the metadata GeoDataFrame. Defaults to None.
        nbchuncks (int, optional): The number of chunks to split the Dask DataFrame into. Defaults to 20.

    Returns:
        dask.dataframe.DataFrame: The processed Dask DataFrame.

    Raises:
        Exception: If an error occurs during parallelization.

    """
    client = settings.getDaskClient()

    logging.info(f"reading intake source  {intakeSource}...")
    df = intakeSource.read()
    logging.info(f"df: {len(df.index)}")
    if len(df.index) > 0:
        
        if metaModelList:
            logging.info(f"metaModelList {metaModelList}")
            df_meta =GeoDataFrame(columns = metaModelList)
        else:
            metaModelList = df.columns
            df_meta =GeoDataFrame(columns = metaModelList)
        #df.reindex(columns=columnList)
        print("Load data in memory", df.shape)
        print("converting to dask with chunksize ", nbchuncks)
        data = ddg_from_geopandas(df,nbchuncks)
        print("data", data)
        try:
            df2 = data.map_partitions(func, iterables=paramsTuples, meta=df_meta)
        except Exception as e:
            print("DASk  parallelize ERROR: ", e)
        if client:   
            return client.persist(df2)
        return df2.compute()
    else:
        return False
    

def generateIndicateur_parallel_v2(data, iterables):
    '''
    data : unité d'analyse
    iterables: configuration de la données indicateur à croiser avec l'unité d'analyse
    
    La version 2 fonctionne bien avec une données data organisée spatialement. Effectuer un order by sur un attribut qui organise les données spatialement avant.
    les données erosions sont par exemple organisée par identidiant et se suivent dans l'espace par ligne/bande successives.
    Cette indexation spatiale permet d'eviter des erreurs memoires, des crashs du worker, des crash du serveur de base de donnée, 
    du fait d'un trop grand nombre ou trop grande complexité des données à intersecer (gdf_to_split)
    
    '''
    print('processing generateIndicateur_parallel')
    paths = settings.getPaths()


    data_catalog_dir = paths.get('data_catalog_dir')
    commun_path  = paths.get('commun_path')


    indicateurSpec, individuStatSpec, data_indicateur, keepList, data_geom, data_indicator_geom_col  = iterables

    result = GeoDataFrame()
    
    
    catalog = fixOsPath(f"{data_catalog_dir}{indicateurSpec.get('catalogUri',None)}", commun_path )
    dataName = indicateurSpec.get('dataName',None)
    sql_expr = indicateurSpec.get('sql_expr',None)
    indexRef = individuStatSpec.get('indexRef',None)
    
    overlayHow = indicateurSpec.get('overlayHow',None)
    print('indicateurSpec', indicateurSpec)
    print('indexRef', indexRef)
    indicateur_dissolve_byList = individuStatSpec.get('indicateur_dissolve_byList',[])  + indicateurSpec.get('indicateur_dissolve_byList',[]) + [indexRef]
    
    print('data cols', data.columns)
    
    if data_geom in data.columns and data_geom != 'geometry':
        data.rename_geometry('geometry', inplace=True)
    
    # on calcul l'emprise total du geodataframe (du chunck dans le cas d'un traitement DASK)
    xmin, ymin, xmax, ymax = data.total_bounds
    bbox = [(xmin, ymin), (xmax, ymax)]
    
    # On construit la requete SQl correspondante pour chercher les données indicateurs de la même emprise
    sql_with_where_clause = connection.getSqlWhereClauseBbox(bbox,data_indicator_geom_col,"3163","3163")
        
    # Si on dispose d'une requete sql spécifique on l'utilise et on ajoute la clause where de la BBOX 
         

    #
    if isinstance(data_indicateur,GeoDataFrame):
        by_geom_filtered = data_indicateur
    else:        
        if not sql_expr:
            sql_expr = data_indicateur.sql_expr

        if sql_expr.find("where") >= 1 :
            data_ind = data_indicateur(sql_expr=f'{sql_expr} and {sql_with_where_clause}')
        else:
            data_ind = data_indicateur(sql_expr=f'{sql_expr} where {sql_with_where_clause}')
        
        by_geom_filtered = data_ind.read()
        
    by_geom_filtered = by_geom_filtered.cx[xmin:xmax,ymin:ymax]
    
    data.columns = data.columns.str.lower()
    #data = cleanOverlaps(data, individuStatSpec.get('indicateur_dissolve_byList',[]))
    result = splitGeomByAnother(data,by_geom_filtered,overlayHow=overlayHow)
    
    if not result.empty:
 
        if indexRef not in result.columns :
            result[indexRef] = result[indexRef + '_' + '1' ]

        result['id_split'] = result[indexRef].map(str) + '_' + result.index.map(str)

        if len(keepList)>0:
            result = cleanOverlaps(result[keepList],indicateur_dissolve_byList+['id_split'])
        else:
            result = cleanOverlaps(result,indicateur_dissolve_byList)

        result = result[keepList]
        return result
    else:
        print("Result is empty")
        return result
    

def generateIndicateur_parallel(data, iterables):
    '''
    data : unité d'analyse
    iterables: configuration de la données indicateur à croiser avec l'unité d'analyse
    '''
    print('processing generateIndicateur_parallel')
    indicateurSpec, individuStatSpec, data_indicateur, keepList, data_geom, data_indicator_geom_col  = iterables
    paths = settings.getPaths()


    data_catalog_dir = paths.get('data_catalog_dir')
    commun_path  = paths.get('commun_path')
    
    #data = data.read()
    
    catalog = fixOsPath(f"{data_catalog_dir}{indicateurSpec.get('catalogUri',None)}", commun_path )
    dataName = indicateurSpec.get('dataName',None)
    sql_expr = indicateurSpec.get('sql_expr',None)
    indexRef = individuStatSpec.get('indexRef',None)
    
    overlayHow = indicateurSpec.get('overlayHow',None)
    indicateur_dissolve_byList = individuStatSpec.get('indicateur_dissolve_byList',[])  + indicateurSpec.get('indicateur_dissolve_byList',[]) + [indexRef]
    

    #print("data loaded ", data.shape, data.columns)
    if data_geom in data.columns:
        #print("geom",data_geom)
        data = data.rename(columns ={data_geom:'geometry'})
    
    
    #print("data lenght", data.shape)
    result = GeoDataFrame()
    for _,row in data.iterrows():
        # on a besoin d'un gdf pour faire l'overlay : splitGeomByAnother
        gdf_to_split = GeoDataFrame([row],crs="EPSG:3163")
        xmin, ymin, xmax, ymax = gdf_to_split.total_bounds
        bbox = [(xmin, ymin), (xmax, ymax)]
        #print("bbox", bbox)
                
        sql_with_where_clause = getSqlWhereClauseBbox(bbox,data_indicator_geom_col,"3163","3163")
        
       # print("sql_with_where_clause", sql_with_where_clause)
        if sql_expr:
            print(sql_expr, sql_with_where_clause)
            if sql_expr.find("where") >= 1 :
                data_ind = data_indicateur(sql_expr=f'{sql_expr} and {sql_with_where_clause}')
            else:
                data_ind = data_indicateur(sql_expr=f'{sql_expr} where {sql_with_where_clause}')       
        else:
            pass
            #data_indicateur = getattr(open_catalog(catalog),dataName)
    
        if isinstance(data_indicateur,GeoDataFrame):
            by_geom_filtered = data_indicateur
        else:
            sql_expr = data_indicateur.sql_expr
            
            if sql_expr.find("where") >= 1 :
                data_ind = data_indicateur(sql_expr=f'{sql_expr} and {sql_with_where_clause}')
            else:
                data_ind = data_indicateur(sql_expr=f'{sql_expr} where {sql_with_where_clause}')
            by_geom_filtered = data_ind.read()
            
        by_geom_filtered = by_geom_filtered.cx[xmin:xmax,ymin:ymax]
        gdf_to_split.columns = gdf_to_split.columns.str.lower()
        
        
        c = splitGeomByAnother(gdf_to_split,by_geom_filtered,overlayHow=overlayHow)
        
        result = pd_concat([result,c])
    
    if not result.empty:

        if indexRef not in result.columns :
            result[indexRef] = result[indexRef + '_' + '1' ]

            #del by_geom_filtered, data_ind, c
            #gc.collect()     


        result['id_split'] = result[indexRef].map(str) + '_' + result.index.map(str)

        if len(keepList)>0:
            result = cleanOverlaps(result[keepList],indicateur_dissolve_byList+['id_split'])
        else:
            result = cleanOverlaps(result,indicateur_dissolve_byList)

        result = result[keepList]

        print("Netoyage terminée")
        # ajout suite à la regression à cause de l'indicteur à partir de raster 
        #result['id_split'] = result[indexRef].map(str) + "_" +  result.index.map(str)

        

        return result
    else:
        print("Result is empty")