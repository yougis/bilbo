from oeilnc_utils.connection import fixOsPath, getSqlWhereClauseBbox
from geopandas import GeoDataFrame
import geopandas as gpd
from dask_geopandas import from_geopandas as ddg_from_geopandas
import logging
from pandas import concat as pd_concat


from oeilnc_config import settings
from oeilnc_utils import connection
from oeilnc_utils.geometry import splitGeomByAnother, cleanOverlaps, geomToH3
from oeilnc_geoindicator.geometry import voronoiSplitting

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


def parallelize_DaskDataFrame_From_Parquet(
        parquetFilePath: str,
        func, 
        paramsTuples, 
        conf_parquet_file, 
        metaModelList=None,  
        nbchuncks=20, 
        voronoi_splitting=False):
    
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
    from dask.distributed import get_client
    client = get_client()
    logging.info(f"reading parquet File  {parquetFilePath}...")
    df = gpd.read_parquet(f'parquet/{parquetFilePath}') 

    nbchuncks = len(df.index)


    logging.debug(f"df: {len(df.index)}")
    if len(df.index) > 0:
        
        if metaModelList:
            logging.debug(f"metaModelList {metaModelList}")
            df_meta =GeoDataFrame(columns = metaModelList)
        else:
            metaModelList = df.columns
            df_meta =GeoDataFrame(columns = metaModelList)
        #df.reindex(columns=columnList)
        logging.debug(f"Load data in memory {df.shape}")
        logging.debug(f"converting to dask with chunksize {nbchuncks}")
        data = ddg_from_geopandas(df,nbchuncks)
        logging.debug(f"data : {data}")
        logging.debug(f"func : {func}")
        try:
            df2 = data.map_partitions(func, iterables=paramsTuples, meta=df_meta)
        except Exception as e:
            logging.critical(f"DASk  parallelize ERROR: {e}")
        if client:
            #df2 = ddg_from_daskDataframe(df2.to_dask_dataframe(),'geometry')
            return df2
        return df2.compute()
    else:
        return False
    

def parallelize_DaskDataFrame_From_Intake_Source(
        intakeSource: any,
        func, 
        paramsTuples, 
        conf_parquet_file, 
        metaModelList=None,  
        nbchuncks=20, 
        voronoi_splitting=False):
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
    from dask.distributed import get_client
    client = get_client()
    logging.info(f"reading intake source  {intakeSource}...")
    df = intakeSource.read()
    crs = df.crs

    
    if voronoi_splitting:
        results = []
        total_splited=0
        for idx, row in df.iterrows():
            logging.info(f'iterate to split by voronoi befor process : {idx+1}/{len(df)}')
            polygon = row.geometry
            nbFormes = round(row.geometry.area / 100000000) # on divise en zone d'environs 100 km2
            total_splited += nbFormes 
            if nbFormes <=1:
                divided_polygons = gpd.GeoDataFrame(geometry=[row.geometry], crs=crs)
                for col in df.columns:                    
                    if col != 'geometry':
                        divided_polygons[col] = row[col]
            else:

                randomPoint=nbFormes*100
                divided_polygons = voronoiSplitting(polygon, randomPoint, 3 + nbFormes, crs) # +3 pour s'assurer d'avoir au moins 4 zones minimum sinon erreur
                for col in df.columns:
                    if col != 'geometry':
                        divided_polygons[col] = row[col]
                
            results.append(divided_polygons)
        df = GeoDataFrame(pd_concat(results, ignore_index=True))

        # Calculer la taille de chaque sous-ensemble
        taille_sous_ensemble = len(df) 
        if taille_sous_ensemble > 1 and total_splited > 1:
            for i in range(taille_sous_ensemble):
                parquetFilePath = f'parquet/{intakeSource.name}_{i + 1}.parquet'
                debut = i 
                fin = i + 1
                sous_ensemble = df.iloc[debut:fin]
                sous_ensemble.to_parquet(parquetFilePath)
            return parquetFilePath
        
    
    nbchuncks = len(df.index)


    logging.debug(f"df: {len(df.index)}")
    if len(df.index) > 0:
        
        if metaModelList:
            logging.debug(f"metaModelList {metaModelList}")
            df_meta =GeoDataFrame(columns = metaModelList)
        else:
            metaModelList = df.columns
            df_meta =GeoDataFrame(columns = metaModelList)
        #df.reindex(columns=columnList)
        logging.debug(f"Load data in memory {df.shape}")
        logging.debug(f"converting to dask with chunksize {nbchuncks}")
        data = ddg_from_geopandas(df,nbchuncks)
        logging.debug(f"data : {data}")
        logging.debug(f"func : {func}")
        try:
            df2 = data.map_partitions(func, iterables=paramsTuples, meta=df_meta)
        except Exception as e:
            logging.critical(f"DASk  parallelize ERROR: {e}")
        if client:
            #df2 = ddg_from_daskDataframe(df2.to_dask_dataframe(),'geometry')
            return df2
        return df2.compute()
    else:
        return False
        

def generateIndicateur_parallel_v2(data, iterables):
    '''
    Generate indicator in parallel version 2.

    Args:
        data (GeoDataFrame): The spatially organized data for analysis.
        iterables (tuple): Configuration of the indicator data to be crossed with the analysis unit.

    Returns:
        GeoDataFrame: The result of the indicator generation.

    Notes:
        - This version works well with spatially organized data.
        - It is recommended to perform an "order by" operation on an attribute that organizes the data spatially before using this function.
        - Spatial indexing helps avoid memory errors, worker crashes, database server crashes due to a large number or complexity of intersecting data (gdf_to_split).
    '''

    from oeilnc_config import settings
    from dask.distributed import get_client
    client = get_client()
    logging.info('GenerateIndicateur_parallel_V2')
    logging.debug(f'GenerateIndicateur_parallel_V2 - {type(data)}')
    paths = settings.getPaths()
    logging.debug(f'Path : {paths}')

    data_catalog_dir = paths.get('data_catalog_dir')
    commun_path  = paths.get('commun_path')


    indicateurSpec, individuStatSpec, data_indicateur, keepList, data_geom, data_indicator_geom_col, splitted  = iterables

    keepList_zoi = [col for col in data.columns if col in keepList and col != 'geometry']
    #keepList_theme = [col for col in by_geom_filtered.columns if col in keepList]

    if not splitted:
        data = geomToH3(data, res=8, clip=True, keepList=keepList_zoi)

    if data.shape[0]>1:
        
        splitted = True
        dd_data = ddg_from_geopandas(data,data.shape[0])
        df_meta = GeoDataFrame(columns = keepList)
        try:
            result = dd_data.map_partitions(generateIndicateur_parallel_v2,iterables=(indicateurSpec, individuStatSpec, data_indicateur, keepList, data_geom, data_indicator_geom_col , splitted), meta=df_meta)
        except Exception as e:
            logging.critical(f"DASk  parallelize generateIndicateur_parallel_v2 ERROR: {e}")        
        #result = dd_data.map_partitions(_daskSplitGeomByAnother, iterables=(by_geom_filtered[keepList_theme],overlayHow, keepList), meta=df_meta, align_dataframes=False)
        del dd_data 
        return result.compute()

    result = GeoDataFrame()
    
    
    catalog = fixOsPath(f"{data_catalog_dir}{indicateurSpec.get('catalogUri',None)}", commun_path )
    dataName = indicateurSpec.get('dataName',None)
    sql_expr = indicateurSpec.get('sql_expr',None)
    indexRef = individuStatSpec.get('indexRef',None)

    ### verifier que l'indexRef est unique ou sinon le rendre unique via l'utilisation de la valeur d'index
    if data[indexRef].duplicated().any():
        data[indexRef] = data[indexRef].astype(str) + '_' + data.index.astype(str)

    overlayHow = indicateurSpec.get('overlayHow',None)
    logging.debug(f'indicateurSpec {indicateurSpec}')
    logging.debug(f'indexRef: {indexRef}' )
    indicateur_dissolve_byList = individuStatSpec.get('indicateur_dissolve_byList',[])  + indicateurSpec.get('indicateur_dissolve_byList',[]) + [indexRef]
    
    logging.debug(f'data cols : {data.columns}' )
    
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
    
    if not by_geom_filtered.empty:

        by_geom_filtered = by_geom_filtered.cx[xmin:xmax,ymin:ymax]
        by_geom_filtered.columns = by_geom_filtered.columns.str.lower()


        data.columns = data.columns.str.lower()
        

        #data = geomToH3(data, res=8, clip=True, keepList=keepList_zoi)
        result = splitGeomByAnother(data,by_geom_filtered,overlayHow=overlayHow)
    else:
        logging.info("Result is empty")
        return GeoDataFrame()
    
    if not result.empty:
        logging.info(f"Result has {result.shape[0]} entities")

        if indexRef not in result.columns :
            result[indexRef+'_zoi'] = ""

        result['id_split'] = result[indexRef].map(str) + '_' + result.index.map(str)

        if len(keepList)>0:
            result = cleanOverlaps(result[keepList],indicateur_dissolve_byList+['id_split'])
        else:
            result = cleanOverlaps(result,indicateur_dissolve_byList)

        result = result[keepList]
        del data, by_geom_filtered
        return result
    else:
        logging.info("Result is empty")
        return result
    

def generateIndicateur_parallel(data, iterables):
    '''
    data : unité d'analyse
    iterables: configuration de la données indicateur à croiser avec l'unité d'analyse
    '''
    logging.info('processing generateIndicateur_parallel')
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
            logging.debug(f"{sql_expr} {sql_with_where_clause}")
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

        logging.info("Netoyage terminée")
        # ajout suite à la regression à cause de l'indicteur à partir de raster 
        #result['id_split'] = result[indexRef].map(str) + "_" +  result.index.map(str)

        

        return result
    else:
        logging.warning("Result is empty")