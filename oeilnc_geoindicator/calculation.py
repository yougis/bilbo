import logging

import pandas as pd
from geopandas import sjoin as gpd_sjoin
from geopandas import GeoDataFrame
from dask_geopandas import from_geopandas as ddg_from_geopandas, GeoDataFrame as DaskGeoDataFrame
from oeilnc_geoindicator.interpolation import indicateur_from_pre_interpolation, indicateur_from_interpolation
from oeilnc_geoindicator.distribution import parallelize_DaskDataFrame_From_Intake_Source, generateIndicateur_parallel_v2, generateIndicateur_parallel
from oeilnc_geoindicator.raster import indicateur_from_raster
from oeilnc_utils.connection import getSqlWhereClauseBbox, fixOsPath, persistGDF, AjoutClePrimaire
from oeilnc_utils.geometry import daskSplitGeomByAnother
from oeilnc_config.settings import getPaths, getDbConnection, getDaskClient
from intake import open_catalog


logging.info("GeoIndicator - Calculation Imported")



# on doit initialiser le projet pour disposer des variables globales utilisées dans les méthodes
# settings.initializeBilboProject(dotenvPath)

def calculateMesures(gdf, mesures):
    """
    Calculate measures for each geometry in a GeoDataFrame.

    Parameters:
        gdf (GeoDataFrame): The input GeoDataFrame containing geometries.
        mesures (DataFrame): The input DataFrame containing measure values.

    Returns:
        DataFrame: A DataFrame with duplicated rows for each measure value.

    """
    
    rows = pd.DataFrame()
    for _, m in mesures.iterrows():
        gdf['id_mesure'] = m.id_mesure
        
        gdf['values'] = gdf.geometry.area * m.scale
        rows = pd.concat([rows,gdf])

    return rows



def getMultipleIndexByVariable(x, variablesList):
    """
    Get multiple index by variable.

    Args:
        x (dict): The input dictionary.
        variablesList (list): The list of variables.

    Returns:
        tuple: The index tuple.
    """
    indexTuple = ()
    print("getMultipleIndexByVariable")
    if len(variablesList) == 0:
        print('Aucun index pour rechercher une reference dans pdRef')
        return None
    if len(variablesList) > 1:
        for v in variablesList:
            indexTuple = indexTuple + (x[v],)
    elif len(variablesList) == 1:
        indexTuple = (slice(None), x[variablesList[0]])

    return indexTuple

def calculateRatio(df, iterables):
    """
    Calculate the ratio for each row in the DataFrame.

    Parameters:
    - df: DataFrame - The input DataFrame.
    - iterables: tuple - A tuple containing the variables needed for calculation.

    Returns:
    - Series: The calculated ratio for each row in the DataFrame.
    """
    rationalizeBy, indexList, pdRef = iterables
    return df.apply(lambda x: (x[rationalizeBy] / pdRef.loc[getMultipleIndexByVariable(x, indexList), rationalizeBy].sum()) * 100, axis=1)


def groupResult(df, iterables):
    """
    Groups the DataFrame `df` by the specified `iterables` and calculates the sum of the `rationalizeBy` column.
    
    Args:
        df (pandas.DataFrame): The DataFrame to be grouped.
        iterables (tuple): A tuple containing the index list and the column to be rationalized.
    
    Returns:
        pandas.DataFrame: The grouped DataFrame with the sum of the `rationalizeBy` column multiplied by 100.
    """
    indexList, rationalizeBy = iterables
    pdRef = df.groupby(indexList)[[rationalizeBy]].sum() * 100
    return pdRef

# Point calculation

def countPointInArea(point, area, sJoinHow, indexRef, pointIndex):
    """
    Count the number of points within each area.

    Parameters:
    - point: GeoDataFrame of points.
    - area: GeoDataFrame of areas.
    - sJoinHow: Join method for spatial join.
    - indexRef: Column name to use as index in the result.
    - pointIndex: Column name to count points.

    Returns:
    - area_count_point: DataFrame with the count of points within each area.
    """
    joined_df = gpd_sjoin(point, area, how=sJoinHow, predicate='intersects')
    area_count_point = joined_df.groupby([indexRef], as_index=False)[pointIndex].count()
    print("area_count_point", area_count_point)
    return area_count_point


def aggregatePointAttribut(point, area, sJoinHow, indexRef, pointIndex, confAggregate):
    """
    Aggregates point attributes based on their intersection with areas.

    Args:
        point (GeoDataFrame): GeoDataFrame containing point geometries.
        area (GeoDataFrame): GeoDataFrame containing area geometries.
        sJoinHow (str): Join method for spatial join.
        indexRef (str): Column name to use as index for aggregation.
        pointIndex (str): Column name to use as index for point GeoDataFrame.
        confAggregate (dict): Dictionary specifying the attributes to aggregate.

    Returns:
        GeoDataFrame: Aggregated point attributes based on their intersection with areas.
    """
    joined_df = gpd_sjoin(point, area, how=sJoinHow, predicate='intersects')
    area_agg_point = joined_df.groupby(indexRef).aggregate(confAggregate.items())
    print('area agg', area_agg_point)
    return area_agg_point

def generateIndicateurPoint(data, iterables):
    """
    Generate indicator points based on input data and iterables.

    Args:
        data (DataFrame): Input data.
        iterables (tuple): Tuple containing indicator specifications, individual statistics specifications,
                           indicator data, and meta model list.

    Returns:
        DataFrame: Indicator points generated based on the input data and iterables.
    """
    indicateurSpec, individuStatSpec, data_indicateur, metaModelList  = iterables
    if not isinstance(data_indicateur,GeoDataFrame):
        data_indicateur = data_indicateur.read()
    
    
    indexRef = individuStatSpec.get('indexRef',None)
    keepList = [indexRef] + individuStatSpec.get('keepList',None)  + ['geometry','id_split']
    data = data[keepList]
    groupBy = indexRef
    sJoinHow=indicateurSpec.get('sJoinHow','inner')
    pointIndex=indicateurSpec.get('pointIndex',None)
    keepListIndicateur = [pointIndex] + indicateurSpec.get('keepList',None) + ['geometry']
    data_indicateur = data_indicateur[keepListIndicateur]

    confAggregate = indicateurSpec.get('confAggregate',None)
    print("data_indicateur  ",type(data_indicateur), " data ",type(data))

    if confAggregate:
        area_sjoin = aggregatePointAttribut(data_indicateur, data,sJoinHow,groupBy,pointIndex, confAggregate)
    else:
        area_sjoin = countPointInArea(data_indicateur, data,sJoinHow,groupBy,pointIndex)

    data = data.merge(
                area_sjoin, 
                on=indexRef, 
                how='left', 
                )
    renameMap={pointIndex:'nb'}
    data = data.rename(columns=renameMap)
    data = data[metaModelList]
    return data



# Dask calculation

def daskCalculateMesures(ddf, iterables):
    '''
    Calculate measures using Dask DataFrame.

    Args:
        ddf (Dask DataFrame): The input Dask DataFrame.
        iterables (tuple): A tuple containing the following elements:
            - mesures (DataFrame): A DataFrame containing the measures.
            - individuSpec (dict): A dictionary containing individual specifications.
            - indicateurSpec (dict): A dictionary containing indicator specifications.
            - nbchuncks (int): The number of chunks.

    Returns:
        Dask DataFrame: The calculated Dask DataFrame.
    '''
    from dask.distributed import get_client
    client = get_client()

    mesures, individuSpec, indicateurSpec, nbchuncks = iterables
    indexRef = individuSpec.get('indexRef', None)
    logging.info("daskCalculateMesures")
    confRatio = individuSpec.get('confRatio', None)
    
    indexList = confRatio.get('indexList', None)
    rationalizeBy = confRatio.get('rationalizeBy', None)
    
    calculateRatioAfter = False
    for _, m in mesures.iterrows():
        if m.id_mesure == 100:
            calculateRatioAfter = True  # case of a ratio
            pass       
        else:
            ddf['values'] = ddf.geometry.area * m.scale
    
    if calculateRatioAfter:
        ddf = client.persist(ddf)

        try:
            # create a multiindexed df
            pdRef = client.submit(lambda a: a.groupby(indexList)[[rationalizeBy]].sum() * 100, client.compute(ddf))
        except Exception as e:
            logging.info("Unexpected error calculateRatioAfter: {e}")
        
        try:
            pass
        except Exception as e:
            logging.info("Unexpected error calculateRatioAfter groupby: {e}")
        
        try:
            # dask does not support multi-indexed dfs
            ddf['ratio'] = ddf.map_partitions(calculateRatio, iterables=(rationalizeBy, indexList, pdRef), meta=('ratio', 'f8'))
            ddf = client.persist(ddf)
            newIndexList = [x for x in indexList if x != indexRef]
            ddf['ratio_id_spatial'] = ddf.map_partitions(calculateRatio, iterables=(rationalizeBy, newIndexList, pdRef), meta=('ratio_id_spatial', 'f8'))
            ddf = client.persist(ddf)
        except Exception as e:
            logging.info("Unexpected error calculateRatioAfter apply: {e}")
    
    return ddf



def generateValueBydims(data, iterables):
    """
    Generate values by dimensions.

    Args:
        data (DataFrame): The input data.
        iterables (tuple): A tuple of iterables containing various parameters.

    Returns:
        DataFrame: The generated values.

    """
    
    logging.info(f"generateValueBydims ... ")
    logging.debug(f"generateValueBydims data type {type(data)} {data}")

 
    individuSpec, indicateurSpec, dim_spatial, dim_mesure, model ,nbchuncks = iterables
    #print("data", data.columns, data.shape)
    spatials = dim_spatial.read()
    mesures= dim_mesure.read()
    indexRef = individuSpec.get('indexRef', 'id')
    confDims = individuSpec.get('confDims',None)
    keepList = individuSpec.get('keepList',[]) + indicateurSpec.get('keepList',[])
    overlayHow = indicateurSpec.get('overlayHow',None)
    epsg = indicateurSpec.get('epsg','EPSG:3163')
    confRatio = individuSpec.get('confRatio',None)
    indexList = confRatio.get('indexList',None)
    rationalizeBy = confRatio.get('rationalizeBy',None)
    
    #Ajout JFNGVS 24/02/2023 sourceType pour le type Point
    sourceType = indicateurSpec.get('sourceType',None)
    confDb = indicateurSpec.get('confDb',None)

    dfMeta = GeoDataFrame(columns = model)
    logging.info(f"generateValueBydims - model : {model}")
    logging.info(f"generateValueBydims - dfMeta.columns {dfMeta.columns}")
    logging.info(f"generateValueBydims - data.columns {data.columns}")
    if confDims is not None:
        isin_id_spatial = confDims.get('isin_id_spatial',None)
        isin_id_mesure = confDims.get('isin_id_mesure',None)
        if isin_id_spatial is not None:
            
            if isin_id_spatial == '*':
                logging.info(f"generateValueBydims - isin_id_spatial == '*' /// Compute all spatials dimension  ")
                pass
            else:
                spatials = spatials[spatials.id_spatial.isin(isin_id_spatial)]            
            # simplification du code. Moins euristique mais plus rapide et plus simple 
            #Ajout JFNGVS: 08/02/2023 ajout de level et upper_libelle
            if sourceType == 'Point':
                first_round = False
                data = data.drop(['index_right'],axis=1)
                for ids in isin_id_spatial:
                    if first_round:
                        data_sn = data.sjoin(spatials[spatials.id_spatial == ids],how="inner")
                        data_sn = data_sn.drop(['index_right'],axis=1)
                        data_s = GeoDataFrame(pd.concat([data_s,data_sn],ignore_index=True))
                    else:
                        data_s = data.sjoin(spatials[spatials.id_spatial == ids],how="inner")
                        data_s = data_s.drop(['index_right'],axis=1)
                        first_round = True
                data = ddg_from_geopandas(data_s,npartitions=1)
                dfMeta = data_s 
            else:
                logging.debug(f"generateValueBydims - NNNNNNNNNNNNNNNNNNN :{type(data)} {data.compute()}")
                data_splited = data.map_partitions(daskSplitGeomByAnother, iterables=(spatials[['id_spatial','level','upper_libelle','geometry']],"intersection"),meta=dfMeta)

                logging.debug(f"generateValueBydims - data_splited : {data_splited.compute()}")
                #data = client.persist(data_splited.repartition(npartitions=data.npartitions))
                logging.debug(f"generateValueBydims - data : {data.compute()}")
            
        else:
            logging.info(f"generateValueBydims - aucun id spatial n'est renseigné dans isin_id_spatial")

    
        if isin_id_mesure is not None:
            logging.info(f"generateValueBydims - mesures isin_id_mesure : {isin_id_mesure}")
            mesures = mesures[mesures.id_mesure.isin(isin_id_mesure)]         
            result = daskCalculateMesures(data,(mesures,individuSpec,indicateurSpec,nbchuncks))
            logging.debug(f"generateValueBydims - mesures - result: {result.compute()}")
            return result
        else:
            logging.info(f"generateValueBydims - isin_id_mesure est {isin_id_mesure}")
            return data.compute()

    logging.warning(f'generateValueBydims - confDims: {confDims} ')
    return dfMeta


def createBboxList(individuStatSpec, indicateurSpec):

    data_catalog_dir = getPaths().get('data_catalog_dir')

    index = individuStatSpec.get('indexRef',None)
    confDb =  indicateurSpec.get('confDb',None)
    #catalog = f"{data_catalog_dir}indicateurs_nc.yaml"
    catalog = f"{data_catalog_dir}{individuStatSpec.get('catalogUri',None)}"
    dataName = individuStatSpec.get('dataName',None)
    ext_table_name = individuStatSpec.get('theme',None)
    logging.info(f"dataname {dataName}")

    dataCatalog = getattr(open_catalog(catalog),dataName)
    geom = dataCatalog.describe().get('args').get('geopandas_kwargs').get('geom_col')
    geom = 'geom'
    table = dataCatalog.describe().get('args').get('table')
   # sql = f'select {index} as index from {table} group by "index"'
    #sql = f'select {index} as index,  from {table} group by "index"'

    
    data = dataCatalog.read().set_index(index).head()
    
    listIndex = data.index.unique().to_list()
    
    for i in listIndex:
        sql = f'select {index} as index, ST_Envelope(ST_UNION({geom})) as geometry from {table} where "{index}" = {i} group by "index"'
        dataCatalog = getattr(open_catalog(catalog),dataName)(sql_expr=sql)
        df = dataCatalog.read().set_index("index")
        logging.debug(f"df.columns {df.columns}")
        xmin, ymin, xmax, ymax = df.geometry.total_bounds
        bbox = [(xmin, ymin), (xmax, ymax)]
        sqlWhereBBOX = f"{getSqlWhereClauseBbox(bbox, geom,'3163','3163')} and {index} = '{i}'"
        logging.debug(f"sqlWhereBBOX {sqlWhereBBOX}")
        df["sql"] = sqlWhereBBOX    
    
    return df


def create_indicator(bbox, individuStatSpec, indicateurSpec, dims, geomfield='geometry', stepList=[1,2,3],indexListIndicator=None, sql_pagination='',indicateur_sql_flow=False,daskComputation=True):
    """
    Create an indicator by performing a series of operations based on the provided configuration.

    Args:
        bbox (tuple): The bounding box coordinates (xmin, ymin, xmax, ymax).
        individuStatSpec (dict): The configuration for the individual statistics.
        indicateurSpec (dict): The configuration for the indicator.
        dims (tuple): The spatial and measurement dimensions.
        geomfield (str, optional): The name of the geometry field. Defaults to 'geometry'.
        stepList (list, optional): The list of steps to perform. Defaults to [1,2,3].
        indexListIndicator (list, optional): The list of indicators to use. Defaults to None.
        sql_pagination (str, optional): The SQL pagination string. Defaults to ''.
        indicateur_sql_flow (bool, optional): Flag indicating whether to use SQL flow for the indicator. Defaults to False.
        daskComputation (bool, optional): Flag indicating whether to use Dask for computation. Defaults to True.

    Returns:
        None
    """
    # Function principale qui pilote l'ensemble des opérations suivante à partir des éléments de configuration fournis individuStatSpec, indicateurSpec en input
    # Step 1 (facultatif si la donnée indicateur est déjà créée) : créer la données indicateur. Croisement donnée individu source/indicateur
    # Step 2 (facultatif l'étape 1 est faite) : appliquer les dimensions spatiales et mesures.
    # Step 3 (facultatif) : persister les données en base Postgis.
    
    from dask.distributed import get_client
    client = get_client()
     #logging.info(f"Dask client : {client}")

    paths = getPaths()


    data_catalog_dir = paths.get('data_catalog_dir')
    commun_path  = paths.get('commun_path')
    user = getDbConnection().get('user')
    pswd = getDbConnection().get('pswd')
    host  = getDbConnection().get('host')
    db_traitement = getDbConnection().get('db_traitement')



    indexRef = individuStatSpec.get('indexRef',None)
    nbchuncks = individuStatSpec.get('nbchuncks',None)
    keepList = individuStatSpec.get('keepList',[]) + indicateurSpec.get('keepList',[])
    
    adaptingDataframe = indicateurSpec.get('adaptingDataframe',None)

    #result = gpd.GeoDataFrame(columns = columnList)
    if indicateurSpec is None:
        confDb = individuStatSpec.get('confDb',None)
        epsg = individuStatSpec.get('epsg','EPSG:3163')
    else:
        confDb = indicateurSpec.get('confDb',None)
        epsg = indicateurSpec.get('epsg','EPSG:3163')

    tableName = confDb.get('tableName',None)
    ext_table_name = individuStatSpec.get('theme',None)

    #Ajout JFNGVS 15/02/2023 strategy 'append' obligatoire si limit
    if "limit" in sql_pagination:
        confDb["strategy"] = 'append'
    dim_spatial,dim_mesure = dims
    
    if 1 not in stepList:
        logging.info("create_indicator: Pas d'etape 1")
        #print(f"{data_catalog_dir}indicateurs_nc.yaml")
        catalog = f"{data_catalog_dir}indicateurs_nc.yaml"
        dataName = f"{tableName}_{ext_table_name}"
        entryCatalog = getattr(open_catalog(catalog),dataName)
        selectString = entryCatalog.describe().get('args').get('sql_expr')
        geom = entryCatalog.describe().get('args').get('geopandas_kwargs').get('geom_col')
        
        if bbox is not None:
            #bbox ,  index = bbox
            xmin, ymin, xmax, ymax = bbox
            bbox = [(xmin, ymin), (xmax, ymax)]
            sql_with_where_clause = getSqlWhereClauseBbox(bbox,geom,"3163","3163")
            if selectString.find("where") >= 1 :
                sql = f'{selectString} and {sql_with_where_clause}'
            else:
                sql = f'{selectString} where {sql_with_where_clause}'
            #print(sql)
            data = entryCatalog(sql_expr=sql)
        else:
            data = entryCatalog
        
        if indexListIndicator:
            pass

        else:
            logging.info("create_indicator: Pas d'etape 1 et pas de bbox")
            #print("data",data)
            indicateur = data.read()
            
            if indicateur.shape[0] > 0 :
                indicateur.rename_geometry('geometry')
                #print('df.columns -->',df.columns)
                keepList = indicateur.columns.tolist()
                keepList =  [c for c in keepList if c not in [indexRef,'id_split','id_spatial','geometry']]
                #print("keepList",keepList)
                indicateur = indicateur[[indexRef] + keepList+['id_split','geometry']]
                indicateur = ddg_from_geopandas(indicateur,nbchuncks)

            else:
                pass

        
    else:
        # spec data
        logging.info("create_indicator: Etape 1")
        catalog = f"{data_catalog_dir}{individuStatSpec.get('catalogUri',None)}"
        catalogInd = f"{data_catalog_dir}{indicateurSpec.get('catalogUri',None)}"
        dataName = individuStatSpec.get('dataName',None)
        entryCatalog = getattr(open_catalog(catalog),dataName)
        selectString = individuStatSpec.get('selectString',entryCatalog.describe().get('args').get('sql_expr'))
        whereString = individuStatSpec.get('whereString',None)
        offset = individuStatSpec.get('offset',False)
        sourceType = indicateurSpec.get('sourceType',None)
        sourceTypeInd = individuStatSpec.get('sourceType',None)
        geom = entryCatalog.describe().get('args').get('geopandas_kwargs').get('geom_col')
        schema = confDb.get('schema',None)

        if bbox is not None:
            logging.info("create_indicator: Etape 1 avec bbox")
            #bbox ,  index = bbox
            xmin, ymin, xmax, ymax = bbox
            bbox = [(xmin, ymin), (xmax, ymax)]
            sql_whith_where_clause = getSqlWhereClauseBbox(bbox,geom,"3163","3163")
            if  selectString.find("where") >= 1 :
                sql = f'{selectString} and {sql_whith_where_clause}'           
            else :
                sql = f'{selectString} where  {sql_whith_where_clause}'
            #print(sql)
            data = entryCatalog(sql_expr=sql)
            
        else:
            logging.info("create_indicator: Etape 1 sans bbox")
            if selectString is not None or offset:
                if whereString is not None:
                    sql = f'{selectString} where {whereString}'
                else:
                    sql = f'{selectString}'
            
                data = entryCatalog(sql_expr=sql + ' ' + sql_pagination)
            else:
                data = entryCatalog()
        ###Ajout JFNGVS 24/02/2023 traitement simplement par point
        if sourceType == 'Point':
            logging.info("create_indicator: Etape 1: indicateur = 'Point'")
            dataName = indicateurSpec.get('dataName',None)
            data_indicateur = getattr(open_catalog(catalogInd),dataName)
            data =  data_indicateur.read()
            dtn_ind = individuStatSpec.get('dataName',None)
            print("dtn_ind",dtn_ind)
            print("catalogInd",catalogInd)
            print("catalog",catalog)
            data_indiv = getattr(open_catalog(catalog),dtn_ind)
            indicateur = data.sjoin(data_indiv.read(),how="left")

            
        else:
            if indicateurSpec.get('catalogUri') and indicateurSpec.get('dataName') is not None:
                logging.info("create_indicator: Etape 1 --> indicateurSpec.get('catalogUri') and indicateurSpec.get('dataName') is not None")
                metaModelList =  [indexRef] + keepList + ['geometry','id_split']
                
                logging.info(f"create_indicator: Etape 1 --> sourceType : {sourceType}")
                if sourceTypeInd == "Interpolation":
                
                    confInterpolation = indicateurSpec.get('confInterpolation',None)
                    interpolation = confInterpolation.get('interpolation',None)
                    interpolation_from_pre_interpolation = interpolation.get('from_pre_interpolation',None)
                    
                    allocate_total = interpolation.get('allocate_total',None)

                    conf_pre_interpolation = confInterpolation.get('pre_interpolation',None)

                    catalog = fixOsPath(f"{data_catalog_dir}{indicateurSpec.get('catalogUri',None)}", commun_path )
                    dataName = indicateurSpec.get('dataName',None)
                    
                    intensive_variables = confInterpolation.get('intensive_variables',None)
                    extensive_variables = confInterpolation.get('extensive_variables',None)
                    source_df = getattr(open_catalog(catalog),dataName).read()
                
                    
                    if conf_pre_interpolation and interpolation_from_pre_interpolation:
                                        
                        pre_interpolation = confInterpolation.get('pre_interpolation',None)
                    
                        pre_catalogUri = pre_interpolation.get('catalogUri',None)
                        pre_interpolate_dataName = pre_interpolation.get('target_df',None)
                        split_geometry = conf_pre_interpolation.get('split_geometry',None)
                        pre_allocate_total = conf_pre_interpolation.get('allocate_total',None)
                        
                        pre_catalog = fixOsPath(f"{data_catalog_dir}{pre_catalogUri}", commun_path )
                        target_entry = getattr(open_catalog(pre_catalog),pre_interpolate_dataName)
                        target_entry_geom = pre_catalog.describe().get('args').get('geopandas_kwargs').get('geom_col')

                        target_df = target_entry.read()
                        pre_int_keepList = pre_interpolation.get('keepList',None)
                        
                        pre_int_indexRef = pre_interpolation.get('indexRef',None)                    
                        keepList = keepList + pre_int_keepList + [f'{indexRef}_interpolation']
                        
                        metaModelList =  [indexRef] + keepList + [target_entry_geom,'id_split']
                        
                        interpolation = indicateur_from_pre_interpolation(source_df, target_df, intensive_variables,extensive_variables, pre_allocate_total, pre_int_keepList, pre_int_indexRef)
                        if split_geometry:
                            indicateur = parallelize_DaskDataFrame_From_Intake_Source(data,generateIndicateur_parallel,(indicateurSpec,individuStatSpec, interpolation, metaModelList, geom, target_entry_geom),(tableName,ext_table_name),metaModelList,nbchuncks=nbchuncks)                       
                        else:                        
                            indicateur = parallelize_DaskDataFrame_From_Intake_Source(data,indicateur_from_interpolation,(interpolation, intensive_variables,extensive_variables,allocate_total),(tableName,ext_table_name),metaModelList,nbchuncks=nbchuncks)
                    else:
                        
                        indicateur = parallelize_DaskDataFrame_From_Intake_Source(data,indicateur_from_interpolation,(source_df, intensive_variables,extensive_variables,allocate_total),(tableName,ext_table_name),metaModelList,nbchuncks=nbchuncks)
                    
                    
                    if isinstance(indicateur,(GeoDataFrame,DaskGeoDataFrame)):                
                        indicateur.set_crs(epsg, inplace=True)
                        print('indicateur Interpolation done. Indexing.....', indicateur.columns)
                        print("indicateur_generateIndicateur_Parallel", type(indicateur))
                    else:
                        stepList = []
                        print("No data to process")
                        pass
                        
                elif sourceTypeInd == "Raster":
                    logging.info(f"Raster keepList {keepList}")
                    logging.info(f"Raster metaModelList {metaModelList}")            
                    indicateur = parallelize_DaskDataFrame_From_Intake_Source(
                        data,indicateur_from_raster,
                        (indicateurSpec,individuStatSpec,epsg,metaModelList),
                        (tableName,ext_table_name),
                        metaModelList,
                        nbchuncks=nbchuncks)
                    if isinstance(indicateur,(GeoDataFrame,DaskGeoDataFrame))  :
                        logging.info('indicateur Raster done. Indexing.....')
                        logging.info(f"indicateur_generateIndicateur_Parallel {indicateur}")
                    else:
                        stepList = []
                        logging.info("No data to process")
                        pass
                
                else:
                    logging.info("source Type OTHER : ex . VECTOR ")
                    catalog = f"{data_catalog_dir}{indicateurSpec.get('catalogUri',None)}"
                    dataName = indicateurSpec.get('dataName',None)
                    data_indicateur = getattr(open_catalog(catalog),dataName)
                    data_indicator_geom = data_indicateur.describe().get('args').get('geopandas_kwargs').get('geom_col')
                    if not indicateur_sql_flow:
                        logging.info('Loading indicateur dataset ...')
                        data_indicateur = data_indicateur.read()
                        logging.info(f"{data_indicateur.shape[0]} entitées")

                    if sourceTypeInd == "Point":
                        metaModelList =  [indexRef] + keepList + ['geometry','id_split'] + ['nb']
                        indicateur = parallelize_DaskDataFrame_From_Intake_Source(
                            data,
                            generateIndicateurPoint,
                            (indicateurSpec, individuStatSpec, data_indicateur, metaModelList),
                            (tableName,ext_table_name),
                            metaModelList,
                            nbchuncks=nbchuncks
                        )
                    else :
                        logging.info('Calculation ...')
                        try:
                            if daskComputation:
                                logging.info(f"with Dask - metaModelList : ' {metaModelList}")
                                indicateur = parallelize_DaskDataFrame_From_Intake_Source(
                                    data,
                                    generateIndicateur_parallel_v2,
                                    (indicateurSpec, individuStatSpec, data_indicateur, metaModelList, geom, data_indicator_geom),
                                    (tableName,ext_table_name),
                                    metaModelList,
                                    nbchuncks=nbchuncks
                                )
                            else:
                                indicateur = generateIndicateur_parallel(data.read(),(indicateurSpec, individuStatSpec, data_indicateur, metaModelList, geom, data_indicator_geom))
                            
                            if isinstance(indicateur,(GeoDataFrame,DaskGeoDataFrame))  :
                                logging.info(f"Etape 1 - Result:  {type(indicateur)}")
                                #logging.info(f"Etape 1 - Result:  {client.compute(indicateur)}")
                                pass
                            else:
                                stepList = []
                                logging.info("No data to process")
                                pass
                        except Exception as e:
                            logging.info(f"calculation error: {e}")
                
                
            else:
                # l'indicateur est la donnée individu source directement
                indicateur = data.read()
                logging.info(f"indicateur au niveau 1  {indicateur}")
                if indicateur :
                    #indicateur.rename_geometry('geometry', inplace=True)
                    indicateur['id_split'] = indicateur[indexRef]
                else:
                    stepList = []
                        
                    logging.info("No data to process")
                    pass
    
    
    
    if 2 not in stepList:
        pass
    else:
        logging.info(f"create_indicator: Etape 2")
        logging.info(f"indexListIndicator {indexListIndicator}")
        if indexListIndicator:
            logging.info(f"create_indicator: Etape 2 --> indexListIndicator {indexListIndicator}")
            for indexIndicator in indexListIndicator :

                #print("indexRef",indexRef," indexIndicator",indexIndicator)
                sql_with_where_clause = f"{indexRef}::Text = '{indexIndicator}'::Text"
                if selectString.find("where") >= 1 :
                    sql = f'{selectString} and {sql_with_where_clause}'
                else:
                    sql = f'{selectString} where {sql_with_where_clause}'
                    
                #print(sql)
                chunk_dataframe = data(sql_expr=sql)
                #print(chunk_dataframe)
                chunk_dataframe = chunk_dataframe.read()
                chunk_dataframe.rename(columns ={geom:'geometry'})

                keepList = chunk_dataframe.columns.tolist()
                #print("keepList",keepList)
                #Ajout JFNGVS: 08/02/2023 ajout de level et upper_libelle
                keepList =  [c for c in keepList if c not in [indexRef,'id_split','id_spatial','level','upper_libelle','geometry']]
                #print("keepList",keepList)
                metaModelList =  [indexRef] + keepList +  ['id_split','id_spatial','level','upper_libelle','geometry']
                logging.info(f"Etape 2: metaModelList {metaModelList}")
                logging.info(f"Warning ajouter +['id_split','geometry'] si indicateur a ces champs...")
                indicateur = chunk_dataframe[[indexRef] + keepList]
                indicateurDask = ddg_from_geopandas(indicateur,nbchuncks)
                indicateurDask = generateValueBydims(indicateurDask,(individuStatSpec,indicateurSpec,dim_spatial,dim_mesure, metaModelList, nbchuncks))
                indicateurDask = client.persist(indicateurDask)
                indicateurDask = client.gather(client.compute(indicateurDask))
                results = client.submit(persistGDF, indicateurDask,(confDb,adaptingDataframe,individuStatSpec, epsg)).result()
                if 3 in stepList:
                    stepList.remove(3)
        else:
            logging.info(f"create_indicator: Etape 2 --> pas de indexListIndicator")
            logging.debug(f"create_indicator: Etape 2 -->  indicateur {indicateur.compute()}")
            #indicateur = client.submit(spatial_partitions, indicateur)
            fromIntake = True
            # spec indicateur
            logging.info(f"indicateurSpec {indicateurSpec}")
            confRatio = indicateurSpec.get('confRatio',None)
            metaModelList =  [indexRef] + keepList +  ['id_split','id_spatial','level','upper_libelle','geometry']
            logging.info(f"create_indicator: Etape 2 --> metaModelList {metaModelList}")
            try:
                logging.info(f"create_indicator: Etape 2 --> indicateur : {indicateur}")
                indicateur = generateValueBydims(indicateur,(individuStatSpec,indicateurSpec,dim_spatial,dim_mesure, metaModelList, nbchuncks))
                indicateur = client.persist(indicateur)
                results = client.compute(indicateur).result()
                logging.info(f"results : {results}")
            except Exception as e:
                logging.critical(f"generateValueBydims error more details : {e}")      

    if 3 not in stepList:
        return True
        pass
    else:
        logging.info(f"create_indicator: Etape 3 --> persisting in database: confDb {confDb}")
        try:
            logging.debug(f"create_indicator: Etape 3 --> Indicateur {indicateur}")
            #indicateur = client.gather(client.compute(indicateur))
            #results = client.submit(persistGDF, client.scatter(client.gather(client.compute(indicateur))),(confDb,adaptingDataframe,individuStatSpec, epsg))
            #client.compute(indicateur)
            dbEngineConnection = (user, pswd, host, db_traitement)
            results = client.submit(persistGDF, client.scatter(client.gather(client.compute(indicateur))),(confDb,adaptingDataframe,individuStatSpec, epsg, dbEngineConnection)).result()
            #Ajout JFNGVS 09/02/2023
            logging.info(f"create_indicator: Etape 3 --> Resultat {results}")
            ext_table_name = individuStatSpec.get('dataName',None)
            AjoutClePrimaire(schema,user, pswd, host, db_traitement, f"{tableName}_{ext_table_name}")
            return f"{tableName}_{ext_table_name}"
        except Exception as e:
            logging.critical(f"persistGDF error: {e}")
            return 0
