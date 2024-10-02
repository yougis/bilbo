import os
import logging
from sqlalchemy import create_engine, Engine
from geopandas import GeoDataFrame, GeoSeries
from pandas import DataFrame, Series
from shapely.geometry import Polygon,MultiPolygon
from intake import entry
from intake import open_catalog

from oeilnc_utils.catalog import create_yaml_intake_catalog_from_dict


logging.info("Utils - Connection Imported")

def getEngine(user='usr', pswd='pswd', host='host', port=5432, dbase='oeil_traitement') -> Engine:
    """
    Returns a SQLAlchemy engine object for connecting to a PostgreSQL database.

    Args:
        user (str): The username for the database connection.
        pswd (str): The password for the database connection.
        host (str): The host address for the database connection.
        dbase (str): The name of the database to connect to.

    Returns:
        sqlalchemy.engine.Engine: The SQLAlchemy engine object.

    """
    connection = f'postgresql://{user}:{pswd}@{host}:{port}/{dbase}'
    return create_engine(connection)


def getSqlWhereClauseBbox(bbox, geom, bbox_crs="4326", geom_crs="3163") -> str: 
    """
    Generate a SQL WHERE clause for filtering geometries based on a bounding box.

    Args:
        bbox (tuple): A tuple containing the coordinates of the bounding box in the format ((latmin, lonmin), (latmax, lonmax)).
        geom (str): The name of the geometry column in the database.
        bbox_crs (str, optional): The CRS (Coordinate Reference System) of the bounding box coordinates. Defaults to "4326".
        geom_crs (str, optional): The CRS of the geometry column in the database. Defaults to "3163".

    Returns:
        str: The SQL WHERE clause for filtering geometries based on the bounding box.
    """
    (latmin, lonmin), (latmax, lonmax) = bbox
    #bbox_sql_where_clause = f"ST_Intersects(ST_Transform(ST_SetSRID({geom},3163)::geometry,{geom_crs}::int),ST_Transform(st_makeenvelope({latmin},{lonmin},{latmax},{lonmax}, {bbox_crs} )::geometry,{geom_crs}::int))"
    bbox_sql_where_clause = f"{geom} && ST_Transform(st_makeenvelope({latmin},{lonmin},{latmax},{lonmax}, {bbox_crs} )::geometry,{geom_crs}::int)"

    return bbox_sql_where_clause


def fixpath(path,replace,winDisque="N:"):
    logging.info("change uri to windows: Create Commun Path")
    path = path.replace(replace, '/')
    path = os.path.normpath(os.path.expanduser(path))
    if path.startswith("\\"): return winDisque + path
    return path


def fixOsPath(path, replace, win_disque="N:\\", lin_disque="/media/commun"):
    logging.info("fix_os_path : change linux uri to windows: Create Commun Path")
    
    path = path.replace(replace, os.path.sep)
    path = os.path.normpath(os.path.expanduser(path))
    
    if os.name == 'nt':
        if path.startswith(os.path.sep):
            return win_disque + path.lstrip(os.path.sep)
    elif os.name == 'posix':
        if path.startswith(os.path.sep):
            return os.path.join(lin_disque, path.lstrip(os.path.sep))
    
    return path

def AjoutClePrimaire(schem, user, pswd, host, dbase, tb):
    from sqlalchemy.sql import text
    """
    Adds a primary key column to a table in the specified schema.

    Args:
        schem (str): The name of the schema.
        user (str): The username for the database connection.
        mdp (str): The password for the database connection.
        hote (str): The host address of the database.
        db (str): The name of the database.
        tb (str): The name of the table.

    Returns:
        None
    
    Notes: Ajout JFNGVS 09/02/2023

    """

    engine = getEngine(user=user,pswd=pswd,host=host,dbase=dbase)
    sqlseqid = f"create sequence if not exists {schem}.{tb}_id_seq increment 1 start 1 minvalue 1 maxvalue 2147483647 cache 1;" 
    alter = f"alter sequence {schem}.{tb}_id_seq OWNER TO oeil_admin;"
    sqlid = f"alter table {schem}.{tb} add column if not exists id_fait numeric(9,0) not null default nextval('{schem}.{tb}_id_seq'::regclass)"
    sqlcontrainte = f"alter table {schem}.{tb} drop constraint if exists {tb}_pkey; alter table {schem}.{tb} add constraint {tb}_pkey primary key (id_fait);"
    

    with engine.connect() as connection:
        connection.execute(text(sqlseqid))
        connection.execute(text(alter))
        connection.execute(text(sqlid))
        connection.execute(text(sqlcontrainte))
        
    
    logging.info(f"cle primaire id_fait ajoutee sur {tb}")


    return True


def getNbLignes(source: entry.Catalog):


    tableName = source.describe().get('args').get('table')
    uri = source.describe().get('args').get('uri')
    sql_expr = f"SELECT COUNT(*) as nb FROM {tableName};"
    #dbConnection = settings.getDbConnection()
    source_config = {
        'sources': {
            'compte_entites': {
                'driver': 'sql',
                'metadata': {},  # Remplacez ceci par le nom correct du driver
                'args': {
                    'uri': uri,
                    'sql_expr': sql_expr,
                },
                'description': 'Compter le nombre d’entités sans charger les géométries',
            }
        }
    }
        
    reader = open_catalog(create_yaml_intake_catalog_from_dict(source_config))
    
    df = reader.compte_entites.read()    
    nbLignes = df.loc[0, 'nb']
    logging.info(f"{tableName} nblignes : {nbLignes}")
    return nbLignes

def adapting_dataframe(dd, adaptingDataframe):

    fillNanClasse = adaptingDataframe.get('fillNanClasse',None)
    toDrop = adaptingDataframe.get('toDrop',[])
    renameMap = adaptingDataframe.get('renameMap',{})
    setAllClasseValue = adaptingDataframe.get('setAllClasseValue',None)
    
    changeType = adaptingDataframe.get('changeType',{})

    setValue = adaptingDataframe.get('setValue',{})
    colNameValue = setValue.get('colName',None)
    value = setValue.get('value',None)

    if colNameValue:
        dd[colNameValue]=value
    

    if fillNanClasse:
        dd['classe'].fillna(fillNanClasse, inplace = True)


    if len(toDrop) > 0 :
        dd = dd.drop(toDrop, axis=1)
        
    
    if setAllClasseValue:
        dd['classe'] = setAllClasseValue
        
    if changeType :
        logging.info(f"changeType :  {changeType}")
        dd = dd.astype(changeType)
        logging.info(f"dtypes : {dd.dtypes}")   
    
    if len(renameMap.values()) > 0:
        logging.info(f"rename {renameMap}")
        dd = dd.rename(columns=renameMap)
    
    return dd

def persistGDF(gdf,iterables):
    logging.info("persistGDF")

    logging.debug(f"persistGDF - {type(gdf)} ")

    if gdf.shape[0] == 0:
        logging.warning(f"persistGDF - Le Dataframe est vide, on passe")
        return gdf
    confDb, individuStatSpec,epsg, dbEngineConnection = iterables
    user, pswd, host, dbase = dbEngineConnection
    tableName = confDb.get('tableName',None)
    ext_table_name = individuStatSpec.get('dataName',None)
    
    if isinstance(gdf, (GeoDataFrame, GeoSeries)) :
        logging.info(f"CRS transformation from {gdf.crs} to {epsg} ?")
        if gdf.crs != epsg:
            gdf.set_crs(epsg, allow_override=True)
            if gdf.crs == '-1':
                gdf.set_crs(epsg, allow_override=True)
        #gdf.to_crs(epsg, inplace=True)

    elif isinstance(gdf, (DataFrame, Series)):
        logging.warning(f"persistGDF - Vous essayer de persister un donnée sans géométrie: {gdf.shape[0]} -  {gdf}")
        # Convertir les chaînes de caractères en tuples de clé et d'indice
    
    else :
        logging.warning(f"persistGDF - Vous essayer de persister un donnée de type -  {type(gdf)}")
        return gdf
    


    if tableName:
        
        schema = confDb.get('schema',None)
        strategy = confDb.get('strategy',None)
        chunksize = confDb.get('chunksize',None )
        
        
        
        
        
        logging.info(f"{tableName} to postgis {gdf.shape[0]} entities and columns {gdf.columns}")
        
        try:
            gdf["geometry"] = [MultiPolygon([feature]) if type(feature) == Polygon else feature for feature in gdf["geometry"]]
            tableName = f'{tableName}_{ext_table_name}'
            
            gdf.to_postgis(tableName,getEngine(user=user,pswd=pswd,host=host,dbase='oeil_traitement'), schema=schema,if_exists=strategy, chunksize=chunksize)
            logging.info(f"import postgis finish")
            del gdf
            return f"gdf - {tableName}"
        except Exception as e:
            logging.critical(f"{tableName}_withError to postgis {e}")
            if not isinstance( gdf , GeoDataFrame):
                logging.critical(f"{type(gdf)} n'est pas un GeoDataFrame ",e)
            gdf = GeoDataFrame(gdf)
            gdf.to_postgis(f"{tableName}_withError",getEngine(user=user,pswd=pswd,host=host,dbase="oeil_traitement"), schema=schema,if_exists=strategy, chunksize=chunksize)        
            return gdf

    else:
        logging.critical("Il faut renseigner un nom de table dans confDb!")
        return False
