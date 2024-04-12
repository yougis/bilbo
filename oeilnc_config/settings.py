
import logging
from intake import open_catalog
import yaml
from dotenv import load_dotenv
from dask.distributed import Client, LocalCluster, Variable
import os


logging.info("Config - Settings Imported")
configFile = Variable(name="configFile")


usr = os.getenv("DB_USER")
pswd = os.getenv("DB_PWD")
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")

home = os.getenv("HOME_PATH")
db_traitement = os.getenv("DB_WORKSPACE")
db_ref = os.getenv("DB_REF")
db_externe = os.getenv("DB_EXT")

commun_path = os.getenv("COMMUN_PATH")
project_dir = os.getenv("PROJECT_PATH")
data_catalog_dir = os.getenv("DATA_CATALOG_DIR")
data_output_dir = os.getenv("DATA_OUTPUT_DIR")
sig_data_path = os.getenv("SIG_DATA_PATH")
project_db_schema = os.getenv("PROJECT_DB_SCHEMA")
data_config_file = os.getenv("DATA_CONFIG_DIR")
dimension_catalog_dir = os.getenv("DIM_CATALOG_DIR")

null_variables = []
if commun_path is None:
    null_variables.append("commun_path")
if project_dir is None:
    null_variables.append("project_dir")
if data_catalog_dir is None:
    null_variables.append("data_catalog_dir")
if data_output_dir is None:
    null_variables.append("data_output_dir")
if sig_data_path is None:
    null_variables.append("sig_data_path")
if project_db_schema is None:
    null_variables.append("project_db_schema")

if null_variables:
    logging.warning("The following variables are null: {}".format(", ".join(null_variables)))

config_dict = {
    "user": usr,
    "pswd": pswd,
    "host": host,
    "port": port,
    "home": home,
    "db_traitement": db_traitement,
    "db_ref": db_ref,
    "db_externe": db_externe,
    "commun_path": commun_path,
    "project_dir": project_dir,
    "data_catalog_dir": data_catalog_dir,
    "data_output_dir": data_output_dir,
    "sig_data_path": sig_data_path,
    "project_db_schema": project_db_schema,
    "data_config_file": data_config_file,
    "dimension_catalog_dir": dimension_catalog_dir
}

def initializeBilboProject(dotenvPath=None):
    load_dotenv(dotenv_path=dotenvPath)

    global commun_path
    global data_catalog_dir 
    global project_dir
    global data_output_dir
    global sig_data_path
    global project_db_schema
    global data_config_file
    global dimension_catalog_dir
    global usr
    global pswd
    global host
    global port
    global db_traitement

    commun_path = os.getenv("COMMUN_PATH")
    project_dir = os.getenv("PROJECT_PATH")
    data_catalog_dir = os.getenv("DATA_CATALOG_DIR")
    data_output_dir = os.getenv("DATA_OUTPUT_DIR")
    sig_data_path = os.getenv("SIG_DATA_PATH")
    project_db_schema = os.getenv("PROJECT_DB_SCHEMA")
    data_config_file = os.getenv("DATA_CONFIG_DIR")
    dimension_catalog_dir = os.getenv("DIM_CATALOG_DIR")


    usr = os.getenv("DB_USER")
    pswd = os.getenv("DB_PWD")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")

    home = os.getenv("HOME_PATH")
    db_traitement = os.getenv("DB_WORKSPACE")
    db_ref = os.getenv("DB_REF")
    db_externe = os.getenv("DB_EXT")


    null_variables = []
    if commun_path is None:
        null_variables.append("commun_path")
    if project_dir is None:
        null_variables.append("project_dir")
    if data_catalog_dir is None:
        null_variables.append("data_catalog_dir")
    if data_output_dir is None:
        null_variables.append("data_output_dir")
    if sig_data_path is None:
        null_variables.append("sig_data_path")
    if project_db_schema is None:
        null_variables.append("project_db_schema")
    if data_config_file is None:
        null_variables.append("data_config_file")

    if null_variables:
        logging.critical("The following variables are null: {}".format(", ".join(null_variables)))
        return False
    
    
    
    config_dict = {
        "user": usr,
        "pswd": pswd,
        "host": host,
        "port": port,
        "home": home,
        "db_traitement": db_traitement,
        "db_ref": db_ref,
        "db_externe": db_externe,
        "commun_path": commun_path,
        "project_dir": project_dir,
        "data_catalog_dir": data_catalog_dir,
        "data_output_dir": data_output_dir,
        "sig_data_path": sig_data_path,
        "project_db_schema": project_db_schema,
        "data_config_file": data_config_file,
        "dimension_catalog_dir": dimension_catalog_dir
    }

    return config_dict

def getDbConnection():

    logging.info(f"getDbConnection - {usr} {host} {port} {db_traitement}")
    return {
       "user": usr,
        "pswd": pswd,
        "host": host,
        "port": port,
        "db_traitement": db_traitement

    }

def getPaths():
    data_config_file = configFile.get()
    logging.info(f'Settings - getPaths data_config_file {data_config_file} ')

    return data_config_file



def getDaskClient(local=False):
    global client

    def createClient(schedulerIp="172.20.12.13:9786"):
        client = Client(schedulerIp)
        return client

    if local :
        # Démarrer un cluster local avec 4 cœurs
        cluster = LocalCluster(n_workers=2)

        client = Client(cluster)
        return client


    if 'client' in globals():   
        if client.scheduler != None:
        # La variable client existe dans l'espace de noms global
            return client
        else:
            schedulerIp = os.getenv("SCHEDULER_IP")
            if schedulerIp:
                client = createClient(schedulerIp)
            else:
                client = createClient(schedulerIp)
                
    else:
        schedulerIp = os.getenv("SCHEDULER_IP")
        if schedulerIp:
            client = createClient(schedulerIp)
        else:
            client = createClient()

    return client


def getIndexList(individuStatSpec):
    """
    Get the list of unique index values from the specified data catalog.

    Parameters:
    - individuStatSpec (dict): A dictionary containing the specifications for the individual statistics.

    Returns:
    - list: A list of unique index values from the data catalog.
    """
    index = individuStatSpec.get('indexRef',None)
    catalog = f"{data_catalog_dir}{individuStatSpec.get('catalogUri',None)}"
    dataName = individuStatSpec.get('dataName',None)
    dataCatalog = getattr(open_catalog(catalog),dataName)   
    data = dataCatalog.read().set_index(index)    
    listIndex = data.index.unique().to_list()

    return listIndex



def checkListToCalculate(list_indicateur_to_calculate, list_data_to_calculate, listIdMulti):
    for dataFileName in list_data_to_calculate:
        try:
            with open(f'{data_catalog_dir}data_config_files/{dataFileName}.yaml', 'r') as file:        
                individuStatSpec = yaml.load(file, Loader=yaml.Loader)
                print(dataFileName)            
                for listIdSpatial in listIdMulti:
                    individuStatSpec["confDims"]["isin_id_spatial"]= listIdSpatial
                    print("listIdSpatial",listIdSpatial)
                    for indicateurFileName in list_indicateur_to_calculate:
                        print("indicateurFileName",indicateurFileName)
        except FileNotFoundError:
            error_msg = f'Le fichier {dataFileName}.yaml n\'a pas été trouvé dans {data_catalog_dir}data_config_files/.'
            logging.error(error_msg)
            print(error_msg)  # Optionnel : affiche l'erreur aussi dans la console

    return True


def checkTableName(indicateurSpec,individuStatSpec):
    tableName = indicateurSpec.get("confDb").get("tableName")
    dataName = individuStatSpec.get("dataName",'Unknown')
    index_tb = f'idx_{tableName}_{dataName}_geometry'
    tb_name_with_error = f'faits_{tableName}_{dataName}_withError'
    if len(index_tb) > 63:
        logging.critical(f"le nom de l'index sur postgis est trop long. Veuillez reduire le nom de la table de faits: {index_tb}")
    elif len(tb_name_with_error) > 63:
        logging.warning(f"le nom de la table sur postgis est trop long. Veuillez reduire le nom de la table de faits: {tb_name_with_error}")
        return True
    else:
        logging.info(f"le nom de la table de faits en base de donnée en sortie de traitement répond au pré-requis : {tableName}_{dataName}")
        return True

def checkConfigFiles(list_data_to_calculate, configFile, list_indicateur_to_calculate):
    from oeilnc_utils import connection


    for dataFileName in list_data_to_calculate:
        logging.info(f"### {dataFileName} ####")
        with open(f"{configFile.get('data_config_file')}{dataFileName}.yaml", 'r') as file:        
            individuStatSpec = yaml.load(file, Loader=yaml.Loader)

            offset = individuStatSpec.get("offset", -1)
            limit = individuStatSpec.get("limit", -1)
            logging.info(f"Initial offset : {offset} , limit : {limit}")
                   
            if len(list_indicateur_to_calculate) > 0:
                for indicateurFileName in list_indicateur_to_calculate:             
                    logging.info(f"--- {indicateurFileName} ---")
                    
                    try :
                        with open(f"{configFile.get('data_config_file')}{indicateurFileName}.yaml", 'r') as file:
                            indicateurSpec = yaml.load(file, Loader=yaml.Loader)
                            logging.info(f"individu: {dataFileName} | indicateur: {indicateurFileName}")


                            if checkTableName(indicateurSpec,individuStatSpec) :                                        
                                logging.info(f"nbchuncks: {individuStatSpec.get('nbchuncks','aucun')}")

                                catalog = f"{configFile.get('data_catalog_dir')}{individuStatSpec.get('catalogUri',None)}"

                                dataName = individuStatSpec.get('dataName',None)
                                entryCatalog = getattr(open_catalog(catalog),dataName)
                                selectString = individuStatSpec.get('selectString',entryCatalog.describe().get('args').get('sql_expr'))
                                indexRef = individuStatSpec.get('indexRef',None)
                                nbLignes = connection.getNbLignes(entryCatalog)

                                print("Go")

                                if offset >= 0 or limit > 0:    

                                    while offset < nbLignes:

                                                                            
                                        sql_pagination = f"order by {indexRef} limit {limit} offset {offset}"
                                        logging.info(f"sql_pagination : {sql_pagination}")


                                        offset += limit
                    except Exception as e:
                        logging.critical(e)


