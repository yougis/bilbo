
from os import getenv
import logging
from intake import open_catalog
import yaml
from dask.distributed import Client
from dotenv import load_dotenv
from dask.distributed import Client
from datetime import datetime
import os


current_date = datetime.now().strftime("%Y-%m-%d_%H-%M")
current_directory = os.getcwd()
log_filename = os.path.join(current_directory, f"{current_date}-bilbo-processing.log")
logging.basicConfig( filename= f"{log_filename}",format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%d/%m/%Y %H:%M:%S', level=logging.DEBUG)

logging.info("Config - Settings Imported")



usr = getenv("DB_USER")
pswd = getenv("DB_PWD")
host = getenv("DB_HOST")
port = getenv("DB_PORT")

home = getenv("HOME_PATH")
db_traitement = getenv("DB_WORKSPACE")
db_ref = getenv("DB_REF")
db_externe = getenv("DB_EXT")

commun_path = getenv("COMMUN_PATH")
project_dir = getenv("PROJECT_PATH")
data_catalog_dir = getenv("DATA_CATALOG_DIR")
data_output_dir = getenv("DATA_OUTPUT_DIR")
sig_data_path = getenv("SIG_DATA_PATH")
project_db_schema = getenv("PROJECT_DB_SCHEMA")

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

def initializeWorkers(config_dict: dict):

    global commun_path
    global data_catalog_dir 
    global project_dir
    global data_output_dir
    global sig_data_path
    global project_db_schema
    global data_config_file
    global dimension_catalog_dir
    global user
    global pswd
    global host
    global port
    global home
    global db_traitement
    global db_ref
    global db_externe
 
    user = config_dict.get("user")
    pswd = config_dict.get("pswd")
    host = config_dict.get("host")
    port = config_dict.get("port")
    home = config_dict.get("home")
    db_traitement = config_dict.get("db_traitement")
    db_ref = config_dict.get("db_ref")
    db_externe = config_dict.get("db_externe")
    commun_path = config_dict.get("commun_path")
    project_dir = config_dict.get("project_dir")
    data_catalog_dir = config_dict.get("data_catalog_dir")
    data_output_dir = config_dict.get("data_output_dir")
    sig_data_path = config_dict.get("sig_data_path")
    project_db_schema = config_dict.get("project_db_schema")
    data_config_file = config_dict.get("data_config_file")
    dimension_catalog_dir = config_dict.get("dimension_catalog_dir")

    return

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
    global user
    global pswd
    global host
    global port

    commun_path = getenv("COMMUN_PATH")
    project_dir = getenv("PROJECT_PATH")
    data_catalog_dir = getenv("DATA_CATALOG_DIR")
    data_output_dir = getenv("DATA_OUTPUT_DIR")
    sig_data_path = getenv("SIG_DATA_PATH")
    project_db_schema = getenv("PROJECT_DB_SCHEMA")
    data_config_file = getenv("DATA_CONFIG_DIR")
    dimension_catalog_dir = getenv("DIM_CATALOG_DIR")


    user = getenv("DB_USER")
    pswd = getenv("DB_PWD")
    host = getenv("DB_HOST")
    port = getenv("DB_PORT")

    home = getenv("HOME_PATH")
    db_traitement = getenv("DB_WORKSPACE")
    db_ref = getenv("DB_REF")
    db_externe = getenv("DB_EXT")


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
        return
    
    config_dict = {
        "user": user,
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

    return {
       "user": user,
        "pswd": pswd,
        "host": host,
        "port": port,
        "db_traitement": db_traitement

    }

def getPaths():

    logging.info(f'Settings - getPaths data_config_file {data_config_file} ')
    
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
    if dimension_catalog_dir is None:
        null_variables.append("dimension_catalog_dir")

    if null_variables:
        logging.critical("The following variables are null: {}".format(", ".join(null_variables)))
    
    conf = {
        "commun_path": commun_path,
        "project_dir": project_dir,
        "data_catalog_dir": data_catalog_dir,
        "data_output_dir": data_output_dir,
        "sig_data_path": sig_data_path,
        "project_db_schema": project_db_schema,
        "data_config_file": data_config_file,
        "dimension_catalog_dir": dimension_catalog_dir

    }
    logging.debug(f'Settings - getPaths - config {conf}')

    return conf

def getDaskClient():
    global client
    #client = Client()
    if 'client' in globals():

        # La variable client existe dans l'espace de noms global
        return client
    else:
        schedulerIp = getenv("SCHEDULER_IP")

        if schedulerIp is None:
            logging.warning(f"La variable d'environnement SCHEDULER_IP doit être renseignée pour effectuer les traitements de manière distribuée")
            schedulerIp = "172.20.12.13:9786"
            logging.info(f"on applique cette ip par défaut : {schedulerIp}")

        #client = Client()
        client = Client(schedulerIp)
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


def checkConfig(indicateurSpec,individuStatSpec):
    tableName = indicateurSpec.get("confDb").get("tableName")
    dataName = individuStatSpec.get("dataName",'Unknow')
    index_tb = f'idx_{tableName}_{dataName}_geometry'
    tb_name_with_error = f'faits_{tableName}_{dataName}_withError'
    if len(index_tb) > 63:
        logging.critical(f"le nom de l'index sur postgis est trop long. Veuillez reduire le nom de la table de faits: {index_tb}")
    elif len(tb_name_with_error) > 63:
        logging.warning(f"le nom de la table sur postgis est trop long. Veuillez reduire le nom de la table de faits: {tb_name_with_error}")
        return True
    else:
        logging.info(f"le nom de la table de faits en base de donnée en sortie de traitement répond au pré-requis : faits_{tableName}_{dataName}")
        return True