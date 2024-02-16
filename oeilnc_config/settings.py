
from os import getenv
import logging
from intake import open_catalog
import yaml

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%d/%m/%Y %H:%M:%S', level=logging.INFO)

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

schedulerIp = getenv("SCHEDULER_IP")

if schedulerIp is None:
    logging.critical(f"La variable d'environnement SCHEDULER_IP doit être renseignée pour effectuer les traitements de manière distribuée")


def initializeBilboProject():

    global commun_path
    global data_catalog_dir 
    global project_dir
    global data_output_dir
    global sig_data_path
    global project_db_schema
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



def getIndexList(individuStatSpec):
    index = individuStatSpec.get('indexRef',None)
    #catalog = f"{data_catalog_dir}indicateurs_nc.yaml"
    catalog = f"{data_catalog_dir}{individuStatSpec.get('catalogUri',None)}"
    dataName = individuStatSpec.get('dataName',None)
    print("dataname",dataName)

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
