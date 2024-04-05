from datetime import datetime
import uuid
import yaml
import logging
import os

current_date = datetime.now().strftime("%Y-%m-%d_%H-%M")
current_directory = os.getcwd()
log_dir = 'logs'

try:
    os.mkdir(os.path.join(current_directory,log_dir))
    print("Folder %s created!" % log_dir)
except FileExistsError:
    print("Folder %s already exists" % log_dir)

log_filename = os.path.join(current_directory, log_dir, f"{current_date}-bilbo-processing.log")

logging.basicConfig( filename= f"{log_filename}",format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%d/%m/%Y %H:%M:%S', level=logging.DEBUG)



from oeilnc_config import settings
from oeilnc_utils import connection
from oeilnc_geoindicator.calculation import create_indicator
from intake import open_catalog
from oeilnc_config.metadata import ProcessingMetadata

from dask.distributed import WorkerPlugin

class CustomPlugin(WorkerPlugin):
    def start(self, worker):
        print(f"Worker {worker.address} connected to the scheduler.")
        client = settings.getDaskClient(local=True)
        configFile = settings.initializeBilboProject('.env')
        client.run(settings.initializeWorkers, configFile)
        # Insérer ici la commande que vous souhaitez exécuter sur le worker

    


list_data_to_calculate = [
    #"H3_6_NC_non_traite",
    #"H3_6_NC_non_traite_2010",
    #"H3_6_NC",
    #"MOS_formation_vegetale_DAFE_missing",
    #"MOS_formation_vegetale_DAFE",
    #"Foncier",
    "Reserves_indicateurSpec",
    "UNESCO_Zones_terrestres",
    "PerimetresProtectionEau", # mettre jour la donnée
    "BassinsVersantsProducteursEP",
    "Foret_Seche_zone_vigilance",
    "Foret_Seche_corridors",
    "Mangrove",
    # Incendies #VIIRS et MODIS
    "SurfacesAgricoles",
    #"exploitation_miniere", # fichier config à faire / cadastre minier ? > sur georep / cadastre_minier / couches "concessions minières" et "permis de recherche"  (pas sur la BDD SIG de l'ŒIL)
    "Geologie_Substrat",
    # Urbanisation à créer Dynamic world built area
    # Cyclone à créer > sur georep / bd cyclones / historique des trajectoires
    # Zone de réhabilitation de sites miniers > BDD RECOSYNTH et données FNI?--> A réccupérer
    # données Sylviculture de vulcain // différe légerement du MOS 2014
    # Sècheresse végétale : VHI/VAI annualisé ?

# Fragmentation : pas de donnée
    # Secteurs de mise en défens (FS et mine)

# Fréquentation humaine :  Zone de Bati et voiries / calcul de distance ?
# Dynamique ripisylve : > Modelisation écoulement hydro DAVAR. Les données sont accéssibles ici, je ne les ai pas trouvée sur la BD SIG de l'ŒIL: https://carto.gouv.nc/public/rest/services/modelisation_ecoulements_surface/MapServer. Il faut a mon avis considérer uniquement la couche Réseau Hydrographique / RHM seuil5km2. 

    "bande_littorale_375M_individuStatSpec" #Dynamique trait de côte


]

steplist= [1,2,3]  # 1 : generate indicators by spatial intersection (interpolation/raster/vector)/ 2: spliting byDims & calculate ratio... / 3: persist
list_indicateur_to_calculate = [
    #"KBA",
    #"observation_nidification"
    #"GFC_gain_2012",
    #"GFC_treecover2000"
    #"MOS_arbore_indicateurSpec",
    #"MOS_formation_arboree",
    #"MOS_formation_arbustive",
    #'Foncier',
    'Test'

    ]

listIdSpatialNC = ["0"]
listIdSpatialCommune = [
    "98827", "98817", "98813", "98826", "98819", "98816", "98828", "98824", "98832", "98821", 
    "98831", "98818", "98804", "98822", "98814", "98815", "98806", "98809", "98820", "98811", 
    "98807", "98805", "98803", "98823", "98810", "98812", "98833", "98808", "98825", "98801", 
    "98830", "98802", "98829"
]

Poya = ["98827"]
listIdSpatialCommuneMaritime = [
    "MONT_DORE", "LA_FOA", "POUM", "OUEGOA", "MOINDOU", "SARRAMEA", "POUEBO", "YATE", "PAITA", 
    "VOH", "NOUMEA", "CANALA", "POINDIMIE", "LIFOU", "MARE", "FARINO", "ILE_DES_PINS", "OUVEA", 
    "KONE", "POYA", "HIENGHENE", "DUMBEA", "BOURAIL", "PONERIHOUEN", "KAALA_GOMEN", "KOUMAC", 
    "KOUAOUA", "HOUAILOU", "POUEMBOUT", "BELEP", "TOUHO", "BOULOUPARI", "THIO"
]

listIdSpatialProvinceMaritime = ['PROVINCE_SUD_MAR', 'PROVINCE_NORD_MAR', 'PROVINCE_ILES_MAR']
listIdSpatialProvince = ["1", "2", "3"]
listIdSpatialHER = ["C", "F", "G", "B", "D", "A", "E"]
listIdSpatialRegHydro = [
    "0100", "0200", "0300", "0400", "0505", "0600", "0700", "0800", "0900", "1000", "1100", 
    "1400", "1500", "1600", "1700", "1800", "1900", "2000", "2100", "2200", "2300", "2400", 
    "2500", "2700", "2800", "2900", "3100", "3200", "3300", "3400", "3500", "3600", "3700", 
    "3800", "3900", "4000", "4100", "4300", "4400", "4500", "4600", "4700", "4800", "4900", 
    "5000", "5100", "5200", "5300", "5500", "5600", "5700", "5800", "5900", "6000", "6100", 
    "6200", "6400", "6500", "6600", "6700", "6800", "6900", "7000", "7100", "7200", "7300", 
    "7400", "7500", "7700", "7800", "7900", "8000", "8100", "8200", "8300", "8400", "8500", 
    "8600", "8700", "8800", "8900", "9000", "9100", "9200", "0301", "4200", "2600", "0302", 
    "0501", "2602", "0502", "5400", "1602", "0503", "0504", "0500", "1601", "2601", "2603", 
    "3000", "7600", "6300", "1200", "1300"
]

listIdSpatialAiresCoutumieres = [
    "AJIE-ARO", "DJUBEA-KAPONE", "DREHU", "HOOT MA WHAAP", "IAII", "NENGONE", "PAICI-CAMUKI", "XARACUU"
]
listIdSpatialZee = ["ZEE"]

listIdSpatialCommuneIles = ["98814", "98815", "98820"]
listIdSpatialCommuneMaritimeIles = ["LIFOU", "MARE", "OUVEA"]
listIdSpatialProvinceIles = ["3"]
listIdSpatialProvinceMaritimeIles = ["PROVINCE_DES_ILES_MAR"]
listIdMulti=[listIdSpatialNC, listIdSpatialHER, listIdSpatialProvince, listIdSpatialCommune ]
listbbox= [c for c in listIdSpatialCommune]



client = settings.getDaskClient()
#client = Client()
configFile = settings.initializeBilboProject('.env')

# Attacher le plugin au client
client.register_plugin(CustomPlugin())

# Maintenant, lorsqu'un worker se connecte, la fonction start du plugin sera appelée
# et vous pouvez exécuter votre commande personnalisée dans cette fonction




bboxing = False #par emprise communale
bb=None
fromIndexList=False
steplist= [1,2,3]# 1 : generate indicators / 2: spliting byDims & calculate ratio... / 3: persist
info_integration = False
sql_pagination = ""
indicateur_sql_flow=True # si Vrai, attention à ne pas depasser le nombre de connection postgres maximales avec la somme de chunck de l'ensemble du cluster
daskComputation=True
faits = list()
theme=configFile.get('project_db_schema')


cat_dimensions = open_catalog(f"{configFile.get('dimension_catalog_dir')}DWH_Dimensions.yaml")

dim_spatial = cat_dimensions.dim_spatial
dim_mesure= cat_dimensions.dim_mesure

run_id = str(uuid.uuid4())

for dataFileName in list_data_to_calculate:
    logging.info(f"### {dataFileName} ####")
    with open(f"{configFile.get('data_config_file')}{dataFileName}.yaml", 'r') as file:        
        individuStatSpec = yaml.load(file, Loader=yaml.Loader)
        individuStatSpec["theme"]=theme
        individuStatSpec["confDims"]["isin_id_spatial"] = []

        offset = individuStatSpec.get("offset", -1)
        limit = individuStatSpec.get("limit", -1)
        logging.info(f"Initial offset : {offset} , limit : {limit}")

        logging.info(f"step list : {steplist}")

        if fromIndexList:            
            indexList = settings.getIndexList(individuStatSpec)
            pass
        for listIdSpatial in listIdMulti:       
            individuStatSpec["confDims"]["isin_id_spatial"] += listIdSpatial            
        
        logging.info(f"Id Spatial qui seront calculés : {individuStatSpec['confDims']['isin_id_spatial']}")
            
        if len(list_indicateur_to_calculate) > 0:
            for indicateurFileName in list_indicateur_to_calculate:             
                logging.info(f"--- {indicateurFileName} ---")
                
                with open(f"{configFile.get('data_config_file')}{indicateurFileName}.yaml", 'r') as file:
                    indicateurSpec = yaml.load(file, Loader=yaml.Loader)
                    indicateurSpec["confDb"]["schema"] = theme

                    logging.info(f"individu: {dataFileName} | indicateur: {indicateurFileName}")


                    if not fromIndexList:
                        indexList= None

                    if settings.checkConfig(indicateurSpec,individuStatSpec) :                                        
                        logging.info(f"nbchuncks: {individuStatSpec.get('nbchuncks','aucun')}")

                        if offset >= 0 or limit > 0:        
                            #Ajout JFNGVS = boucle sur limit et offset
                            catalog = f"{configFile.get('data_catalog_dir')}{individuStatSpec.get('catalogUri',None)}"

                            dataName = individuStatSpec.get('dataName',None)
                            entryCatalog = getattr(open_catalog(catalog),dataName)
                            selectString = individuStatSpec.get('selectString',entryCatalog.describe().get('args').get('sql_expr'))
                            indexRef = individuStatSpec.get('indexRef',None)
                            nbLignes = connection.getNbLignes(entryCatalog)

                            

                            while offset < nbLignes:
                                
                                sql_pagination = f"order by {indexRef} limit {limit} offset {offset}"
                                logging.info(f"sql_pagination : {sql_pagination}")
                                
                                print("Go")

                                client.run(settings.initializeWorkers, configFile)

                                metadata = ProcessingMetadata(run_id=run_id)
                                metadata.environment_variables = configFile
                                metadata.output_schema = configFile.get('project_db_schema')
                                metadata.operator_name = configFile.get('user')
                                metadata.log_filename = log_filename
                                metadata.zoi_config = individuStatSpec
                                metadata.dimensions_spatiales = individuStatSpec["confDims"]["isin_id_spatial"]
                                metadata.theme_config = indicateurSpec
                                metadata.zoi_catalog = entryCatalog

                                faitsname = create_indicator(
                                    bbox=bb,
                                    individuStatSpec=individuStatSpec,
                                    indicateurSpec=indicateurSpec,
                                    dims=(dim_spatial,dim_mesure),
                                    stepList=steplist,
                                    indexListIndicator=indexList,
                                    sql_pagination=sql_pagination,
                                    indicateur_sql_flow=indicateur_sql_flow,
                                    daskComputation=daskComputation,
                                    metadata=metadata)
                                
                                metadata.output_table_name = faitsname
                                metadata.offset_value = offset
                                metadata.limit_value = limit
                                metadata.insert_metadata()

                                offset += limit
                    else : 
                        pass
                    

    