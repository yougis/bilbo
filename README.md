# Bilbo Packages

## Objectifs

Ce package a pour ambition de regrouper l'ensemble des fonctions utiles au traitement de la donnée au sein de l'Oeil.
Il se découpe selon plusieurs packages préfixé `oeilnc_` pour les retrouver ensemble si on liste les package d'un environnement:
- **oeilnc_config**
  Ce package contient la configuration globale de l'application, incluant le chargement des variables d'environnement et des paramètres de configuration nécessaires au bon fonctionnement de l'ensemble des composants.
  - `__init__.py`: Fichier nécessaire pour faire du dossier un package Python.
  - `settings.py`: Contient les configurations et les variables d'environnement utilisées par l'application.

- **oeilnc_geoindicator**
  Ce package est dédié à la création, au calcul, et à la manipulation des indicateurs géospatiaux. Il englobe tout ce qui est nécessaire pour traiter des données géographiques et générer des indicateurs.
  - `__init__.py`: Fichier nécessaire pour faire du dossier un package Python.
  - `calculation.py`: Fournit des fonctions pour calculer des indicateurs basés sur des données géospatiales.
  - `distribution.py`: Contient des méthodes pour distribuer le calcul des indicateurs sur différents nœuds ou processus.
  - `gee_credentials.json`: Contient les credentials pour accéder à Google Earth Engine.
  - `gee.py`: Intègre les fonctionnalités de Google Earth Engine pour le traitement d'images satellites et la génération d'indicateurs.
  - `geometry.py`: Offre des fonctions pour la manipulation et le traitement de données géométriques.
  - `interpolation.py`: Fournit des méthodes pour interpoler les valeurs entre les points de données spatiales.
  - `raster.py`: Contient des fonctions pour manipuler des données raster, y compris la lecture, l'écriture et le traitement de ces données.

- **oeilnc_utils**
  Regroupe un ensemble de fonctions utilitaires qui peuvent être utilisées à travers le projet pour des tâches communes comme la connexion à des bases de données, la manipulation de dataframes, etc.
  - `__init__.py`: Fichier nécessaire pour faire du dossier un package Python.
  - `connection.py`: Gère les connexions aux bases de données.
  - `dataframe.py`: Offre des fonctions pour manipuler des dataframes, notamment pour leur nettoyage, leur transformation, et leur agrégation.
  - `geometry.py`: Propose des outils pour la manipulation de données géométriques, souvent complémentaires à ceux dans `oeilnc_geoindicator`.
  - `raster.py`: Fournit des outils pour la manipulation de données raster, pouvant inclure des fonctionnalités telles que le re-échantillonnage ou le calcul de statistiques sur des images raster.

Les scripts spécifiques à chaque projet seront par conséquent de taille très réduite car l'ensemble des traitements utilisera ces packages.

Variables d'environnement utilisées dans certaines méthodes

```bash

'SCHEDULER_IP' = "172.20.12.13:9787"
'COMMUN_PATH' = "/media/commun/commun/"
'ARCHIVE_PATH' = "media/archive/"
'PATH_INFOCENTRE_APP' = ${COMMUN_PATH}Informatique/SIG/Application/Jupyterhub/
'PROJECT_PATH' = ${PATH_INFOCENTRE_APP}projets/${PROJECT_ID}/
'DATA_CATALOG_DIR' = ${PATH_INFOCENTRE_APP}projets/catalogFiles/ 
'DATA_CONFIG_DIR' = ${DATA_CATALOG_DIR}data_config_files/
'DATA_OUTPUT_DIR' = ${PROJECT_PATH}output/
'SIG_DATA_PATH' = ${COMMUN_PATH}Informatique/SIG/Donnees/
'STAC_CATALOG_PATH' =  ${ARCHIVE_PATH}STAC/
'DIM_CATALOG_DIR' = ${PATH_INFOCENTRE_APP}projets/catalogFiles/

```


## Déployer


Déployer une branche du repot git sur le scheduler : remplacer [nom_de_la_branche]

`conda run --name gis311_base pip install --force-reinstall --upgrade --exists-action=w  "git+https://informatique:rxf4qdzjc5pccj2423ycuedtyma3ughg6e2oepohoc7oilbbjukq@dev.azure.com/Oeilnc/Bilbo/_git/bilbo-packages@[nom_de_la_branche]"`

le déploiement se fait en mode --quiet
pour voir ce qui se passe activer l'environnement conda 
`conda activate gis311_base`

Lancer :

`pip install --force-reinstall --upgrade --exists-action=w  "git+https://informatique:rxf4qdzjc5pccj2423ycuedtyma3ughg6e2oepohoc7oilbbjukq@dev.azure.com/Oeilnc/Bilbo/_git/bilbo-packages@[nom_de_la_branche]`

ex sur la branche "refactoring_from_bilbo"
`pip install --force-reinstall --upgrade --exists-action=w  "git+https://informatique:rxf4qdzjc5pccj2423ycuedtyma3ughg6e2oepohoc7oilbbjukq@dev.azure.com/Oeilnc/Bilbo/_git/bilbo-packages@refactoring_from_bilbo"`


`pip install --force-reinstall --upgrade --exists-action=w  "git+https://informatique:rxf4qdzjc5pccj2423ycuedtyma3ughg6e2oepohoc7oilbbjukq@dev.azure.com/Oeilnc/Bilbo/_git/bilbo-packages@refactoring_from_bilbo#oeilnc_geoindicator"`

`pip install --force-reinstall --upgrade --exists-action=w  "git+https://informatique:rxf4qdzjc5pccj2423ycuedtyma3ughg6e2oepohoc7oilbbjukq@dev.azure.com/Oeilnc/Bilbo/_git/bilbo-packages@refactoring_from_bilbo#oeilnc_utils"`

`pip install --force-reinstall --upgrade --exists-action=w  "git+https://informatique:rxf4qdzjc5pccj2423ycuedtyma3ughg6e2oepohoc7oilbbjukq@dev.azure.com/Oeilnc/Bilbo/_git/bilbo-packages@refactoring_from_bilbo#oeilnc_config"`

## Contribuer

### Poetry

Pour contribuer au package vous pouvez utiliser l'environnement virtuel créé par poetry.
Pour installer poetry, il suffit de faire :
```powershell
# installer 
curl -sSL https://install.python-poetry.org | python3 -

```
#### Commande utiles


```
poetry add ${package-name}
```

```
poetry update
```

```
poetry lock
```

### Conda

Pour contribuer au package, il faut installer les packages suivants dans votre environnement Conda :
```bash
conda install -c conda-forge earthengine-api numpy geemap rasterstats geopandas rasterio matplotlib plotly
```
Le dossier `generate_indicator` contient les méthodes permettant de récupérer des données provenant de catalogue Google Earth Engine.
Différents exemples illustrent son utilisation dans le dossier `examples`.

## Utiliser

Pour utiliser ce package dans votre projet, il suffit de l'installer via le repo Git.
```bash
pip install git+https://Oeilnc@dev.azure.com/Oeilnc/Bilbo/_git/bilbo-packages
```

Cependant, cette installation n'inclus pas les dépendances pourtant spécifiées dans le fichier `setup.py`. 
Tant qu'une solution n'a pas été trouvée pour automatiquement installer les dépendances, ces dernières devront être installées manuellement :
```bash
conda install -c conda-forge earthengine-api numpy geemap rasterstats geopandas
```

Une fois installées, on peut utiliser le package :

```python
from generate_indicator import gee
gee.extract_data()
```



