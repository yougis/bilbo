# Bilbo Packages

## Objectifs

Ce package a pour ambition de regrouper l'ensemble des fonctions utiles au traitement de la donnée au sein de l'Oeil.
Il se découpe selon plusieurs packages :
- generate_indicator : permet de récupérer de la donnée à partir d'une source définie (Google Earth Engine, ...)
- ...

Les scripts spécifiques à chaque projet seront par conséquent de taille très réduite car l'ensemble des traitements utilisera ces packages.

## Contribuer

Pour contribuer au package, il suffit d'utiliser, soit l'envrionnement Conda fourni dans le fichier `environment.yml`, soit d'installer les packages utilisés :
```bash
conda install -c conda-forge earthengine-api numpy geemap rasterstats geopandas rasterio matplotlib
```
Le dossier `generate_indicator` contient les méthodes permettant de récupérer des données provenant de catalogue Google Earth Engine.
Différents exemples illustrent son utilisation dans le dossier `examples`.

## Utiliser

Pour utiliser ce package dans votre projet, il suffit de l'installer via le répo Git.
```bash
pip install git+https://Oeilnc@dev.azure.com/Oeilnc/Bilbo/_git/bilbo-packages
```

Cependant, cette installation n'inclus pas les dépdendances pourtant spécifiées dans le fichier `setup.py`. 
Tant qu'une solution n'a pas été trouvée pour automatiquement installer les dépendances, ces dernières devront être installées manuellement :
```bash
conda install -c conda-forge earthengine-api numpy geemap rasterstats geopandas
```

Une fois installées, on peut utiliser le package :

```python
from generate_indicator import gee
gee.extract_data()
```