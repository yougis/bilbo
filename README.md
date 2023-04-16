# Bilbo Packages

## Objectifs

Ce package a pour ambition de regrouper l'ensemble des fonctions utiles au traitement de la donnée au sein de l'Oeil.
Il se découpe selon plusieurs packages :
- generate_indicator : permet de récupérer de la donnée à partir d'une source définie (Google Earth Engine, ...)
- ...

Les scripts spécifiques à chaque projet seront par conséquent de taille très réduite car l'ensemble des traitements utilisera ces packages.

## Contribuer

### Pipenv

Pour contribuer au package vous pouvez utiliser l'environnement virtuel créé par pipenv.
Pour installer pipenv, il suffit de faire :
```powershell
# installer d'abord pipenv
pip install pipenv

# création de l'environnement virtuel (depuis la racine du projet)
python -m pipenv install
```
Dans le prompt vous verrez le nom de l'environnement virtuel créé (bilbo_packages_<sha>). Vous pourrez directement l'utiliser comme Kernel Jupyter dans VSCode.

### Conda

Pour contribuer au package, il faut installer les packages suivants dans votre envrionnement Conda :
```bash
conda install -c conda-forge earthengine-api numpy geemap rasterstats geopandas rasterio matplotlib plotly
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