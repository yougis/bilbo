# Bilbo Packages

## Qu'est-ce que Bilbo ?

Bilbo est l'accronyme de Buisness Intelligence for Land and Biodiversity Observation. C'est un projet de recherche et développement qui vise à développer des outils d'analyse de données pour l'observation de la terre. 

![](ressources/schema_chaine_traitement.png)


## Objectifs

Ce package a pour ambition de regrouper l'ensemble des fonctions utiles au traitement de la donnée au sein de l'Oeil.
Il se découpe selon plusieurs packages préfixé `oeilnc_` pour les retrouver ensemble si on liste les packages d'un environnement Python:
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

```sh

'SCHEDULER_IP' = "*.*.*.*:****"
'COMMUN_PATH' = "path/to/files¨
'ARCHIVE_PATH' = "path/to/archive"
```


## Déployer

considérant que le nom de l'environnement conda est **gis311** : 

```
conda run --name gis311 pip install --force-reinstall --upgrade --exists-action=w  "git+https://user:******q@repo/_git/bilbo-packages"
```

le déploiement se fait en mode *--quiet*
pour voir ce qui se passe activer d'abord l'environnement conda 
`conda activate gis311`

et lancer pip directement (non recommandée):


`pip install --force-reinstall --upgrade --exists-action=w   "git+https://user:******q@repo/_git/bilbo-packages"

cela va mettre à jour les versions des librairies dépendantes.



### Déployer sans installer les dépendances

De nombreuses dépendances sont indiquées dans le fichier .toml 
Cela peut être utile de ne pas vouloir réinstaller toutes les dépendances à chaque déploiement. Pour cela il faut ajouter `--no-deps`

`conda run --name gis311_base pip install --force-reinstall --upgrade --exists-action=w   "git+https://user:******q@repo/_git/bilbo-packages" --no-deps`


### Déployer un sous package uniquement
Pour déployer uniquement un sous packages  il faut ajouter indiquer # et remplacer [nom_du_sous_package]

```
pip install --force-reinstall --upgrade --exists-action=w   "git+https://user:******q@repo/_git/bilbo-packages#[nom_du_sous_package]"
```

**Quelques exemples** 
```
pip install --force-reinstall --upgrade --exists-action=w   "git+https://user:******q@repo/_git/bilbo-packages@refactoring_from_bilbo#oeilnc_geoindicator"
```

`pip install --force-reinstall --upgrade --exists-action=w  "git+https://user:******q@repo/_git/bilbo-packages@refactoring_from_bilbo#oeilnc_utils"`

`pip install --force-reinstall --upgrade --exists-action=w   "git+https://user:******q@repo/_git/bilbo-packages@refactoring_from_bilbo#oeilnc_config"`


#### troubleshooting

Sur windows la commande git n'aboutit jamais car une popup credential manager n'est pas renvoyé si la commande est faite depuis un protocole SSH. il faut lancer la commande git sur le poste et activer l'option de ne plus demander l'autorisation. 

parfois, bien que SSH soit installé sur windows, il faut redemarer le protocole pour pouvoir se connecter au machine
`Restart-Service sshd`

### Déployer sur le **Scheduler**
Le scheduler utilise un environnement conda "light" suffixé de  *_base*.

```
conda run --name gis311_base pip install --force-reinstall --upgrade --exists-action=w   "git+https://user:******q@repo/_git/bilbo-packages@dev.azure.com/Oeilnc/Bilbo/_git/bilbo-packages"
```

## Déployer à partir d'une branche du repot git sur le scheduler
Pour déployer les packages à partir d'une branche, il faut ajouter @ et remplacer [nom_de_la_branche]

`conda run --name gis311 pip install --force-reinstall --upgrade --exists-action=w   "git+https://user:******q@repo/_git/bilbo-packages@[nom_de_la_branche]"`

