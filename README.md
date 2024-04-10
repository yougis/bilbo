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

considérant que le nom de l'environnement conda est **gis311** : 

```
conda run --name gis311 pip install --force-reinstall --upgrade --exists-action=w  "git+https://informatique:rxf4qdzjc5pccj2423ycuedtyma3ughg6e2oepohoc7oilbbjukq@dev.azure.com/Oeilnc/Bilbo/_git/bilbo-packages"
```

le déploiement se fait en mode *--quiet*
pour voir ce qui se passe activer d'abord l'environnement conda 
`conda activate gis311`

et lancer pip directement :

`pip install --force-reinstall --upgrade --exists-action=w  "git+https://informatique:rxf4qdzjc5pccj2423ycuedtyma3ughg6e2oepohoc7oilbbjukq@dev.azure.com/Oeilnc/Bilbo/_git/bilbo-packages"`


### Déployer sans installer les dépendances

De nombreuses dépendances sont indiquées dans le fichier .toml 
Cela peut être utile de ne pas vouloir réinstaller toutes les dépendances à chaque déploiement. Pour cela il faut ajouter `--no-deps`

`conda run --name gis311_base pip install --force-reinstall --upgrade --exists-action=w  "git+https://informatique:rxf4qdzjc5pccj2423ycuedtyma3ughg6e2oepohoc7oilbbjukq@dev.azure.com/Oeilnc/Bilbo/_git/bilbo-packages" --no-deps`


### Déployer un sous package uniquement
Pour déployer uniquement un sous packages  il faut ajouter indiquer # et remplacer [nom_du_sous_package]

```
pip install --force-reinstall --upgrade --exists-action=w  "git+https://informatique:rxf4qdzjc5pccj2423ycuedtyma3ughg6e2oepohoc7oilbbjukq@dev.azure.com/Oeilnc/Bilbo/_git/bilbo-packages@refactoring_from_bilbo#[nom_du_sous_package]"
```

**Quelques exemples** 
```
pip install --force-reinstall --upgrade --exists-action=w  "git+https://informatique:rxf4qdzjc5pccj2423ycuedtyma3ughg6e2oepohoc7oilbbjukq@dev.azure.com/Oeilnc/Bilbo/_git/bilbo-packages@refactoring_from_bilbo#oeilnc_geoindicator"
```

`pip install --force-reinstall --upgrade --exists-action=w  "git+https://informatique:rxf4qdzjc5pccj2423ycuedtyma3ughg6e2oepohoc7oilbbjukq@dev.azure.com/Oeilnc/Bilbo/_git/bilbo-packages@refactoring_from_bilbo#oeilnc_utils"`

`pip install --force-reinstall --upgrade --exists-action=w  "git+https://informatique:rxf4qdzjc5pccj2423ycuedtyma3ughg6e2oepohoc7oilbbjukq@dev.azure.com/Oeilnc/Bilbo/_git/bilbo-packages@refactoring_from_bilbo#oeilnc_config"`


#### troubleshooting

Sur windows la commande git n'aboutit jamais car une popup credential manager n'est pas renvoyé si la commande est faite depuis un protocole SSH. il faut lancer la commande git sur le poste et activer l'option de ne plus demander l'autorisation. 

parfois, bien que SSH soit installé sur windows, il faut redemarer le protocole pour pouvoir se connecter au machine
`Restart-Service sshd`

### Déployer sur le **Scheduler**
Le scheduler utilise un environnement conda "light" suffixé de  *_base*.

```
conda run --name gis311_base pip install --force-reinstall --upgrade --exists-action=w  "git+https://informatique:rxf4qdzjc5pccj2423ycuedtyma3ughg6e2oepohoc7oilbbjukq@dev.azure.com/Oeilnc/Bilbo/_git/bilbo-packages"
```

## Déployer à partir d'une branche du repot git sur le scheduler
Pour déployer les packages à partir d'une branche, il faut ajouter @ et remplacer [nom_de_la_branche]

`conda run --name gis311 pip install --force-reinstall --upgrade --exists-action=w  "git+https://informatique:rxf4qdzjc5pccj2423ycuedtyma3ughg6e2oepohoc7oilbbjukq@dev.azure.com/Oeilnc/Bilbo/_git/bilbo-packages@[nom_de_la_branche]"`

## Développer et contribuer

Pour développer des packages Python avec Visual Studio Code (VSCode) à partir d'une source Git clonée localement et gérer les dépendances avec **Poetry**.
Vous pourez ensuite lancer les tests unitaires de manière à ce que l'interpréteur Python trouve les fichiers sources locaux.

### Installer et configurer Poetry

Pour contribuer au package vous pouvez utiliser l'environnement virtuel créé par poetry.
Pour installer poetry, il suffit de faire :
```shell
# installer 
curl -sSL https://install.python-poetry.org | python3 -

```

Une fois installé, ouvrez un terminal dans VSCode (Terminal > Nouveau terminal) et naviguez vers le dossier de votre projet si ce n'est pas déjà fait. Ensuite, configurez Poetry pour le projet :


exemple de sortie 
```
hugo@RSIdebian:~/projets/bilbo-packages$ poetry shell
Spawning shell within /home/hugo/.cache/pypoetry/virtualenvs/bilbo-packages-fNViwJlA-py3.9
. /home/hugo/.cache/pypoetry/virtualenvs/bilbo-packages-fNViwJlA-py3.9/bin/activate
(base) hugo@RSIdebian:~/projets/bilbo-packages$ . /home/hugo/.cache/pypoetry/virtualenvs/bilbo-packages-fNViwJlA-py3.9/bin/activate
(bilbo-packages-py3.9) (base) hugo@RSIdebian:~/projets/bilbo-packages$ poetry install
Installing dependencies from lock file

No dependencies to install or update

Installing the current project: bilbo-packages (0.0.4)
```

``` 
poetry shell
```

ex. de retour : 

hugo@RSIdebian:~/projets/bilbo-packages$ 

``` 
poetry install
```

### Configurer l'interpréteur Python dans VSCode

Assurez-vous que l'extension Python est installée dans VSCode. Ensuite, configurez VSCode pour utiliser l'interpréteur Python créé par Poetry :

Ouvrez la palette de commandes `Ctrl+Shift+P`.
Tapez "Python: Select Interpreter" et sélectionnez-le.
Choisissez l'interpréteur qui correspond à l'environnement virtuel de votre projet Poetry. Il devrait être nommé quelque chose comme Python 3.x ('nom_du_projet-py3.x': poetry).

**Vous ne trouvez pas l'interpreteur**

Utilisez la commande `poetry env list`

Si vous voyez que l'environnement virtuel créé par Poetry est listé et activé (comme indiqué par "Activated") mais que vous ne le voyez pas dans VSCode lorsque vous essayez de sélectionner l'interpréteur Python, voici quelques étapes supplémentaires à considérer pour résoudre ce problème :

Ajoutez le chemin vers le dossier contenant les environnements virtuels de Poetry. Vous pouvez trouver ce chemin en exécutant `poetry env info -p`

#### Commande Poetry utiles

**Ajouter une dépendance supplémentaire au projet**
```
poetry add ${package-name}
```

**Ajouter une dépendance supplémentaire au projet**

```
poetry update
```

**Ajouter toutes les dépendance dans le fichier lock**

```
poetry lock
```

**Exécuter les tests**

nécéssite le package pytest. 
```
poetry run pytest
```

### Changer la version de Python utilisée par Poetry 
Pour changer la version de Python utilisée par Poetry, vous pouvez suivre ces étapes :

1. **Vérifier les versions de Python disponibles :**
   Tout d'abord, vérifiez les versions de Python disponibles sur votre système en utilisant la commande :
   ```
   poetry env list
   ```

2. **Ajouter une nouvelle version de Python :**
   Si la version de Python que vous souhaitez utiliser n'est pas déjà installée, vous pouvez l'ajouter avec Poetry. Par exemple, pour ajouter Python 3.9, utilisez la commande :
   ```
   poetry env use 3.11
   ```

3. **Changer la version de Python :**
   Si la version que vous souhaitez utiliser est déjà installée, vous pouvez simplement basculer vers cette version en utilisant la commande :
   ```
   poetry env use <version>
   ```
   Remplacez `<version>` par la version de Python que vous souhaitez utiliser, par exemple `3.11`.

4. **Mettre à jour le fichier de verrouillage des dépendances :**
   Après avoir changé la version de Python, vous devriez mettre à jour le fichier `pyproject.toml` pour refléter la nouvelle version de Python. Cela garantira que les autres utilisateurs de votre projet ou votre système de construction utilisent la même version de Python. Assurez-vous de spécifier la nouvelle version dans la section `[tool.poetry]` du fichier `pyproject.toml`.

5. **Reconstruire l'environnement :**
   Une fois que vous avez modifié la version de Python dans votre projet Poetry, reconstruisez l'environnement virtuel avec la nouvelle version en utilisant la commande :
   ```
   poetry install
   ```

Si malgrès ces étapes, python n'est pas à la version la plus récente (il peut y avoir un décalage entre les repo utilisés par poetry et conda), il faut faire ceci :
```
poetry shell
```

pour python 3.11.8
```
conda install python=3.11.8
```


## Developper et évaluer sur l'infrastructure clusterisée

Lorsqu'on développe un nouvelle version du package vous pourriez rencontrer des erreurs sans quelles soient veritablement explicite :
ex : 

| RuntimeError: Error during deserialization of the task graph. This frequently
| occurs if the Scheduler and Client have different environments.
| For more information, see
| https://docs.dask.org/en/stable/deployment-considerations.html#consistent-software-environments

Assurez vous d'avoir des environnements identiques entre les workers , le scheduler et le client.
cela signifie que lorsque vous souhaitez tester un nouveau module sur le cluster, il faut penser à déployer les librairies de la branche en cours de développement. Pour cela le script deploy.sh va nous aider à faire ce travail dans les différents environnement (qualif et production):

pour rappel, la documentation se trouve  [ici dans le projet backup](https://dev.azure.com/Oeilnc/Backup)


ex. 
- installer la dernière version de generate_indicator sur l'environnement de qualification sur les machines du cluster: 
```
./deploy.sh conda --hosts 172.20.12.14,172.20.12.15,172.20.12.16,172.20.12.17 --packages git+https://informatique:rxf4qdzjc5pccj2423ycuedtyma3ughg6e2oepohoc7oilbbjukq@dev.azure.com/Oeilnc/Bilbo/_git/bilbo-packages@[branche-name]

```