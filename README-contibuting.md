
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


``` 
poetry shell
```

``` 
poetry install
```

### Construire l'environnement conda qui permet de créer le fichier environnement.yaml

créer un environnement conda vierge.

installer les librairies à partir du fichier requirements.txt


### Déployer les dépendances dans un environnement conda 

Créer l'environnement à partir du fichier environment.yml
`conda env create -f environment.yml`

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

**Générer le fichier requierement.txt**

```
poetry export
```

ce fichier pourra ensuite être exploité par pip pour installer les dependances via la commande `pip install -r requierements.txt`


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

### Configugrer le debugeur python VSCODE 

Pour débuger le traitement fait par un worker, configurer un executeur debug dans le launch.json:

```
{
    "version": "0.2.0",
    "configurations": [
        {
            "name": "Debug Dask Worker PROD",
            "type": "debugpy",
            "request": "launch",
            "module": "dask.distributed.cli.dask_worker",
            "args": [
                "172.20.12.13:9786", // Adresse du scheduler
                "--nthreads=1",
                "--preload",
                "{path-to-dev-package}/bilbo-packages/oeilnc_config/preload_worker.py"
            ],
            "console": "integratedTerminal"
        },
        {
            "name": "Debug Dask Worker QUALIF",
            "type": "debugpy",
            "request": "launch",
            "module": "dask.distributed.cli.dask_worker",
            "args": [
                "172.20.12.11:8786", // Adresse du scheduler
                "--nthreads=1",
                "--preload",
                "{path-to-dev-package}/bilbo-packages/oeilnc_config/preload_worker.py"
            ],
            "console": "integratedTerminal"
        }


    ]
}

       

```