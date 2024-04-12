import os
import logging

logging.info("Utils - File management Imported")



def exist_file(chemin_fichier):
    """
    Vérifie si un fichier existe à l'emplacement spécifié.

    Args:
        chemin_fichier (str): Le chemin complet du fichier à vérifier.

    Returns:
        bool: True si le fichier existe, False sinon.
    """
    return os.path.exists(chemin_fichier)