import geopandas
import ee
import geemap.foliumap as geemap
import geemap as gee

import numpy as np
import matplotlib.pyplot as plt
import folium
import requests
import rasterio
from rasterio.plot import show


#Pourquoi pas une liste de polygon en entrée ?
def extract_gee_data(spec_file_path: str, data):
    # découper chaque géométrie en sous géométrie ayant une valeur unique fournie par GEE
    gfc = ee.Image("UMD/hansen/global_forest_change_2021_v1_9")
    image = gfc.clip(data)
    bands_list = image.getInfo()['bands']
    band = bands_list[0]
    width = band['dimensions'][0]
    height = band['dimensions'][1]
    treecover = image.select(band['id'])
    scale = image.projection().nominalScale().getInfo()

    reduction = treecover.reduceToVectors(
        geometry=None,
        crs=image.projection(),
        scale=scale,
        geometryType='polygon',
        eightConnected=True,
        labelProperty='zone',
        maxPixels=1e15,
        tileScale=16
    )

    return data

    # OU

    # donner min, max, mean pour chaque géométrie donnée en entrée
    pass

service_account = 'ee-oeil@surfor.iam.gserviceaccount.com'
credentials = ee.ServiceAccountCredentials(service_account, 'surfor-8383e43c3aa7.json')
ee.Initialize(credentials)
print(ee.Image("UMD/hansen/global_forest_change_2021_v1_9"))

