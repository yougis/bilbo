import logging

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%d/%m/%Y %H:%M:%S', level=logging.DEBUG)
logging.info("Utils - Dataframe Imported")


def filterDF(ddf,attribut, inv):
    if inv:
        return ddf[~ddf[attribut]]
    else:
        return ddf[ddf[attribut]]