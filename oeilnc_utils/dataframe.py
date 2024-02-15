def filterDF(ddf,attribut, inv):
    if inv:
        return ddf[~ddf[attribut]]
    else:
        return ddf[ddf[attribut]]