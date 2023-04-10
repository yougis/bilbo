import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name='generate_indicator',
    version='0.1.1',
    author='Clément Niot',
    author_email='',
    description="Contient l'ensemble des packages nécessaires au traitement de la donnée pour l'Oeil",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url='https://dev.azure.com/Oeilnc/Bilbo/_git/bilbo-packages',
    license='OEIL.NC',
    packages=['generate_indicator'],
    install_requires=['geopandas', 'geemap', 'rasterstats', 'earthengine-api', 'numpy']
)
