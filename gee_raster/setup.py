import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name='gee_raster',
    version='0.0.1',
    author='Clément Niot',
    author_email='',
    description='Testing installation of Package',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url='https://dev.azure.com/Oeilnc/Bilbo/_git/bilbo-packagesx',
    license='OEIL.NC',
    packages=['gee_raster'],
    install_requires=['geopandas', 'geemap', 'rasterstats', 'ee', 'numpy'],
)
