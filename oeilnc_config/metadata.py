import yaml
import pandas as pd
from datetime import datetime
from oeilnc_utils.connection import getEngine
import uuid
import logging
import json
import importlib.metadata

DB_META_SCHEMA = 'bilbo'

class ProcessingMetadata:
    """
    Represents the metadata for a processing operation.

    Attributes:
        output_schema (str): The output schema name.
        output_table_name (str): The output table name.
        operator_name (str): The name of the operator.
        zoi_config (str): The configuration for the zone of interest.
        zoi_catalog (str): The catalog for the zone of interest.
        theme_config (str): The configuration for the theme.
        theme_catalog (str): The catalog for the theme.
        limit_value (int): The limit value.
        offset_value (int): The offset value.
        environment_variables (str): The environment variables.
        dimensions_spatiales (str): The spatial dimensions.
        execution_time (str): The execution time. Automatic calculation when persiting in database
        log_file_name (str): The log file name.

    Exemple: 
    # Utilisation de la classe ProcessingMetadata
    metadata = ProcessingMetadata(output_schema='schema_name', output_table_name='table_name', operator_name='John Doe',
                               zoi_config={"key": "value"}, zoi_catalog={"key": "value"},
                               theme_config={"key": "value"}, theme_catalog={"key": "value"},
                               limit_value=100, offset_value=0,
                               environment_variables={"key": "value"},
                               dimensions_spatiales=["dimension1", "dimension2"],
                               log_file_name='example.log')

    # Modification d'une propriété à l'aide d'un setter
    metadata.output_schema = 'new_schema_name'

    # Affichage de la propriété modifiée
    print(metadata.output_schema)

    # Persit des metadata en base de donnée  
    metadata.insert_metadata()
    """

    def __init__(self, output_schema, output_table_name, operator_name, zoi_config, zoi_catalog, 
                 theme_config, theme_catalog, limit_value, offset_value, environment_variables, 
                 dimensions_spatiales, log_file_name):
        self._id= str(uuid.uuid4())
        self._execution_date = datetime.now()
        self._output_schema = output_schema
        self._output_table_name = output_table_name
        self._operator_name = operator_name
        self._zoi_config = zoi_config
        self._zoi_catalog = zoi_catalog
        self._theme_config = theme_config
        self._theme_catalog = theme_catalog
        self._limit_value = limit_value
        self._offset_value = offset_value
        self._environment_variables = environment_variables
        self._dimensions_spatiales = dimensions_spatiales
        self._execution_time = None
        self._log_file_name = log_file_name
        self._engine =  getEngine(
            user = self.environment_variables.get('user'),
            pswd = self.environment_variables.get('pswd'),
            host = self.environment_variables.get('host')
            )
        self._biblo_version= importlib.metadata.version('bilbo-packages')

    @property
    def id(self):
        return self._id
    
    @id.setter
    def id(self, value):
        self._id = value

    @property
    def execution_date(self):
        return self._execution_date

    @property
    def output_schema(self):
        return self._output_schema

    @output_schema.setter
    def output_schema(self, value):
        self._output_schema = value

    @property
    def output_table_name(self):
        return self._output_table_name

    @output_table_name.setter
    def output_table_name(self, value):
        self._output_table_name = value

    @property
    def operator_name(self):
        return self._operator_name

    @operator_name.setter
    def operator_name(self, value):
        self._operator_name = value

    @property
    def zoi_config(self):
        return self._zoi_config

    @zoi_config.setter
    def zoi_config(self, value):
        self._zoi_config = value

    @property
    def zoi_catalog(self):
        return self._zoi_catalog

    @zoi_catalog.setter
    def zoi_catalog(self, value):
        self._zoi_catalog = value

    @property
    def theme_config(self):
        return self._theme_config

    @theme_config.setter
    def theme_config(self, value):
        self._theme_config = value

    @property
    def theme_catalog(self):
        return self._theme_catalog

    @theme_catalog.setter
    def theme_catalog(self, value):
        self._theme_catalog = value

    @property
    def limit_value(self):
        return self._limit_value

    @limit_value.setter
    def limit_value(self, value):
        self._limit_value = value

    @property
    def offset_value(self):
        return self._offset_value

    @offset_value.setter
    def offset_value(self, value):
        self._offset_value = value

    @property
    def environment_variables(self):
        return self._environment_variables
    
    @property
    def environment_variables_nopwd(self):
        nopwd= self._environment_variables
        nopwd['pswd'] = "****"
        return nopwd
    
    

    @environment_variables.setter
    def environment_variables(self, value):
        self._environment_variables = value

    @property
    def dimensions_spatiales(self):
        return self._dimensions_spatiales

    @dimensions_spatiales.setter
    def dimensions_spatiales(self, value):
        self._dimensions_spatiales = value

    @property
    def execution_time(self):
        return self._execution_time

    @execution_time.setter
    def execution_time(self, value):
        self._execution_time = value

    @property
    def log_file_name(self):
        return self._log_file_name

    @log_file_name.setter
    def log_file_name(self, value):
        self._log_file_name = value

    @property
    def engine(self):
        return self._engine


    def get_metadata_by_id(self, id):
        table = "bilbo.processing_metadata"
        requete_sql = f"SELECT * FROM {table} WHERE id='{id}'"
        print(requete_sql)
        df = pd.read_sql_query(requete_sql, self.engine)
        return df


    def insert_metadata(self):

        self.execution_time = datetime.now() - self.execution_date

        # Establish a connection to the PostgreSQL database
        metadata = {
            'id': self.id,
            'execution_date': datetime.now(),
            'output_schema': self.output_schema,
            'output_table_name': self.output_table_name,
            'operator_name': self.operator_name,
            'zoi_config': json.dumps(self.zoi_config),
            'zoi_catalog': json.dumps(self.zoi_catalog._yaml()),
            'theme_config': json.dumps(self.theme_config),
            'theme_catalog': json.dumps(self.theme_catalog._yaml()),
            'limit_value': self.limit_value,
            'offset_value': self.offset_value,
            'environment_variables': json.dumps(self.environment_variables_nopwd),
            'dimensions_spatiales': self.dimensions_spatiales,
            'execution_time': self.execution_time,
            'log_file_name': self.log_file_name,
            'bilbo_version': self._biblo_version
        }
        # Créer un DataFrame à partir du dictionnaire
        df = pd.DataFrame([metadata])

        print("df",df)

        # Insérer le DataFrame dans la base de données
        with pd.option_context('display.max_colwidth', None):  # Permet d'afficher des colonnes de texte longues
            df.to_sql('processing_metadata', schema=DB_META_SCHEMA, con=self.engine, if_exists='append', index=False)
        
        
        logging.info("metadata créée : ", metadata.get('id'))
