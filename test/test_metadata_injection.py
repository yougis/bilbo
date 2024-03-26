import unittest
from datetime import datetime
from oeilnc_config.metadata import ProcessingMetadata
from oeilnc_config import settings
import yaml
from intake import open_catalog


class TestProcessingMetadataInjection(unittest.TestCase):
    def setUp(self):

        listIdSpatial = ["C", "F", "G", "B", "D", "A", "E"]
        configFile = settings.initializeBilboProject('test/.env')

        with open(f"test/test_meta_individuStatSpec.yaml", 'r') as file:
                    zoi_config = yaml.load(file, Loader=yaml.Loader)
        with open(f"test/test_meta_indicateurSpec.yaml", 'r') as file:
                    theme_config = yaml.load(file, Loader=yaml.Loader)



        catalog = zoi_config.get('catalogUri',None)
        dataName = zoi_config.get('dataName',None)
        zoi_catalog = getattr(open_catalog('test/'+catalog),dataName)

        catalog = theme_config.get('catalogUri',None)
        dataName = theme_config.get('dataName',None)
        theme_catalog = getattr(open_catalog('test/'+catalog),dataName)

        output_schema= configFile.get('project_db_schema')
        output_table_name='test_metadata'
        operator_name='John Doe'
        offset_value = zoi_config.get("offset", -1)
        limit_value = zoi_config.get("limit", -1)

        environment_variables= configFile
        dimensions_spatiales= listIdSpatial
        log_file_name='example.log'

        self.metadata = ProcessingMetadata(output_schema, output_table_name, operator_name,
                                           zoi_config, zoi_catalog,
                                           theme_config, theme_catalog,
                                           limit_value, offset_value,
                                           environment_variables,
                                           dimensions_spatiales,
                                           log_file_name)

    def test_execution_date(self):
        self.assertIsInstance(self.metadata.execution_date, datetime)
          

    def test_insert_metadata(self):
        self.metadata.insert_metadata()
        persist_meta = self.metadata.get_metadata_by_id(self.metadata.id)

        self.assertEqual(persist_meta['id'].iloc[0], self.metadata.id)
        


if __name__ == '__main__':
    unittest.main()