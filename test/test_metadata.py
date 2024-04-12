import unittest
import uuid
from datetime import datetime
from oeilnc_config.metadata import ProcessingMetadata
from oeilnc_config import settings


configFile=settings.initializeBilboProject('.env')

class TestProcessingMetadata(unittest.TestCase):
    def setUp(self):
        self.metadata = ProcessingMetadata(run_id= str(uuid.uuid4()),
                                           output_schema='schema_name', 
                                           output_table_name='table_name', 
                                           operator_name='John Doe',
                                           zoi_config={"key": "value"}, 
                                           zoi_catalog={"key": "value"},
                                           theme_config={"key": "value"}, 
                                           theme_catalog={"key": "value"},
                                           limit_value=100, 
                                           offset_value=0,
                                           environment_variables={},
                                           dimensions_spatiales=["dimension1", "dimension2"],
                                           log_file_name='example.log')
        self.metadata.environment_variables = configFile

    def test_execution_date(self):
        self.assertIsInstance(self.metadata.execution_date, datetime)

    def test_output_schema(self):
        self.assertEqual(self.metadata.output_schema, 'schema_name')

    def test_output_table_name(self):
        self.assertEqual(self.metadata.output_table_name, 'table_name')

    def test_operator_name(self):
        self.assertEqual(self.metadata.operator_name, 'John Doe')

    def test_zoi_config(self):
        self.assertEqual(self.metadata.zoi_config, {"key": "value"})

    def test_zoi_catalog(self):
        self.assertEqual(self.metadata.zoi_catalog, {"key": "value"})

    def test_theme_config(self):
        self.assertEqual(self.metadata.theme_config, {"key": "value"})

    def test_theme_catalog(self):
        self.assertEqual(self.metadata.theme_catalog, {"key": "value"})

    def test_limit_value(self):
        self.assertEqual(self.metadata.limit_value, 100)

    def test_offset_value(self):
        self.assertEqual(self.metadata.offset_value, 0)

  

    def test_dimensions_spatiales(self):
        self.assertEqual(self.metadata.dimensions_spatiales, ["dimension1", "dimension2"])

    def test_log_file_name(self):
        self.assertEqual(self.metadata.log_file_name, 'example.log')
    
    def test_get_all_by_conf_property(self):
        conf = 'zoi_config'
        prop = "dataName"
        value = "h3_nc_8"

        actual_df = self.metadata.get_all_by_conf_property(conf, prop, value)


    if __name__ == '__main__':
        unittest.main()
