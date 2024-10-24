import unittest
from aws_resources.glue_data_catalog import GlueDataCatalog
from aws_resources.glue_data_catalog import GlueCatalog
from aws_resources.glue_data_catalog import GlueDatabase
from aws_resources.glue_data_catalog import GlueTable

from lakeformation_utils.data_lake_location_generator import LakeFormationS3DataLakeLocationGenerator
from tests.unit.helpers.global_test_variables import GlobalTestVariables
class TestDataLakeLocationGenerator(unittest.TestCase):
    
    def test_generate_data_lake_location(self):
        test_catalog = GlobalTestVariables.test_catalog_id
        test_region = GlobalTestVariables.test_region

        gdcCatalog = GlueDataCatalog()

        gdcCatalog.add_catalog(GlueCatalog(test_region, test_catalog))
        
        # The following should add s3://mybucket/ as the root location for these locations
        gdcCatalog.add_database(GlueDatabase(test_region, test_catalog, "test_database", "s3://mybucket/mydatabases/"))
        gdcCatalog.add_table(GlueTable(test_region, test_catalog, "test_database", "test_table", "s3://mybucket/mydatabases/test_table/"))
        gdcCatalog.add_table(GlueTable(test_region, test_catalog, "test_database", "test_table2", "s3://mybucket/mydatabases/test_table2/"))
        gdcCatalog.add_table(GlueTable(test_region, test_catalog, "test_database", "test_table3", "s3://mybucket/mydatabases/test_table3/"))
        gdcCatalog.add_database(GlueDatabase(test_region, test_catalog, "test_database2", "s3://mybucket/test_database2_location/" ))
        gdcCatalog.add_table(GlueTable(test_region, test_catalog, "test_database2", "test_table", "s3://mybucket/test_database2/test_table"))
        gdcCatalog.add_table(GlueTable(test_region, test_catalog, "test_database2", "test_table2", "s3://mybucket/test_database2/test_table2"))
        gdcCatalog.add_table(GlueTable(test_region, test_catalog, "test_database2", "test_table3", "s3://mybucket/test_database2_abc/test_table3"))

        # The following all share s3://mybucket_2/ as the root location
        gdcCatalog.add_table(GlueTable(test_region, test_catalog, "test_database2", "test_table4", "s3://mybucket_2/test_database2_abc/test_table4"))
        gdcCatalog.add_table(GlueTable(test_region, test_catalog, "test_database2", "test_table5", "s3://mybucket_2/test_database2_abc/test_table5"))
        gdcCatalog.add_table(GlueTable(test_region, test_catalog, "test_database2", "test_table6", "s3://mybucket_2/test_database2_abc/test_table6"))

        gdcCatalog.add_table(GlueTable(test_region, test_catalog, "test_database2", "test_table7", "s3://mybucket_2/test_database3_def/test_table7"))
        gdcCatalog.add_table(GlueTable(test_region, test_catalog, "test_database2", "test_table8", "s3://mybucket_2/test_database3_def/test_table8"))
        gdcCatalog.add_table(GlueTable(test_region, test_catalog, "test_database2", "test_table9", "s3://mybucket_2/test_database3_def/test_table9"))

        # The following all share s3://mybucket_3/test_database4/ as the root location
        gdcCatalog.add_table(GlueTable(test_region, test_catalog, "test_database2", "test_table10", "s3://mybucket_3/test_database4/test_table10"))
        gdcCatalog.add_table(GlueTable(test_region, test_catalog, "test_database2", "test_table11", "s3://mybucket_3/test_database4/test_table11"))
        gdcCatalog.add_table(GlueTable(test_region, test_catalog, "test_database2", "test_table12", "s3://mybucket_3/test_database4/test_table12"))

        expected_results = { "s3://mybucket/", "s3://mybucket_2/", "s3://mybucket_3/test_database4/" }

        dataLakeLocations = LakeFormationS3DataLakeLocationGenerator()

        s3Locations = dataLakeLocations.generate_data_lake_locations(gdcCatalog)

        self.assertEqual(len(s3Locations), 3)
        for s3Location in s3Locations:
            self.assertTrue(s3Location in expected_results)

if __name__ == '__main__':
    unittest.main()
