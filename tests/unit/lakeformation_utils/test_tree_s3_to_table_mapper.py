from lakeformation_utils.s3_to_table_mapper import S3ToTableMapper
from aws_resources.glue_data_catalog import GlueDataCatalog
from aws_resources.glue_catalog import GlueCatalog
from aws_resources.glue_database import GlueDatabase
from aws_resources.glue_table import GlueTable
from tests.unit.helpers.global_test_variables import GlobalTestVariables

import unittest
import logging

class TestS3ToTableMapper(unittest.TestCase):

    gdcCatalog : GlueDataCatalog = GlueDataCatalog()
    s3ToTableMapper : S3ToTableMapper = None

    @staticmethod
    def setUpClass():
        test_catalog = GlobalTestVariables.test_catalog_id
        test_region = GlobalTestVariables.test_region

        TestS3ToTableMapper.gdcCatalog.add_catalog(GlueCatalog(test_region, test_catalog))
        TestS3ToTableMapper.gdcCatalog.add_database(GlueDatabase(test_region, test_catalog, "test_database", "s3://mybucket/mydatabases/"))

        TestS3ToTableMapper.gdcCatalog.add_table(GlueTable(test_region, test_catalog, "test_database", "test_table", "s3://mybucket/mydatabases/test_table/"))
        TestS3ToTableMapper.gdcCatalog.add_table(GlueTable(test_region, test_catalog, "test_database", "test_table2", "s3://mybucket/mydatabases/test_table2/"))
        TestS3ToTableMapper.gdcCatalog.add_table(GlueTable(test_region, test_catalog, "test_database", "test_table3", "s3://mybucket/mydatabases/test_table3/"))

        TestS3ToTableMapper.gdcCatalog.add_database(GlueDatabase(test_region, test_catalog, "test_database2", "s3://mybucket/test_database2_location/" ))
        TestS3ToTableMapper.gdcCatalog.add_table(GlueTable(test_region, test_catalog, "test_database2", "test_table", "s3://mybucket/mydatabases_2/test_table"))
        TestS3ToTableMapper.gdcCatalog.add_table(GlueTable(test_region, test_catalog, "test_database2", "test_table2", "s3://mybucket/mydatabases_2/test_table2"))
        TestS3ToTableMapper.gdcCatalog.add_table(GlueTable(test_region, test_catalog, "test_database2", "test_table3", "s3://mybucket/mydatabases_2/test_table3"))

        TestS3ToTableMapper.gdcCatalog.add_table(GlueTable(test_region, test_catalog, "test_database2", "test_table4", "s3://mybucket_2/mydatabases_2/test_table4"))
        TestS3ToTableMapper.gdcCatalog.add_table(GlueTable(test_region, test_catalog, "test_database2", "test_table5", "s3://mybucket_2/mydatabases_2/test_table5"))
        TestS3ToTableMapper.gdcCatalog.add_table(GlueTable(test_region, test_catalog, "test_database2", "test_table6", "s3://mybucket_2/mydatabases_2/test_table6"))

        TestS3ToTableMapper.gdcCatalog.add_table(GlueTable(test_region, test_catalog, "test_database2", "test_table7", "s3://mybucket_2/test_database3_def/test_table7"))
        TestS3ToTableMapper.gdcCatalog.add_table(GlueTable(test_region, test_catalog, "test_database2", "test_table8", "s3://mybucket_2/test_database3_def/test_table8"))
        TestS3ToTableMapper.gdcCatalog.add_table(GlueTable(test_region, test_catalog, "test_database2", "test_table9", "s3://mybucket_2/test_database3_def/test_table9"))

        TestS3ToTableMapper.gdcCatalog.add_table(GlueTable(test_region, test_catalog, "test_database2", "test_table10", "s3://mybucket_3/test_database4/test_table10"))
        TestS3ToTableMapper.gdcCatalog.add_table(GlueTable(test_region, test_catalog, "test_database2", "test_table11", "s3://mybucket_3/test_database4/test_table11"))
        TestS3ToTableMapper.gdcCatalog.add_table(GlueTable(test_region, test_catalog, "test_database2", "test_table12", "s3://mybucket_3/test_database4/test_table12"))
        TestS3ToTableMapper.gdcCatalog.add_table(GlueTable(test_region, test_catalog, "test_database2", "test_table13", "s3://mybucket_3/test_database4/test_table13/some_partition/"))

        # Next 3 databases/tables have the exact same S3 location
        TestS3ToTableMapper.gdcCatalog.add_database(GlueDatabase(test_region, test_catalog, "test_database3", "s3://mybucket/test_database2_location/" ))
        TestS3ToTableMapper.gdcCatalog.add_table(GlueTable(test_region, test_catalog, "test_database3", "test_table14", "s3://mybucket_4/test_database4/test_table/"))

        TestS3ToTableMapper.gdcCatalog.add_database(GlueDatabase(test_region, test_catalog, "test_database4", "s3://mybucket/test_database2_location/" ))
        TestS3ToTableMapper.gdcCatalog.add_table(GlueTable(test_region, test_catalog, "test_database4", "test_table15", "s3://mybucket_4/test_database4/test_table/"))

        TestS3ToTableMapper.gdcCatalog.add_database(GlueDatabase(test_region, test_catalog, "test_database5", "s3://mybucket/test_database2_location/" ))
        TestS3ToTableMapper.gdcCatalog.add_table(GlueTable(test_region, test_catalog, "test_database5", "test_table16", "s3://mybucket_4/test_database4/test_table/"))

        TestS3ToTableMapper.s3ToTableMapper = S3ToTableMapper(TestS3ToTableMapper.gdcCatalog)

    def test_partitioned_table_path(self):
        tables = TestS3ToTableMapper.s3ToTableMapper.get_tables_from_s3_location_postfix("s3://mybucket_2/test_database3_def/test_table7/some_partition/file.txt")
        self.assertIsNotNone(tables)
        self.assertEqual(len(tables), 1)
        self.assertEqual(tables[0].get_name(), "test_table7")

    def test_partitioned_table(self):
        tables = TestS3ToTableMapper.s3ToTableMapper.get_tables_from_s3_location_postfix("s3://mybucket_3/test_database4/test_table13/some_partition/file.txt")
        self.assertIsNotNone(tables)
        self.assertEqual(len(tables), 1)
        self.assertEqual(tables[0].get_name(), "test_table13")

    def test_non_existant_location(self):
        tables = TestS3ToTableMapper.s3ToTableMapper.get_tables_from_s3_location_postfix("s3://mybucket_2/test_database4/")
        self.assertIsNotNone(tables)
        self.assertEqual(len(tables), 0)

    def test_incorrect_table_location(self):
        tables = TestS3ToTableMapper.s3ToTableMapper.get_tables_from_s3_location_postfix("s3://mybucket_2/test_database4/test_table13/")
        self.assertIsNotNone(tables)
        self.assertEqual(len(tables), 0)

    def test_multiple_tables_in_location(self):
        tables = TestS3ToTableMapper.s3ToTableMapper.get_tables_from_s3_location_postfix("s3://mybucket_4/test_database4/test_table/")
        self.assertIsNotNone(tables)
        self.assertEqual(len(tables), 3)

    def test_get_all_tables_from_path(self):
        tables = TestS3ToTableMapper.s3ToTableMapper.get_all_tables_from_s3_path_prefix("s3://mybucket_2/test_database3_def/")
        self.assertIsNotNone(tables)
        self.assertEqual(len(tables), 3)
        tables.sort()
        self.assertEqual(tables[0].get_name(), "test_table7")
        self.assertEqual(tables[1].get_name(), "test_table8")
        self.assertEqual(tables[2].get_name(), "test_table9")

    def test_no_path(self):
        tables = TestS3ToTableMapper.s3ToTableMapper.get_all_tables_from_s3_path_prefix("s3://")
        self.assertIsNotNone(tables)
        self.assertEqual(len(tables), 19)

    def test_get_all_tables_from_s3_arn(self):
        tables = TestS3ToTableMapper.s3ToTableMapper.get_all_tables_from_s3_arn_prefix("arn:aws:s3:::mybucket/mydatabases/")
        self.assertIsNotNone(tables)
        self.assertEqual(len(tables), 3)

    def test_get_tables_from_s3_arn(self):
        tables = TestS3ToTableMapper.s3ToTableMapper.get_tables_from_s3_arn_postfix("arn:aws:s3:::mybucket_4/test_database4/test_table/")
        self.assertIsNotNone(tables)
        self.assertEqual(len(tables), 3)

    def test_try_to_find_bug(self):
        tables = TestS3ToTableMapper.s3ToTableMapper.get_all_tables_from_s3_arn_prefix("arn:aws:s3:::mybucket/mydatabases/")
        self.assertIsNotNone(tables)
        self.assertEqual(len(tables), 3)

if __name__ == '__main__':
    unittest.main()
