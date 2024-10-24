import unittest

from aws_resources.aws_resource_exceptions import CatalogEntityAlreadyExistsException
from aws_resources.glue_catalog import GlueCatalog
from aws_resources.glue_data_catalog import GlueDataCatalog
from aws_resources.glue_database import GlueDatabase
from aws_resources.glue_table import GlueTable
from tests.unit.helpers.permissions_list_test_helpers import PermissionsListTestHelper

class TestGlueDataCatalog(unittest.TestCase):

    def test_create_and_query_data_catalog(self):
        catalog_id = PermissionsListTestHelper.test_catalog_id
        glueDataCatalog = PermissionsListTestHelper.create_glue_data_catalog()

        self.assertEqual(len(glueDataCatalog.get_catalogs()), 1)

        catalog : GlueCatalog = glueDataCatalog.get_catalog(catalog_id)
        self.assertEqual(catalog.get_catalog_id(), catalog_id)
        self.assertEqual(len(catalog.get_databases()), 2)

        database1 : GlueDatabase = catalog.get_database("test_database")
        self.assertEqual(database1.get_name(), "test_database")
        self.assertEqual(len(database1.get_tables()), 3)

        non_existant_database : GlueDatabase = catalog.get_database("non_existant_database")
        self.assertIsNone(non_existant_database)

    def test_adding_existing_table(self):
        catalog_id = PermissionsListTestHelper.test_catalog_id
        test_region = PermissionsListTestHelper.test_region
        glueDataCatalog = PermissionsListTestHelper.create_glue_data_catalog()

        catalog : GlueCatalog = glueDataCatalog.get_catalog(catalog_id)
        database : GlueDatabase = catalog.get_database("test_database")
        table : GlueTable = GlueTable(test_region, catalog_id, "test_database", "test_table3", "s3://mybucket/mydatabases/test_table3/")

        with self.assertRaises(CatalogEntityAlreadyExistsException):
            database.add_table(table)

    def test_adding_existing_database(self):
        catalog_id = PermissionsListTestHelper.test_catalog_id
        test_region = PermissionsListTestHelper.test_region
        glueDataCatalog = PermissionsListTestHelper.create_glue_data_catalog()

        catalog : GlueCatalog = glueDataCatalog.get_catalog(catalog_id)
        database : GlueDatabase = GlueDatabase(test_region, catalog_id, "test_database")

        with self.assertRaises(CatalogEntityAlreadyExistsException):
            catalog.add_database(database)

    def test_wildcard_search(self):
        catalog_id : str = PermissionsListTestHelper.test_catalog_id
        glueDataCatalog : GlueDataCatalog = PermissionsListTestHelper.create_glue_data_catalog()

        results = glueDataCatalog.get_resources_by_wildcard(catalog_id, "*")
        self.assertEqual(len(results), 2)

        results = glueDataCatalog.get_resources_by_wildcard(catalog_id, "*", "test_table")
        results.sort()
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0].get_name(), "test_table")
        self.assertEqual(results[0].get_database(), "test_database")
        self.assertEqual(results[1].get_name(), "test_table")
        self.assertEqual(results[1].get_database(), "test_database2")

        results = glueDataCatalog.get_resources_by_wildcard("*", "*", "test_table2")
        results.sort()

        self.assertEqual(len(results), 2)
        self.assertEqual(results[0].get_name(), "test_table2")
        self.assertEqual(results[0].get_database(), "test_database")
        self.assertEqual(results[1].get_name(), "test_table2")
        self.assertEqual(results[1].get_database(), "test_database2")

        results = glueDataCatalog.get_resources_by_wildcard("*", "test_database", "*")
        results.sort()

        self.assertEqual(len(results), 3)
        self.assertEqual(results[0].get_name(), "test_table")
        self.assertEqual(results[0].get_database(), "test_database")
        self.assertEqual(results[1].get_name(), "test_table2")
        self.assertEqual(results[1].get_database(), "test_database")
        self.assertEqual(results[2].get_name(), "test_table3")
        self.assertEqual(results[2].get_database(), "test_database")

        results = glueDataCatalog.get_resources_by_wildcard("*", "test_database")

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].get_name(), "test_database")

if __name__ == '__main__':
    unittest.main()
