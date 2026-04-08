import unittest
from unittest.mock import Mock, MagicMock, PropertyMock

from aws_resources.readers.glue_data_catalog_reader import GlueDataCatalogReaderAPI


def _mock_paginator(pages):
    paginator = Mock()
    paginator.paginate.return_value = pages
    return paginator


class TestGlueDataCatalogReaderAPI(unittest.TestCase):
    """Tests for GlueDataCatalogReaderAPI."""

    def _make_reader(self, glue_client, region="us-east-1", account_id="123456789012"):
        session = Mock()
        session.client.return_value = glue_client
        session.region_name = region
        return GlueDataCatalogReaderAPI(session, account_id)

    def test_reads_single_database_with_tables(self):
        glue_client = Mock()

        db_pages = [{"DatabaseList": [
            {"Name": "mydb", "CatalogId": "123456789012", "Location": "s3://bucket/mydb/"}
        ]}]
        table_pages = [{"TableList": [
            {"Name": "table1", "StorageDescriptor": {"Location": "s3://bucket/mydb/table1/"}},
            {"Name": "table2", "StorageDescriptor": {"Location": "s3://bucket/mydb/table2/"}},
        ]}]

        def get_paginator(operation):
            if operation == "get_databases":
                return _mock_paginator(db_pages)
            if operation == "get_tables":
                return _mock_paginator(table_pages)
            return _mock_paginator([])

        glue_client.get_paginator.side_effect = get_paginator

        reader = self._make_reader(glue_client)
        catalog = reader.read_catalog()

        self.assertEqual(len(catalog.get_catalogs()), 1)
        db = catalog.get_database("123456789012", "mydb")
        self.assertIsNotNone(db)
        self.assertEqual(db.get_name(), "mydb")
        self.assertIsNotNone(db.get_table("table1"))
        self.assertIsNotNone(db.get_table("table2"))

    def test_table_without_storage_descriptor(self):
        """Tables without StorageDescriptor should have None location."""
        glue_client = Mock()

        db_pages = [{"DatabaseList": [{"Name": "mydb", "CatalogId": "123456789012"}]}]
        table_pages = [{"TableList": [{"Name": "view1"}]}]

        def get_paginator(operation):
            if operation == "get_databases":
                return _mock_paginator(db_pages)
            if operation == "get_tables":
                return _mock_paginator(table_pages)
            return _mock_paginator([])

        glue_client.get_paginator.side_effect = get_paginator

        reader = self._make_reader(glue_client)
        catalog = reader.read_catalog()

        table = catalog.get_table("123456789012", "mydb", "view1")
        self.assertIsNotNone(table)
        self.assertIsNone(table.get_location())

    def test_database_without_catalog_id_uses_account_id(self):
        """If CatalogId is missing from the DB response, use the account ID."""
        glue_client = Mock()

        db_pages = [{"DatabaseList": [{"Name": "mydb"}]}]
        table_pages = [{"TableList": []}]

        def get_paginator(operation):
            if operation == "get_databases":
                return _mock_paginator(db_pages)
            if operation == "get_tables":
                return _mock_paginator(table_pages)
            return _mock_paginator([])

        glue_client.get_paginator.side_effect = get_paginator

        reader = self._make_reader(glue_client, account_id="999999999999")
        catalog = reader.read_catalog()

        self.assertIsNotNone(catalog.get_catalog("999999999999"))
        self.assertIsNotNone(catalog.get_database("999999999999", "mydb"))

    def test_multiple_databases_same_catalog(self):
        glue_client = Mock()

        db_pages = [{"DatabaseList": [
            {"Name": "db1", "CatalogId": "123456789012"},
            {"Name": "db2", "CatalogId": "123456789012"},
        ]}]
        table_pages = [{"TableList": []}]

        def get_paginator(operation):
            if operation == "get_databases":
                return _mock_paginator(db_pages)
            if operation == "get_tables":
                return _mock_paginator(table_pages)
            return _mock_paginator([])

        glue_client.get_paginator.side_effect = get_paginator

        reader = self._make_reader(glue_client)
        catalog = reader.read_catalog()

        self.assertEqual(len(catalog.get_catalogs()), 1)
        cat = catalog.get_catalog("123456789012")
        self.assertEqual(len(cat.get_databases()), 2)

    def test_access_denied_on_tables_continues(self):
        """AccessDeniedException on tables should log warning and continue."""
        glue_client = Mock()

        db_pages = [{"DatabaseList": [{"Name": "mydb", "CatalogId": "123456789012"}]}]

        def get_paginator(operation):
            if operation == "get_databases":
                return _mock_paginator(db_pages)
            if operation == "get_tables":
                paginator = Mock()
                paginator.paginate.side_effect = glue_client.exceptions.AccessDeniedException(
                    {"Error": {"Code": "AccessDeniedException", "Message": "Denied"}}, "GetTables"
                )
                return paginator
            return _mock_paginator([])

        glue_client.get_paginator.side_effect = get_paginator
        glue_client.exceptions.AccessDeniedException = type("AccessDeniedException", (Exception,), {
            "__init__": lambda self, *args, **kwargs: Exception.__init__(self, "Access Denied")
        })
        glue_client.exceptions.EntityNotFoundException = type("EntityNotFoundException", (Exception,), {})

        reader = self._make_reader(glue_client)
        catalog = reader.read_catalog()

        # Database should still be added even though tables failed
        self.assertIsNotNone(catalog.get_database("123456789012", "mydb"))

    def test_entity_not_found_on_tables_continues(self):
        """EntityNotFoundException on tables should log warning and continue."""
        glue_client = Mock()

        db_pages = [{"DatabaseList": [{"Name": "mydb", "CatalogId": "123456789012"}]}]

        def get_paginator(operation):
            if operation == "get_databases":
                return _mock_paginator(db_pages)
            if operation == "get_tables":
                paginator = Mock()
                paginator.paginate.side_effect = glue_client.exceptions.EntityNotFoundException(
                    {"Error": {"Code": "EntityNotFoundException", "Message": "Not found"}}, "GetTables"
                )
                return paginator
            return _mock_paginator([])

        glue_client.get_paginator.side_effect = get_paginator
        glue_client.exceptions.AccessDeniedException = type("AccessDeniedException", (Exception,), {})
        glue_client.exceptions.EntityNotFoundException = type("EntityNotFoundException", (Exception,), {
            "__init__": lambda self, *args, **kwargs: Exception.__init__(self, "Not Found")
        })

        reader = self._make_reader(glue_client)
        catalog = reader.read_catalog()

        self.assertIsNotNone(catalog.get_database("123456789012", "mydb"))


if __name__ == '__main__':
    unittest.main()
