import unittest

from aws_resources.glue_catalog import GlueCatalog
from aws_resources.glue_database import GlueDatabase
from aws_resources.glue_table import GlueTable
from aws_resources.aws_resource_exceptions import (
    CatalogEntityMismatchException,
    CatalogEntityAlreadyExistsException,
)


class TestGlueCatalogDatabaseErrorMessages(unittest.TestCase):
    """Tests for bug #5 — error messages in add_database / add_table
    were calling .get_name() on plain strings, causing AttributeError."""

    def test_catalog_add_database_mismatch_raises(self):
        catalog = GlueCatalog("us-east-1", "111111111111")
        db = GlueDatabase("us-east-1", "222222222222", "mydb")

        with self.assertRaises(CatalogEntityMismatchException) as ctx:
            catalog.add_database(db)

        self.assertIn("222222222222", str(ctx.exception))
        self.assertIn("111111111111", str(ctx.exception))

    def test_catalog_add_database_duplicate_raises(self):
        catalog = GlueCatalog("us-east-1", "111111111111")
        db1 = GlueDatabase("us-east-1", "111111111111", "mydb")
        db2 = GlueDatabase("us-east-1", "111111111111", "mydb")

        catalog.add_database(db1)
        with self.assertRaises(CatalogEntityAlreadyExistsException):
            catalog.add_database(db2)

    def test_database_add_table_catalog_mismatch_raises(self):
        db = GlueDatabase("us-east-1", "111111111111", "mydb")
        table = GlueTable("us-east-1", "222222222222", "mydb", "mytable")

        with self.assertRaises(CatalogEntityMismatchException) as ctx:
            db.add_table(table)

        self.assertIn("222222222222", str(ctx.exception))
        self.assertIn("111111111111", str(ctx.exception))

    def test_database_add_table_db_mismatch_raises(self):
        db = GlueDatabase("us-east-1", "111111111111", "mydb")
        table = GlueTable("us-east-1", "111111111111", "otherdb", "mytable")

        with self.assertRaises(CatalogEntityMismatchException) as ctx:
            db.add_table(table)

        self.assertIn("otherdb", str(ctx.exception))
        self.assertIn("mydb", str(ctx.exception))

    def test_database_add_table_duplicate_raises(self):
        db = GlueDatabase("us-east-1", "111111111111", "mydb")
        t1 = GlueTable("us-east-1", "111111111111", "mydb", "mytable")
        t2 = GlueTable("us-east-1", "111111111111", "mydb", "mytable")

        db.add_table(t1)
        with self.assertRaises(CatalogEntityAlreadyExistsException):
            db.add_table(t2)

    def test_database_add_table_success(self):
        db = GlueDatabase("us-east-1", "111111111111", "mydb")
        table = GlueTable("us-east-1", "111111111111", "mydb", "mytable", "s3://bucket/path/")

        db.add_table(table)
        self.assertEqual(db.get_table("mytable"), table)


if __name__ == '__main__':
    unittest.main()
