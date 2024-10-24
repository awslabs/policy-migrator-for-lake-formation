from .glue_catalog import GlueCatalog
from .glue_database import GlueDatabase
from .glue_table import GlueTable

from .aws_resource_exceptions import CatalogEntityAlreadyExistsException, CatalogEntityNotFoundException

import logging
logger = logging.getLogger(__name__)

class GlueDataCatalog():
    '''
        Represents a Glue Data Catalog which can be traversed.
    '''

    def __init__(self) -> None:
        self._catalogs : dict [str, GlueCatalog]= {}

    def add_catalog(self, glue_catalog: GlueCatalog):
        if glue_catalog.get_catalog_id() not in self._catalogs:
            self._catalogs[glue_catalog.get_catalog_id()] = glue_catalog
        else:
            raise CatalogEntityAlreadyExistsException(f"Catalog {glue_catalog.get_catalog_id()} already exists")

    def get_catalog(self, catalog_id : str) -> GlueCatalog | None:
        catalog = self._catalogs.get(catalog_id)
        if catalog is None:
            logger.debug(f"get_catalog: Catalog {catalog_id} has not been registered yet.")
            return None
        return catalog

    def get_catalogs(self) -> dict[GlueCatalog]:
        return self._catalogs

    def add_database(self, database: GlueDatabase):
        if database.get_catalog_id() not in self._catalogs:
            raise CatalogEntityNotFoundException(f"add_database: Catalog {database.get_catalog_id()} has not been registered yet.")
        self._catalogs[database.get_catalog_id()].add_database(database)

    def get_database(self, catalog_id : str, database_name : str) -> GlueDatabase:
        catalog = self.get_catalog(catalog_id)
        if catalog is None:
            return None
        database = catalog.get_database(database_name)
        if database is None:
            logger.debug(f"get_database: Database {database_name} in Catalog {catalog_id} does not exist")
            return None
        return database

    def add_table(self, glueTable: GlueTable):
        if glueTable.get_catalog_id() not in self._catalogs:
            raise CatalogEntityNotFoundException(f"Catalog {glueTable.get_catalog_id()} does not exist")
        self._catalogs[glueTable.get_catalog_id()].get_database(glueTable.get_database()).add_table(glueTable)

    def get_table(self, catalog_id : str, database_name : str, table_name : str) -> GlueTable:
        database = self.get_database(catalog_id, database_name)
        if database is None:
            return None
        table = database.get_table(table_name)
        if table is None:
            logger.debug(f"get_table: Table {table_name} in Database {database_name} in Catalog {catalog_id} does not exist")
            return None
        return table

    def get_resources_by_wildcard(self, catalog_id : str, database_name : str | None = None, table_name : str | None = None) -> list[GlueCatalog] | list[GlueDatabase] | list[GlueTable]:
        catalogs : list[GlueCatalog] = []
        if catalog_id == "*":
            catalogs.extend(self.get_catalogs().values())
        else:
            catalog = self.get_catalog(catalog_id)
            if catalog is not None:
                catalogs.append(catalog)

        if database_name is None:
            return catalogs

        databases : list[GlueDatabase] = []
        for catalog in catalogs:
            if database_name == "*":
                databases.extend(self.get_catalog(catalog.get_catalog_id()).get_databases().values())
            else:
                database = self.get_database(catalog.get_catalog_id(), database_name)
                if database is not None:
                    databases.append(database)

        if table_name is None:
            return databases

        tables : list[GlueTable] = []
        for database in databases:
            if table_name == "*":
                tables.extend(database.get_tables().values())
            else:
                table = database.get_table(table_name)
                if table is not None:
                    tables.append(table)

        return tables

    def get_tables(self):
        return GlueDataCatalogTablesIterator(self)

    def __iter__(self):
        return GlueDataCatalogIterator(self)

class GlueDataCatalogIterator:
    '''
    Iterator for GlueDataCatalog for all items in the catalog
    '''

    def __init__(self, glueDataCatalog : GlueDataCatalog):
        self._gen = self._generate(glueDataCatalog)

    def __iter__(self):
        return self

    def __next__(self) -> GlueCatalog | GlueDatabase | GlueTable:
        return next(self._gen)

    def _generate(self, glueDataCatalog : GlueDataCatalog):
        for catalog in glueDataCatalog.get_catalogs().values():
            yield catalog
            for database in catalog.get_databases().values():
                yield database
                for table in database.get_tables().values():
                    yield table

class GlueDataCatalogTablesIterator:
    '''
    Iterator for GlueDataCatalog for all tables in the catalog
    '''

    def __init__(self, glueDataCatalog : GlueDataCatalog):
        self._gen = self._generate(glueDataCatalog)

    def __iter__(self):
        return self

    def __next__(self) -> GlueTable:
        return next(self._gen)

    def _generate(self, glueDataCatalog : GlueDataCatalog):
        for catalog in glueDataCatalog.get_catalogs().values():
            for database in catalog.get_databases().values():
                for table in database.get_tables().values():
                    yield table
