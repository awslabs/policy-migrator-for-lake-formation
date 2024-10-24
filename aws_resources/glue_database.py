from .glue_table import GlueTable
from .aws_resource import AwsObject
from .aws_resource_exceptions import CatalogEntityMismatchException, CatalogEntityAlreadyExistsException

class GlueDatabase(AwsObject):
    """
    Represents a Glue Database
    """

    def __init__(self, region : str, catalog: str, name : str, location : str = None):
        self._region = region
        self._name = name
        self._location = location
        self._catalog = catalog
        self._tables = dict()

    def get_region(self) -> str:
        return self._region

    def get_name(self) -> str:
        return self._name

    def get_location(self) -> str:
        return self._location

    def get_catalog_id(self) -> str:
        return self._catalog

    def get_table(self, table_name : str) -> GlueTable | None:
        return self._tables.get(table_name)

    def get_tables(self) -> set[GlueTable]:
        return self._tables

    def get_arn(self) -> str:
        return f"arn:aws:glue:{self._region}:{self._catalog}:database/{self._name}"

    def add_table(self, glueTable : GlueTable):
        if glueTable.get_catalog_id() != self._catalog:
            raise CatalogEntityMismatchException(f"Invalid table provided. Tables catalog name: {glueTable.get_catalog_id().get_name()} does not match this databases name: {self._catalog.get_name()}")

        if glueTable.get_database() != self._name:
            raise CatalogEntityMismatchException(f"Invalid table provided. Tables database name: {glueTable.get_database().get_name()} does not match this databases name: {self._name}")

        if glueTable.get_name() in self._tables:
            raise CatalogEntityAlreadyExistsException(f"Table: {glueTable.get_name()} already exists in database: {self}")

        self._tables[glueTable.get_name()] = glueTable

    def __eq__(self, other):
        return isinstance(other, GlueDatabase) and self._name == other._name and self._catalog == other._catalog

    def __lt__(self, other):
        return self._region < other._region and self._catalog < other._catalog and self._name < other._name

    def __str__(self):
        return f"GlueDatabase(region={self._region}, catalog={self._catalog}), name={self._name}, location={self._location}"
