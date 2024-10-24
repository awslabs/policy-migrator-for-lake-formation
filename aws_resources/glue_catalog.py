from .glue_database import GlueDatabase
from .aws_resource import AwsObject
from .aws_resource_exceptions import CatalogEntityAlreadyExistsException, CatalogEntityMismatchException

class GlueCatalog(AwsObject):
    '''
    Represents a Catalog in the Glue Data Catalog
    '''

    def __init__(self, region : str, catalog_id : str):
        self._region : str = region
        self._catalog_id : str = catalog_id
        self._databases : dict[GlueDatabase] = {}

    def get_region(self) -> str:
        return self._region

    def get_catalog_id(self) -> str:
        return self._catalog_id

    def get_databases(self) -> dict[GlueDatabase]:
        return self._databases

    def get_database(self, databaseName : str) -> GlueDatabase:
        return self._databases.get(databaseName)

    def add_database(self, glueDatabase: GlueDatabase):
        if glueDatabase.get_catalog_id() != self._catalog_id:
            raise CatalogEntityMismatchException(f"Invalid table provided. Tables catalog name: {glueDatabase.get_catalog_id().get_name()} does not match this databases name: {self.get_name()}")

        if glueDatabase.get_name() in self._databases:
            raise CatalogEntityAlreadyExistsException(f"Database: {glueDatabase.get_name()} already exists in catalog: {self}")

        self._databases[glueDatabase.get_name()] = glueDatabase

    def get_name(self) -> str:
        return self._catalog_id

    def get_arn(self) -> str:
        return f"arn:aws:glue:{self._region}:{self._catalog_id}"

    def __eq__(self, other) -> bool:
        return isinstance(other, GlueCatalog) and self._catalog_id == other._catalog_id

    def __lt__(self, other) -> bool:
        if other is None:
            return False
        return self._region < other._region and self._catalog_id < other._catalog_id

    def __str__(self) -> str:
        return f"GlueCatalog(region={self._region}, id={self._catalog_id})"
