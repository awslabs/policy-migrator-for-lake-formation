from .aws_resource import AwsObject

class GlueTable(AwsObject):
    """
    Represents a Glue Table
    """

    def __init__(self, region : str, catalog : str, database : str, name : str, location : str = None):
        self._region = region
        self._catalog = catalog
        self._database = database
        self._name = name
        if location is not None:
            if location.endswith("/"):
                self._location = location
            else:
                self._location = location + "/"
        else:
            self._location = None

    def get_catalog_id(self) -> str:
        return self._catalog

    def get_database(self) -> str:
        return self._database

    def get_name(self):
        return self._name

    def get_location(self):
        return self._location

    def get_region(self):
        return self._region

    def get_arn(self):
        return f"arn:aws:glue:{self._region}:{self._catalog}:table/{self._database}/{self._name}"

    def __lt__(self, other):
        return self._region < other._region and self._catalog < other._catalog and self._database < other._database and self._name < other._name

    def __eq__(self, other):
        return isinstance(other, GlueTable) and self._catalog == other._catalog and self._database == other._database and self._name == other._name

    def __str__(self):
        return f"GlueTable(region={self._region}, catalog={self._catalog}, database={self._database}, name={self._name}, location={self._location})"

    def __hash__(self):
        return hash((self._catalog, self._database, self._name))
