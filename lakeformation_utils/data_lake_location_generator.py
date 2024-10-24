from aws_resources.glue_catalog import GlueCatalog
from aws_resources.glue_data_catalog import GlueDataCatalog
from aws_resources.glue_database import GlueDatabase
from aws_resources.glue_table import GlueTable


class LakeFormationS3DataLakeLocationGenerator:
    '''
    Given a Glue Data Catalog, this will generate Data Location's to be registered with LF.
    '''

    def generate_data_lake_locations(self, glueDataCatalog : GlueDataCatalog) -> list[str]:
        if glueDataCatalog is None:
            raise ValueError('glueDataCatalog cannot be None')

        s3_locations = self._get_s3_locations_from_catalog(glueDataCatalog)
        #sort s3_locations
        s3_locations.sort()

        #remove duplicates and remove subfolders
        i = 0
        while i < len(s3_locations) - 1:
            if s3_locations[i] == s3_locations[i+1] or s3_locations[i+1].startswith(s3_locations[i]):
                s3_locations.pop(i+1)
            else:
                i += 1

        i = 0
        while i < len(s3_locations):
            #remove trailing "/"
            current_location = s3_locations[i][:-1]
            current_location = current_location[:current_location.rindex("/")] + "/"

            if current_location == 's3://':
                i += 1
                continue

            j = i + 1
            has_changes = False
            while j < len(s3_locations):
                if s3_locations[j].startswith(current_location):
                    s3_locations.pop(j)
                    s3_locations[i] = current_location
                    has_changes = True
                else:
                    break

            if not has_changes:
                i = i + 1

        return s3_locations


    def _get_s3_locations_from_catalog(self, glueDataCatalog : GlueDataCatalog) -> list:
        s3_locations = []
        for item in glueDataCatalog:
            if isinstance(item, GlueCatalog):
                pass
            if isinstance(item, (GlueDatabase, GlueTable)):
                if item.get_location() is not None:
                    location = item.get_location()
                    if item.get_location().startswith("s3a://"):
                        location = "s3://" + location[5:]

                    if location.startswith("s3://"):
                        if not location.endswith("/"):
                            location += "/"
                        s3_locations.append(location)
        return s3_locations
