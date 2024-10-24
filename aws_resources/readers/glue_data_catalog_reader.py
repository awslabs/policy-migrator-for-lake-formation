import logging
import boto3

from ..glue_data_catalog import GlueDataCatalog
from ..glue_catalog import GlueCatalog
from ..glue_database import GlueDatabase
from ..glue_table import GlueTable

logger = logging.getLogger(__name__)

class GlueDataCatalogReaderAPI:
    """
        This script will iterate through the GDC and output all resources with their S3 locations
        so that it can be fed into an algorithm that will create data location permissions, etc

         PreRequisites:
            This module should be run as an LF admin.
    """

    def __init__(self, boto3session : boto3.Session, aws_account_id : str):
        self._glueClient = boto3session.client('glue')
        self._region = boto3session.region_name
        self._aws_account_id = aws_account_id

    def read_catalog(self) -> GlueDataCatalog:
        '''
            This function will iterate through the GDC and output all resources with their S3 locations
            so that it can be fed into an algorithm that will create data location permissions, etc
        '''
        glueDataCatalog = GlueDataCatalog()

        db_paginator = self._glueClient.get_paginator('get_databases')
        table_paginator = self._glueClient.get_paginator('get_tables')
        #----------------------------------------------------------------------------------------
        #                         Paginate through the entire GDC and output their locations
        #----------------------------------------------------------------------------------------
        for db_page in db_paginator.paginate(ResourceShareType='ALL'):
            for db in db_page['DatabaseList']:
                catalog_id = db.get('CatalogId', self._aws_account_id)

                glueCatalog = None
                if glueDataCatalog.get_catalog(catalog_id) is None:
                    glueCatalog = GlueCatalog(self._region, catalog_id)
                    glueDataCatalog.add_catalog(glueCatalog)
                else:
                    glueCatalog = glueDataCatalog.get_catalog(catalog_id)

                glueDatabase = GlueDatabase(self._region, catalog_id, db["Name"], db.get('Location', None))
                glueDataCatalog.add_database(glueDatabase)

                #Iterate through the tables in the database and output table data
                #TODO: Change this with GetAttributesToGet to make this much faster. Location will need
                # to be added to the API.
                try:
                    for table_page in table_paginator.paginate(CatalogId=catalog_id, DatabaseName=db["Name"]):
                        for table in table_page['TableList']:
                            tbl_location = None
                            if 'StorageDescriptor' in table and 'Location' in table['StorageDescriptor']:
                                tbl_location = table["StorageDescriptor"]["Location"]

                            glueTable = GlueTable(self._region, catalog_id, glueDatabase.get_name(), table['Name'], tbl_location)
                            glueDataCatalog.add_table(glueTable)

                except self._glueClient.exceptions.AccessDeniedException as e:
                    logger.warning(f'Database {db} was not accessible: {e}')
                except self._glueClient.exceptions.EntityNotFoundException as e:
                    logger.warning(f'Database {db} was not found: {e}')

        return glueDataCatalog
