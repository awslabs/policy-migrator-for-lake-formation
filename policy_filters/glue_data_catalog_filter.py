from permissions.permissions_list import PermissionsList
from aws_resources.glue_data_catalog import GlueDataCatalog
from aws_resources.glue_catalog import GlueCatalog
from aws_resources.glue_database import GlueDatabase
from aws_resources.glue_table import GlueTable
from aws_resources.s3_bucket import S3Bucket
from aws_resources.s3_object import S3Object
from aws_resources.aws_arn_utils import AwsArnUtils
from aws_resources.aws_resource_exceptions import CatalogEntityNotFoundException
from lakeformation_utils.s3_to_table_mapper import S3ToTableMapper
from .policy_filter_interface import PolicyFilterInterface

from config.application_configuration import ApplicationConfiguration

import logging

logger = logging.getLogger(__name__)

class FilterNotInGlueCatalog(PolicyFilterInterface):
    '''
        Filters a permission list based on the current catalog. For permissions on resources that no longer
        exist, we will filter it out.
    '''

    _REQUIRED_CONFIGURATION = {}
    _CONFIGURATION_SECTION = "policy_filter_glue_data_catalog"

    def __init__(self, appConfig : ApplicationConfiguration, conf : dict[str]):
        super().__init__(appConfig, conf)
        self._glueDataCatalog : GlueDataCatalog = appConfig.get_glue_data_catalog()
        self._s3_to_table_mapper : S3ToTableMapper = appConfig.get_s3_to_table_translator()

    def filter_policies(self, permissionsList : PermissionsList) -> PermissionsList:
        for permission in permissionsList.get_permissions():
            resource_arn = permission.resource_arn()
            awsObject = AwsArnUtils.getAwsObjectFromArn(resource_arn)
            if not awsObject:
                # if its not a valid arn, filter it out.
                self._add_filtered_permission_record(permission)
                continue

            if isinstance(awsObject, (S3Object, S3Bucket)):
                s3_arn = awsObject.get_arn()
                if s3_arn.endswith("*"):
                    s3_arn = s3_arn[:-1]
                tables = self._s3_to_table_mapper.get_all_tables_from_s3_arn_prefix(s3_arn)
                # If there are no tables returned from our mapper, then the S3 location doesn't contain any Tables
                # so filter it.
                if not tables:
                    self._add_filtered_permission_record(permission)
            elif isinstance(awsObject, GlueCatalog):
                self._check_glue_catalog(awsObject, permission)
            elif isinstance(awsObject, GlueDatabase):
                self._check_glue_database(awsObject, permission)
            elif isinstance(awsObject, GlueTable):
                self._check_glue_table(awsObject, permission)
            else:
                raise CatalogEntityNotFoundException("Unknown glue object type: " + str(type(awsObject)))
        return self.get_filtered_permissions()

    def _check_glue_catalog(self, glueCatalog : GlueCatalog, permission):
        if self._glueDataCatalog.get_catalog(glueCatalog.get_catalog_id()) is None:
            logger.info(f"not found catalog: {glueCatalog}")
            self._add_filtered_permission_record(permission)

    def _check_glue_database(self, glueDatabase : GlueDatabase, permission):
        if self._glueDataCatalog.get_database(glueDatabase.get_catalog_id(), glueDatabase.get_name()) is None:
            logger.info(f"not found database: {glueDatabase}")
            self._add_filtered_permission_record(permission)

    def _check_glue_table(self, glueTable : GlueTable, permission):
        if self._glueDataCatalog.get_table(glueTable.get_catalog_id(), glueTable.get_database(), glueTable.get_name()) is None:
            logger.info(f"not found table: {glueTable}")
            self._add_filtered_permission_record(permission)

    @classmethod
    def get_name(cls) -> str:
        return FilterNotInGlueCatalog.__name__

    @classmethod
    def get_required_configuration(cls) -> dict:
        return FilterNotInGlueCatalog._REQUIRED_CONFIGURATION

    @classmethod
    def get_config_section(cls) -> str:
        return FilterNotInGlueCatalog._CONFIGURATION_SECTION
