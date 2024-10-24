
from aws_resources.aws_arn_utils import AwsArnUtils
from aws_resources.glue_catalog import GlueCatalog
from aws_resources.glue_database import GlueDatabase
from aws_resources.glue_table import GlueTable

from permissions.permissions_list import PermissionsList

import boto3
import logging

logger = logging.getLogger(__name__)

class LakeFormationPermissionsCommitter:
    '''
    Commits permissions to the LakeFormation service.
    '''

    def __init__(self, boto3_session : boto3.Session ):
        self._lf_client = boto3_session.client('lakeformation')

    def commit_lakeformation_permissions(self, permissionsList : PermissionsList):
        for permission in permissionsList:
            try:
                logger.debug(f"Committing permission: {permission}")
                awsObject = AwsArnUtils.getAwsObjectFromArn(permission.get_resource_arn())
                if not awsObject:
                    continue

                if isinstance(awsObject, GlueCatalog):
                    self._lf_client.grant_permissions(
                            Principal = {  'DataLakePrincipalIdentifier': permission.get_principal_arn() },
                            Resource = { 'Catalog' : {} },
                            Permissions = permission.get_permissions()
                            )
                elif isinstance(awsObject, GlueDatabase):
                    self._lf_client.grant_permissions(
                            Principal = {  'DataLakePrincipalIdentifier': permission.get_principal_arn() },
                            Resource = { 'Database': { 'CatalogId': awsObject.get_catalog_id(), 'Name': awsObject.get_name() } },
                            Permissions = permission.get_permissions()
                            )
                elif isinstance(awsObject, GlueTable):
                    # pylint: disable=no-value-for-parameter
                    self._lf_client.grant_permissions(
                            Principal = {  'DataLakePrincipalIdentifier': permission.get_principal_arn() },
                            Resource = { 'Table': { 'CatalogId': awsObject.get_catalog_id(), 'DatabaseName': awsObject.get_database(), 'Name': awsObject.get_name() } },
                            Permissions = permission.get_permissions()
                            )
                else:
                    logger.error(f"Unrecognized AWS Resource Type for Permissions {permission}")
            except Exception as e:
                logger.error(f"Failed to commit permission: {permission}, Exception: {e}")
