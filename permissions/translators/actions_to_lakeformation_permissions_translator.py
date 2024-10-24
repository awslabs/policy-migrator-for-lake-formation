
from aws_resources.actions.glue_action import GlueAction
from aws_resources.actions.s3_action import S3Action
from aws_resources.aws_arn_utils import AwsArnUtils
from permissions.permission_record import PermissionRecord
from permissions.permissions_list import PermissionsList
from lakeformation_utils.s3_to_table_mapper import S3ToTableMapper

from .glue_data_catalog_action_translator import GlueDataCatalogActionTranslator
from .s3_action_translator import S3ActionTranslator

import logging
logger = logging.getLogger(__name__)

class ActionsToLFPermissionsTranslator:
    '''
    This class is responsible for translating S3 and Glue actions to LakeFormation permissions, which then can be used to 
    commit into Lake Formation
    '''

    def __init__(self, s3_to_table_mapper : S3ToTableMapper):
        self._s3_to_table_mapper = s3_to_table_mapper

    def translate_iam_permissions_to_lf_permissions(self, permissionsList: PermissionsList) -> PermissionsList:
        logger.info("Translating actions to LF permissions. ")
        lf_permissions_list = PermissionsList()

        for permission in permissionsList:
            resource_arn = permission.resource_arn()
            principal_arn = permission.principal_arn()
            actions = permission.permission_actions()
            if AwsArnUtils.isS3Arn(resource_arn):
                glueTables = self._s3_to_table_mapper.get_tables_from_s3_location_postfix(AwsArnUtils.get_s3_path_from_arn(resource_arn))
                if not glueTables:
                    logger.debug(f"Glue Tables not found at S3 location: {resource_arn}. Ignoring.")
                    continue

                lf_permissions = set()
                for action in actions:
                    translatedAction = S3ActionTranslator.translate_s3_action_to_lf_permission_type(
                                        S3Action.translate_s3_action_to_enum(action))
                    if translatedAction is not None:
                        lf_permissions.add(translatedAction)

                if len(lf_permissions) > 0:
                    for glueTable in glueTables:
                        lf_permissions_list.add_permission_record(
                            PermissionRecord(principal_arn, glueTable.get_arn(), lf_permissions))
            elif AwsArnUtils.isGlueArn(resource_arn):
                lf_permissions = set()
                for action in actions:
                    translatedAction = GlueDataCatalogActionTranslator.translate_glue_action_to_lf_permission_type(
                                        GlueAction.translate_glue_action_to_enum(action))

                    if translatedAction is not None:
                        lf_permissions.add(translatedAction)

                if len(lf_permissions) > 0:
                    lf_permissions_list.add_permission_record(
                        PermissionRecord(principal_arn, resource_arn, lf_permissions))
            else:
                logger.error(f"Unknown resource type: {resource_arn}")
        return lf_permissions_list
