from permissions.permission_record import PermissionRecord
from permissions.permissions_list import PermissionsList
from aws_resources.glue_catalog import GlueCatalog
from aws_resources.glue_database import GlueDatabase
from aws_resources.glue_table import GlueTable
from aws_resources.s3_object import S3Object
from aws_resources.aws_arn_utils import AwsArnUtils
from aws_resources.actions.glue_action import GlueAction
from aws_resources.actions.s3_action import S3Action
from .policy_filter_interface import PolicyFilterInterface

from config.application_configuration import ApplicationConfiguration

import logging

logger = logging.getLogger(__name__)

class FilterInvalidActionsToResources(PolicyFilterInterface):
    '''
        Filters a permission list so that invalid actions are removed for resources. For example, if we derive glue:GetTable for a database,
        then it should be removed because glue:GetTable does not apply to databases. 
    '''

    _REQUIRED_CONFIGURATION = {}
    _CONFIGURATION_SECTION = "policy_filter_invalid_actions_for_resources"

    def __init__(self, appConfig : ApplicationConfiguration, conf : dict[str]):
        super().__init__(appConfig, conf)
        logger.info(f"Filter {self.get_name()} started.")

    def filter_policies(self, permissionsList : PermissionsList) -> PermissionsList:
        permission : PermissionRecord
        for permission in permissionsList.get_permissions():
            resource_arn = permission.resource_arn()
            awsObject = AwsArnUtils.getAwsObjectFromArn(resource_arn)

            if not awsObject:
                # if its not a valid arn, filter it out.
                self._add_filtered_permission_record(permission)
                continue

            actions = permission.permission_actions()
            filtered_actions : list[str] = []
            if isinstance(awsObject, GlueCatalog):
                filtered_actions : list[str] = GlueAction.get_filtered_out_catalog_level_actions(actions)
            elif isinstance(awsObject, GlueDatabase):
                filtered_actions : list[str] = GlueAction.get_filtered_out_database_level_actions(actions)
            elif isinstance(awsObject, GlueTable):
                filtered_actions : list[str] = GlueAction.get_filtered_out_table_level_actions(actions)
            elif isinstance(awsObject, S3Object):
                filtered_actions : list[str] = S3Action.get_filtered_out_s3_table_level_actions(actions)

            logger.debug(f"FilterInvalidActions: Resource: {resource_arn} Actions: {actions} filtered actions (invalid): {filtered_actions}")

            if filtered_actions:
                self._add_filtered_permission_record(PermissionRecord(permission.principal_arn(),
                                                                    permission.resource_arn(),
                                                                    set(filtered_actions)))
        return self.get_filtered_permissions()

    @classmethod
    def get_name(cls) -> str:
        return FilterInvalidActionsToResources.__name__

    @classmethod
    def get_required_configuration(cls) -> dict:
        return FilterInvalidActionsToResources._REQUIRED_CONFIGURATION

    @classmethod
    def get_config_section(cls) -> str:
        return FilterInvalidActionsToResources._CONFIGURATION_SECTION
