
from config.application_configuration import ApplicationConfiguration
from .post_processing_plugin_interface import PostProcessingPluginInterface

from permissions.permissions_list import PermissionsList
from permissions.permission_record import PermissionRecord
from permissions.lakeformation_permissions.lakeformation_permissions import LakeFormationPermissions

import logging
logger = logging.getLogger(__name__)

class AddDataPermissionsFromGluePermissions(PostProcessingPluginInterface):
    '''
    This post processing plugin adds S3 permissions from Glue permissions, ie DESCRIBE gives SELECT,
    ALTER gives INSERT and DROP.
    '''

    _CONFIG_SECTION = "post_processing_add_data_permissions_from_glue_permissions"

    def __init__(self, appConfig : ApplicationConfiguration, conf : dict[str]):
        super().__init__(appConfig, conf)
        logger.info(f"=> {self.get_name()} initialized.")

    def process(self, lf_permissions: PermissionsList) -> PermissionsList:
        new_permissions = PermissionsList()
        for item in lf_permissions:
            new_actions = item.permission_actions().copy()
            if LakeFormationPermissions.DESCRIBE.value in item.permission_actions():
                logger.debug(f"Adding SELECT to {item.principal_arn()} for resource: {item.resource_arn()} has DESCRIBE permissions")
                new_actions.add(LakeFormationPermissions.SELECT)
            if LakeFormationPermissions.ALTER.value in item.permission_actions():
                logger.debug(f"Adding INSERT/DELETE to {item.principal_arn()} for resource: {item.resource_arn()} because user has ALTER permissions")
                new_actions.add(LakeFormationPermissions.INSERT)
                new_actions.add(LakeFormationPermissions.DELETE)
            new_permissions.add_permission_record(PermissionRecord(
                item.principal_arn(),
                item.resource_arn(),
                new_actions
                ))
        return new_permissions

    @classmethod
    def get_name(cls) -> str:
        return AddDataPermissionsFromGluePermissions.__name__

    @classmethod
    def get_required_configuration(cls) -> dict:
        return {}

    @classmethod
    def get_config_section(cls) -> str:
        return AddDataPermissionsFromGluePermissions._CONFIG_SECTION
