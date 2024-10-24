from permissions.permissions_list import PermissionsList
from permissions.permission_record import PermissionRecord

from config.application_configuration import ApplicationConfiguration

import logging
logger = logging.getLogger(__name__)

class PolicyFilterInterface:
    '''
    Interface/Baseclass for Policy Filters
    '''

    def __init__(self, appConfig : ApplicationConfiguration, conf : dict[str]):
        self._count_filtered = 0
        self._filtered_permissions = PermissionsList()
        self._appConfig = appConfig
        self._config = conf

    def filter_policies(self, permissionsList : PermissionsList):
        pass

    def get_filtered_permissions(self) -> PermissionsList:
        return self._filtered_permissions

    def get_number_filtered(self) -> int:
        return self._count_filtered

    def _add_filtered_permission(self, principal_arn : str, resource_arn : str, action : str):
        if self._filtered_permissions.add_permission(principal_arn, resource_arn, action):
            logger.debug(f"Filtered permission: Principal:{principal_arn} Resource:{resource_arn} Action:{action}")
            self._count_filtered += 1

    def _add_filtered_permission_record(self, permission : PermissionRecord):
        if self._filtered_permissions.add_permission_record(permission):
            logger.debug(f"Filtered permission: Principal:{permission.principal_arn()} Resource:{permission.resource_arn()} Action:{[action for action in permission.permission_actions()]}")
            self._count_filtered += 1

    @classmethod
    def get_name(cls) -> str:
        pass

    @classmethod
    def get_required_configuration(cls) -> dict:
        pass

    @classmethod
    def get_config_section(cls) -> str:
        pass
