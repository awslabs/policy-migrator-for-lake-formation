from permissions.permissions_list import PermissionsList

from .policy_filter_interface import PolicyFilterInterface

from config.application_configuration import ApplicationConfiguration

import logging
logger = logging.getLogger(__name__)


class FilterDataZoneRoles(PolicyFilterInterface):
    '''
    Filters out permissions for IAM roles that start with "datazone-".
    These roles are managed by DataZone and Lake Formation already,
    so there is no need to migrate their permissions.
    '''

    _REQUIRED_CONFIGURATION = {}
    _CONFIGURATION_SECTION = "policy_filter_datazone_roles"
    _DATAZONE_ROLE_PREFIX = "datazone-"

    def __init__(self, appConfig : ApplicationConfiguration, conf : dict[str]):
        super().__init__(appConfig, conf)

    def filter_policies(self, permissionsList : PermissionsList) -> PermissionsList:
        for permission in permissionsList.get_permissions():
            principal_arn = permission.principal_arn()
            role_name = self._extract_role_name(principal_arn)
            if role_name and role_name.startswith(self._DATAZONE_ROLE_PREFIX):
                logger.debug(f"Filtering DataZone role: {principal_arn}")
                self._add_filtered_permission_record(permission)
        return self.get_filtered_permissions()

    @staticmethod
    def _extract_role_name(principal_arn : str) -> str | None:
        """Extracts the role name from an IAM role ARN.
        e.g. arn:aws:iam::123456789012:role/datazone-MyRole -> datazone-MyRole
             arn:aws:iam::123456789012:role/path/datazone-MyRole -> datazone-MyRole
        """
        if ":role/" not in principal_arn:
            return None
        # Role name is everything after the last '/' in the ARN
        return principal_arn.rsplit("/", 1)[-1]

    @classmethod
    def get_name(cls) -> str:
        return FilterDataZoneRoles.__name__

    @classmethod
    def get_required_configuration(cls) -> dict:
        return FilterDataZoneRoles._REQUIRED_CONFIGURATION

    @classmethod
    def get_config_section(cls) -> str:
        return FilterDataZoneRoles._CONFIGURATION_SECTION
