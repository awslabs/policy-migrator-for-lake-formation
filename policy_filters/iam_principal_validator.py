from permissions.permissions_list import PermissionsList

from .policy_filter_interface import PolicyFilterInterface

from config.application_configuration import ApplicationConfiguration

import logging
logger = logging.getLogger(__name__)

class IAMPrincipalValidator(PolicyFilterInterface):
    '''
    Validates that permissions have valid IAM Users.
    '''

    _REQUIRED_CONFIGURATION = {}
    _CONFIGURATION_SECTION = "policy_filter_iam_principal_validator"

    def __init__(self, appConfig : ApplicationConfiguration, conf : dict[str]):
        super().__init__(appConfig, conf)
        self._iam_policy_reader = appConfig.get_iam_policy_reader()

    def filter_policies(self, permissionsList : PermissionsList) -> PermissionsList:
        self._validate_glue_policies(permissionsList)
        return self.get_filtered_permissions()

    def _validate_glue_policies(self, permissions_list : PermissionsList):
        for permission in permissions_list:
            policies = self._iam_policy_reader.get_iam_policies_for_prinicpal(permission.principal_arn())
            if not policies:
                self._add_filtered_permission_record(permission)
                logger.debug(f"Filtering out permission: {permission}")

    @classmethod
    def get_name(cls) -> str:
        return IAMPrincipalValidator.__name__

    @classmethod
    def get_required_configuration(cls) -> dict:
        return IAMPrincipalValidator._REQUIRED_CONFIGURATION

    @classmethod
    def get_config_section(cls) -> str:
        return IAMPrincipalValidator._CONFIGURATION_SECTION
