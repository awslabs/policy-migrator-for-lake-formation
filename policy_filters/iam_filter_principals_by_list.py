from permissions.permissions_list import PermissionsList
from .policy_filter_interface import PolicyFilterInterface

from config.application_configuration import ApplicationConfiguration

import re
import logging

logger = logging.getLogger(__name__)

class IamFilterPrincipalsByList(PolicyFilterInterface):
    '''
        Filters a permission list based on a hard coded string of IAM principals. This can be used to filter out 
        ServiceRoles, etc that may not need lake formation permissions. It will support an inclusion list and 
        an exclusion list.

        TODO: Not completed.
    '''

    _REQUIRED_CONFIGURATION = { "principals_list", "A list of principals to filter permissions by. "}
    _CONFIGURATION_SECTION = "policy_iam_filter_principals_by_list"

    def __init__(self, appConfig : ApplicationConfiguration, conf : dict[str]):
        super().__init__(appConfig, conf)
        logger.info(f"Starting {IamFilterPrincipalsByList.__name__}")

    def filter_policies(self, permissionsList: PermissionsList):
        raise NotImplementedError(f"{IamFilterPrincipalsByList.__name__} has not been implemented yet!")

    @classmethod
    def get_name(cls) -> str:
        return IamFilterPrincipalsByList.__name__

    @classmethod
    def get_required_configuration(cls) -> dict:
        return IamFilterPrincipalsByList._REQUIRED_CONFIGURATION

    @classmethod
    def get_config_section(cls) -> str:
        return IamFilterPrincipalsByList._CONFIGURATION_SECTION
