from permissions.permissions_list import PermissionsList
from .policy_filter_interface import PolicyFilterInterface

from config.application_configuration import ApplicationConfiguration
from config.config_helper import ConfigHelper

import logging

logger = logging.getLogger(__name__)


class IamFilterPrincipalsByList(PolicyFilterInterface):
    '''
    Filters permissions based on a configured list of IAM principal ARNs.

    Supports two mutually exclusive modes:
      - include_list: Only keep permissions for principals in this list. All others are filtered out.
      - exclude_list: Filter out permissions for principals in this list. All others are kept.

    If both are specified, the filter will raise an error.
    If neither is specified, no filtering is performed.

    Configuration example (INI):
        [policy_filter_principals_by_list]
        enabled = true
        # Use ONE of the following (not both):
        include_list = arn:aws:iam::123456789012:role/RoleA, arn:aws:iam::123456789012:role/RoleB
        # exclude_list = arn:aws:iam::123456789012:role/ServiceRole, arn:aws:iam::123456789012:user/OldUser
    '''

    _REQUIRED_CONFIGURATION = {
        "include_list": "(Optional) Comma-separated list of principal ARNs to include. All others are filtered out.",
        "exclude_list": "(Optional) Comma-separated list of principal ARNs to exclude. All others are kept.",
    }
    _CONFIGURATION_SECTION = "policy_filter_principals_by_list"

    def __init__(self, appConfig: ApplicationConfiguration, conf: dict[str]):
        super().__init__(appConfig, conf)
        logger.info(f"Filter {self.get_name()} initialized.")

        include_raw = ConfigHelper.get_config_string(conf, "include_list")
        exclude_raw = ConfigHelper.get_config_string(conf, "exclude_list")

        if include_raw and exclude_raw:
            raise ValueError(
                f"{self.get_name()}: include_list and exclude_list are mutually exclusive. Specify only one."
            )

        self._include_set = self._parse_list(include_raw) if include_raw else None
        self._exclude_set = self._parse_list(exclude_raw) if exclude_raw else None

        if self._include_set is not None:
            logger.info(f"{self.get_name()}: Using include_list with {len(self._include_set)} principals.")
            logger.debug(f"{self.get_name()}: include_list principals: {self._include_set}")
        if self._exclude_set is not None:
            logger.info(f"{self.get_name()}: Using exclude_list with {len(self._exclude_set)} principals.")
            logger.debug(f"{self.get_name()}: exclude_list principals: {self._exclude_set}")

        if self._include_set is None and self._exclude_set is None:
            logger.info(f"{self.get_name()}: No include_list or exclude_list configured. No filtering will be performed.")

    def filter_policies(self, permissionsList: PermissionsList) -> PermissionsList:
        total_permissions = permissionsList.get_permissions_count()
        mode = "include_list" if self._include_set is not None else "exclude_list" if self._exclude_set is not None else "none"
        logger.info(f"{self.get_name()}: Starting to filter principals. Mode: {mode}. Input permissions count: {total_permissions}")

        if self._include_set is None and self._exclude_set is None:
            logger.info(f"{self.get_name()}: No filtering configured. Skipping.")
            return self.get_filtered_permissions()

        for permission in permissionsList.get_permissions():
            principal = permission.principal_arn()

            if self._include_set is not None and principal not in self._include_set:
                logger.debug(f"Filtering principal not in include_list: {principal}, resource: {permission.resource_arn()}")
                self._add_filtered_permission_record(permission)
            elif self._exclude_set is not None and principal in self._exclude_set:
                logger.debug(f"Filtering principal in exclude_list: {principal}, resource: {permission.resource_arn()}")
                self._add_filtered_permission_record(permission)
            else:
                logger.debug(f"Keeping principal: {principal}, resource: {permission.resource_arn()}")

        logger.info(f"{self.get_name()}: Completed. Filtered {self.get_number_filtered()} permissions out of {total_permissions}.")
        return self.get_filtered_permissions()

    @staticmethod
    def _parse_list(raw: str) -> set[str]:
        """Parse a comma-separated or newline-separated string into a set of trimmed, non-empty values."""
        # Split on commas first, then split each part on newlines to support both formats
        items = set()
        for part in raw.split(","):
            for line in part.split("\n"):
                stripped = line.strip()
                if stripped:
                    items.add(stripped)
        return items

    @classmethod
    def get_name(cls) -> str:
        return IamFilterPrincipalsByList.__name__

    @classmethod
    def get_required_configuration(cls) -> dict:
        return IamFilterPrincipalsByList._REQUIRED_CONFIGURATION

    @classmethod
    def get_config_section(cls) -> str:
        return IamFilterPrincipalsByList._CONFIGURATION_SECTION
