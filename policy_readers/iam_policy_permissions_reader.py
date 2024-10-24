
from policy_readers.iam_policy_parser import IamPolicyParser
from policy_readers.policy_reader_interface import PolicyReaderInterface

from aws_resources.readers.iam_policy_reader import IamPolicyReader

from permissions.permissions_list import PermissionsList
from config.application_configuration import ApplicationConfiguration

import logging

logger = logging.getLogger(__name__)

class IamPolicyPermissionsReader(PolicyReaderInterface):
    '''
    Reads IAM policies and returns a list of permissions from it.

    LIMITATIONS:
    - Wildcards in the S3 path that are not at the end ie s3://bucket/somelocation/*/someotherlocation/ is not currently supported
    '''

    _REQUIRED_CONFIGURATION = { }
    _CONFIGURATION_SECTION = "policy_reader_iam_permissions"

    def __init__(self, appConfig : ApplicationConfiguration, conf : dict[str]):
        super().__init__(appConfig, conf)
        self._iam_policy_reader : IamPolicyReader = appConfig.get_iam_policy_reader()
        self._permissions_list : PermissionsList = PermissionsList()
        self._iam_policy_parser : IamPolicyParser = IamPolicyParser(appConfig)

    def read_policies(self) -> PermissionsList:
        logger.info("Reading policies.")

        for principal, policies in self._iam_policy_reader.get_all_prinicial_policies():
            self._read_policies(principal, policies)

        return self._permissions_list

    def _read_policies(self, principal, policies):
        # We want to read all allow policies first, in which then all denys to remove any allow policies. This
        # cannot be done together because a deny may show up in a policy before an allow policy which wouldn't
        # be removed.
        for policy in policies:
            self._iam_policy_parser.read_iam_principal_allow_policies(self._permissions_list, principal, policy)

        for policy in policies:
            self._iam_policy_parser.read_iam_deny_policies(self._permissions_list, principal, policy)


    @classmethod
    def get_name(cls):
        return "IamPolicyReader"

    @classmethod
    def get_required_configuration(cls) -> dict:
        return IamPolicyPermissionsReader._REQUIRED_CONFIGURATION

    @classmethod
    def get_config_section(cls) -> str:
        return IamPolicyPermissionsReader._CONFIGURATION_SECTION
