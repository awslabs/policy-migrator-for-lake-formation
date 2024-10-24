from policy_readers.iam_policy_parser import IamPolicyParser
from policy_readers.policy_reader_interface import PolicyReaderInterface

from aws_resources.readers.s3_bucket_policy_reader import S3BucketPolicyPolicyReader
from permissions.permissions_list import PermissionsList
from config.application_configuration import ApplicationConfiguration

import logging
logger = logging.getLogger(__name__)

class S3BucketPermissionsPolicyReader(PolicyReaderInterface):
    '''
        This reader will go through the S3 bucket policy and return permissions in which 
        is explicity allowed to access the S3 bucket. 

            LIMITATIONS:
    - Wildcards in the S3 path that are not at the end ie s3://bucket/somelocation/*/someotherlocation/ is not currently supported
    '''

    #_REQUIRED_CONFIGURATION = { "include_cross_account_permissions" : "A boolean that determines if we should add LF permissions to cross account principals." }
    _REQUIRED_CONFIGURATION = { }
    _CONFIGURATION_SECTION = "policy_reader_s3_bucket_policies"

    def __init__(self, appConfig : ApplicationConfiguration, conf : dict[str]):
        super().__init__(appConfig, conf)
        self._permissions_list : PermissionsList = PermissionsList()
        self._iam_policy_parser : IamPolicyParser = IamPolicyParser(appConfig)
        self._s3_bucket_policy_reader : S3BucketPolicyPolicyReader = appConfig.get_s3_bucket_policy_reader()

    def read_policies(self) -> PermissionsList:
        logger.info("Reading policies.")

        for bucket_arn, bucket_policy in self._s3_bucket_policy_reader.get_all_policies():
            logger.debug(f"Reading policy for {bucket_arn} with bucket policy {bucket_policy}")
            self._iam_policy_parser.read_resource_policy_allow_policies(self._permissions_list, bucket_arn, bucket_policy)
            self._iam_policy_parser.read_resource_policy_deny_policies(self._permissions_list, bucket_arn, bucket_policy)

        return self._permissions_list

    @classmethod
    def get_name(cls):
        """Gets the name of this reader. """
        return S3BucketPermissionsPolicyReader.__name__

    @classmethod
    def get_required_configuration(cls) -> dict:
        """Returns the required configuration for this reader. """
        return S3BucketPermissionsPolicyReader._REQUIRED_CONFIGURATION

    @classmethod
    def get_config_section(cls) -> str:
        return S3BucketPermissionsPolicyReader._CONFIGURATION_SECTION