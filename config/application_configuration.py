

import boto3

from aws_resources.glue_data_catalog import GlueDataCatalog
from aws_resources.readers.glue_data_catalog_reader import GlueDataCatalogReaderAPI
from aws_resources.readers.iam_policy_reader import IamPolicyReader
from aws_resources.readers.s3_bucket_policy_reader import S3BucketPolicyPolicyReader
from config.boto3_factory import Boto3Factory
from lakeformation_utils.s3_to_table_mapper import S3ToTableMapper

import logging
logger = logging.getLogger(__name__)

class ApplicationConfiguration:
    '''
    This class is used to get configuration and other helper classes.
    '''

    def __init__(self, args : str, boto3_session : boto3.Session = None,
                 s3_bucket_policies : S3BucketPolicyPolicyReader = None,
                 glue_data_catalog : GlueDataCatalog = None,
                 iam_policy_reader : IamPolicyReader = None,
                 s3_to_table_translator : S3ToTableMapper = None,
                 account_id : str = None
                 ):
        self._args = args
        self._boto3_session = boto3_session
        self._s3_bucket_policies = s3_bucket_policies
        self._glue_data_catalog = glue_data_catalog
        self._iam_policy_reader = iam_policy_reader
        self._s3_to_table_translator = s3_to_table_translator
        self._account_id = account_id

    def get_args(self) -> dict[str | dict]:
        return self._args

    def get_boto3_session(self) -> boto3.Session:
        if self._boto3_session is None:
            self._boto3_session = Boto3Factory.createBoto3SessionFromConfiguration(self._args)

        return self._boto3_session

    def get_s3_bucket_policy_reader(self) -> S3BucketPolicyPolicyReader:
        if self._s3_bucket_policies is None:
            logger.info("Reading S3 bucket policies.")
            self._s3_bucket_policies = S3BucketPolicyPolicyReader(self.get_boto3_session())
            logger.info("Completed S3 bucket policies.")
        return self._s3_bucket_policies

    def get_glue_data_catalog(self) -> GlueDataCatalog:
        if self._glue_data_catalog is None:
            logger.info("Reading Glue Data Catalog.")
            gdcReader = GlueDataCatalogReaderAPI(self.get_boto3_session(), self.get_account_id())
            self._glue_data_catalog = gdcReader.read_catalog()
            logger.info("Completed Glue Data Catalog.")
        return self._glue_data_catalog

    def get_account_id(self) -> str:
        if self._account_id is None:
            sts_client = self.get_boto3_session().client('sts')
            self._account_id = sts_client.get_caller_identity()['Account']
        return self._account_id

    def get_iam_policy_reader(self) -> IamPolicyReader:
        if self._iam_policy_reader is None:
            self._iam_policy_reader = IamPolicyReader(self.get_boto3_session())
        return self._iam_policy_reader

    def get_s3_to_table_translator(self) -> S3ToTableMapper:
        if self._s3_to_table_translator is None:
            self._s3_to_table_translator = S3ToTableMapper(self.get_glue_data_catalog())
        return self._s3_to_table_translator
