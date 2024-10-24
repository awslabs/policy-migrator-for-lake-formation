import boto3
import botocore
import json
import logging

from aws_resources.aws_arn_utils import AwsArnUtils

logger = logging.getLogger(__name__)

class S3BucketPolicyPolicyReader:
    '''
        Stores all the S3 bucket policies for easy retrieval later on.
        Required Permissions: s3:ListAllMyBuckets
    '''

    def __init__(self, boto3Session : boto3.Session ):
        self._s3_client = boto3Session.client('s3')
        self._s3_bucket_policies : dict[str, str | dict]= {}
        self._initialized = False

    def get_policy(self, s3BucketName) -> dict[str, str | dict] | None:
        if s3BucketName not in self._s3_bucket_policies:
            return self._read_policy(s3BucketName)

        return self._s3_bucket_policies[s3BucketName]

    def get_all_policies(self) -> iter:
        '''
            Returns an iterator of all the bucket policies. The iterator returns 2 parameters,
            bucket ARN (not name) [str], and a list of policies (list[str]).
        '''
        if self._initialized:
            return iter(self._s3_bucket_policies.items())
        buckets = self._s3_client.list_buckets()

        for bucket in buckets['Buckets']:
            self._read_policy(bucket['Name'])

        self._initialized = True
        return iter(self._s3_bucket_policies.items())

    def _read_policy(self, bucketName) -> dict[str, str | dict] | None:
        logger.debug(f"Reading policy for bucket {bucketName}.")
        try:
            policy = self._s3_client.get_bucket_policy(Bucket=bucketName)['Policy']
            policy_obj = json.loads(policy)
            self._s3_bucket_policies[AwsArnUtils.generate_s3_bucket_arn(bucketName)] = policy_obj
            return policy_obj
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchBucketPolicy':
                logger.debug(f"No policy found for bucket {bucketName}.: {e}")
                return None
            logger.error(f"Failed to get bucket policy for bucket {bucketName}: {e}")
            raise e
        except Exception as e:
            logger.error(f"Failed to get bucket policy for bucket {bucketName}: {e}")
            raise e
