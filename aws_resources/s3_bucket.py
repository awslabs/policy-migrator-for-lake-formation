from .aws_resource import AwsObject

class S3Bucket(AwsObject):
    """
    Represents an S3 bucket
    """

    _partition : str
    _region : str
    _account_id : str
    _bucket_name : str

    def __init__(self, partition : str, region : str, account_id : str, bucket_name : str):
        self._partition = partition
        self._region = region
        self._account_id = account_id
        self._bucket_name = bucket_name

    def get_partition(self) -> str:
        return self._partition

    def get_region(self) -> str:
        return self._region

    def get_account_id(self) -> str:
        return self._account_id

    def get_bucket_name(self) -> str:
        return self._bucket_name

    def get_arn(self) -> str:
        return f"arn:{self._partition}:s3:::{self._bucket_name}"

    def __str__(self):
        return f"S3Bucket(partition={self._partition}, region={self._region}, account_id={self._account_id}, bucket_name={self._bucket_name})"
