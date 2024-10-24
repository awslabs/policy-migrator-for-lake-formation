import re
import logging

from .glue_catalog import GlueCatalog
from .glue_database import GlueDatabase
from .glue_table import GlueTable
from .s3_bucket import S3Bucket
from .s3_object import S3Object

from .aws_resource_exceptions import InvalidArnException

logger = logging.getLogger(__name__)

class AwsArnUtils():
    '''
        A helper class to parse AWS ARNs.
    '''

    CATALOG_ARN_REGEX = r'^arn:aws:glue:.*:.*:catalog'      #arn:aws:glue:region:account-id:catalog
    DATABASE_ARN_REGEX = r'^arn:aws:glue:.*:.*:database\/.*' #arn:aws:glue:region:account-id:database/database name
    TABLE_ARN_REGEX = r'^arn:aws:glue:.*:.*:table\/.*(\/.*)?'    #arn:aws:glue:region:account-id:table/database name/table name
    S3_BUCKET_ARN_REGEX = r'^arn:aws:s3[a]?:::[^\/]+'            #arn:aws:s3:::bucket_name
    S3_OBJECT_ARN_REGEX = r'^arn:aws:s3[a]?:::[^\/]+(\/.*)+\/*' #arn:aws:s3:::bucket_name/key_name

    @staticmethod
    def getAwsObjectFromArn(arn : str) -> GlueCatalog | GlueDatabase | GlueTable | S3Bucket | S3Object | None:
        '''
            Returns an AWS object from an ARN.
        '''
        if not AwsArnUtils.isArn(arn):
            logger.warning(f"Invalid ARN format: {arn}")
            return None

        partition, _, region, account, resource = AwsArnUtils._split_arn(arn)

        if AwsArnUtils.isGlueCatalogArn(arn):
            return GlueCatalog(region, account)
        if AwsArnUtils.isGlueDatabaseArn(arn):
            database, table = AwsArnUtils._get_database_and_table_from_arn(arn)
            return GlueDatabase(region, account, database)
        if AwsArnUtils.isGlueTableArn(arn):
            database, table = AwsArnUtils._get_database_and_table_from_arn(arn)
            return GlueTable(region, account, database, table)
        if AwsArnUtils.isS3BucketArn(arn):
            bucket, _ = AwsArnUtils._get_s3_bucket_and_key_from_arn(arn)
            return S3Bucket(partition, region, account, resource)
        if AwsArnUtils.isS3ObjectArn(arn):
            bucket, prefix = AwsArnUtils._get_s3_bucket_and_key_from_arn(arn)
            return S3Object(partition, region, account, bucket, prefix)

        logger.debug(f"Unknown object from Arn: {arn}")
        return None

    @staticmethod
    def get_s3_path_from_arn(arn : str) -> str:
        '''
            Returns the S3 path from an ARN.
        '''
        if arn is None:
            return None

        if not AwsArnUtils.isArn(arn):
            raise InvalidArnException(f"Invalid ARN format: {arn}")

        if not AwsArnUtils.isS3Arn(arn):
            return None

        return f"s3://{arn.split(":")[5]}"

    @staticmethod
    def get_s3_arn_from_s3_path(s3_path : str) -> str:
        '''
            Returns the S3 ARN from an S3 path.
        '''
        if s3_path is None:
            return None

        if not s3_path.startswith("s3://") and not s3_path.startswith("s3a://"):
            raise InvalidArnException(f"Invalid S3 path format: {s3_path}")

        if s3_path.endswith("/"):
            s3_path = s3_path[:-1]

        s3_parts = s3_path.split("/")
        bucket_name = s3_parts[2]
        return f"arn:aws:s3:::{bucket_name}/{"".join([part + "/" for part in s3_parts[3:]])}"

    @staticmethod
    def isS3Arn(arn : str) -> bool:
        '''
            Returns true if the ARN is an S3 ARN.
        '''
        if not AwsArnUtils.isArn(arn):
            return False

        try:
            return arn.split(':')[2] == 's3'
        except IndexError as ie:
            logger.error(f"isS3Arn: Invalid ARN format: {arn}")
            raise ie

    @staticmethod
    def isGlueArn(arn : str) -> bool:
        '''
            Returns true if the ARN is an Glue ARN.
        '''
        if not AwsArnUtils.isArn(arn):
            return False

        try:
            return arn.split(':')[2] == 'glue'
        except IndexError as ie:
            logger.error(f"isGlueArn: Invalid ARN format: {arn}")
            raise ie

    @staticmethod
    def isS3BucketArn(arn : str) -> bool:
        '''
            Returns true if the ARN is an S3 bucket ARN.
        '''

        if not AwsArnUtils.isArn(arn):
            return False

        return re.fullmatch(AwsArnUtils.S3_BUCKET_ARN_REGEX, arn) is not None

    @staticmethod
    def isS3ObjectArn(arn : str) -> bool:
        '''
            Returns true if the ARN is an S3 object ARN.
        '''
        if not AwsArnUtils.isArn(arn):
            return False

        return re.fullmatch(AwsArnUtils.S3_OBJECT_ARN_REGEX, arn) is not None

    @staticmethod
    def isGlueCatalogArn(arn : str) -> bool:
        '''
            Returns true if the ARN is an Glue catalog ARN.
        '''
        if not AwsArnUtils.isArn(arn):
            return False

        return re.fullmatch(AwsArnUtils.CATALOG_ARN_REGEX, arn) is not None

    @staticmethod
    def isGlueDatabaseArn(arn : str) -> bool:
        '''
            Returns true if the ARN is an Glue database ARN.
        '''
        if not AwsArnUtils.isArn(arn):
            raise InvalidArnException(f"Invalid ARN format: {arn}")

        return re.fullmatch(AwsArnUtils.DATABASE_ARN_REGEX, arn) is not None

    @staticmethod
    def isGlueTableArn(arn : str) -> bool:
        '''
            Returns true if the ARN is an Glue table ARN.
        '''
        if not AwsArnUtils.isArn(arn):
            raise InvalidArnException(f"Invalid ARN format: {arn}")

        return re.fullmatch(AwsArnUtils.TABLE_ARN_REGEX, arn) is not None

    @staticmethod
    def isArn(arn : str) -> bool:
        return arn is not None and arn.startswith("arn:aws:") and arn.count(':') == 5

    @staticmethod
    def generate_s3_bucket_arn(bucket_name : str) -> str:
        '''
            Generates an S3 bucket ARN.
        '''
        return f"arn:aws:s3:::{bucket_name}"

    @staticmethod
    def get_service_from_arn(resource_arn : str) -> str:
        return AwsArnUtils._split_arn(resource_arn)[1]

    @staticmethod
    def _get_database_and_table_from_arn(arn):
        resource_parts = arn.split(':')[5].split("/")
        if resource_parts[0] == 'database':
            return resource_parts[1], None
        if resource_parts[0] == 'table':
            # We can have an ARN like "arn:aws:glue:*:*:table/*"
            if len(resource_parts) < 3:
                if resource_parts[1] != '*':
                    logger.error(f"Invalid ARN format for Glue Database/Table: {arn}")
                    return None, None
                return resource_parts[1], '*'
            return resource_parts[1], resource_parts[2]

        logger.error(f"Invalid ARN format for Glue Database/Table: {arn}")
        return None, None

    @staticmethod
    def _get_s3_bucket_and_key_from_arn(arn):
        object_part = arn.split(':')[5]
        if object_part.find('/') > 0:
            loc = object_part.find('/')
            bucket = object_part[:loc]
            prefix = object_part[loc+1:]
        else:
            bucket = object_part
            prefix = None
        return bucket, prefix

    @staticmethod
    def _split_arn(arn) -> tuple[str, str, str, str, str]:
        if AwsArnUtils.isArn(arn) is False:
            raise InvalidArnException(f"Invalid ARN format: {arn}")
        arn_splits = arn.split(':')
        return arn_splits[1], arn_splits[2], arn_splits[3], \
                arn_splits[4], arn_splits[5]
