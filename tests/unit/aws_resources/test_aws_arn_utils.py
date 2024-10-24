import unittest

from aws_resources.glue_catalog import GlueCatalog
from aws_resources.glue_database import GlueDatabase
from aws_resources.glue_table import GlueTable
from aws_resources.s3_bucket import S3Bucket
from aws_resources.s3_object import S3Object
from tests.unit.helpers.permissions_list_test_helpers import PermissionsListTestHelper
from aws_resources.aws_arn_utils import AwsArnUtils

class TestAwsArnUtils(unittest.TestCase):
    """
    Test Aws ARN Utils
    """

    def test_catalog_arns(self):
        catalog_id = PermissionsListTestHelper.test_catalog_id
        glueCatalogObj = AwsArnUtils.getAwsObjectFromArn(f"arn:aws:glue:us-east-1:{catalog_id}:catalog")

        self.assertTrue(isinstance(glueCatalogObj, GlueCatalog))
        self.assertEqual(glueCatalogObj.get_catalog_id(), catalog_id)

    def test_database_arn(self):
        catalog_id = PermissionsListTestHelper.test_catalog_id
        glueDatabaseObj = AwsArnUtils.getAwsObjectFromArn(f"arn:aws:glue:us-east-1:{catalog_id}:database/test_database")

        self.assertTrue(isinstance(glueDatabaseObj, GlueDatabase))
        self.assertEqual(glueDatabaseObj.get_catalog_id(), catalog_id)
        self.assertEqual(glueDatabaseObj.get_name(), "test_database")

    def test_table_arn(self):
        catalog_id = PermissionsListTestHelper.test_catalog_id
        glueTableObj : GlueTable = AwsArnUtils.getAwsObjectFromArn(f"arn:aws:glue:us-east-1:{catalog_id}:table/test_database/test_table")

        self.assertTrue(isinstance(glueTableObj, GlueTable))
        self.assertEqual(glueTableObj.get_catalog_id(), catalog_id)
        # pylint: disable=all
        self.assertEqual(glueTableObj.get_database(), "test_database")
        self.assertEqual(glueTableObj.get_name(), "test_table")

    def test_s3_bucket_arn(self):
        s3BucketObj = AwsArnUtils.getAwsObjectFromArn("arn:aws:s3:::my_bucket")

        self.assertIsNotNone(s3BucketObj)
        self.assertTrue(isinstance(s3BucketObj, S3Bucket))
        self.assertEqual(s3BucketObj.get_bucket_name(), "my_bucket")
        self.assertEqual(s3BucketObj.get_partition(), "aws")

    def test_s3_object_arn(self):
        s3ObjectObj = AwsArnUtils.getAwsObjectFromArn("arn:aws:s3:::my_bucket/mytable/mypartition/some_object.parq")

        self.assertIsNotNone(s3ObjectObj)
        self.assertTrue(isinstance(s3ObjectObj, S3Object))
        self.assertEqual(s3ObjectObj.get_bucket_name(), "my_bucket")
        self.assertEqual(s3ObjectObj.get_partition(), "aws")
        self.assertEqual(s3ObjectObj.get_key(), "mytable/mypartition/some_object.parq")

    def test_s3_object_arn(self):
        s3path = AwsArnUtils.get_s3_path_from_arn("arn:aws:s3:::mybucket/test_database2/test_table/object2.parq")
        self.assertEqual(s3path, "s3://mybucket/test_database2/test_table/object2.parq")

    def test_get_service_from_arn(self):
        self.assertEqual(AwsArnUtils.get_service_from_arn("arn:aws:s3:::mybucket/test_database2/test_table/object2.parq"), "s3")

    def test_get_s3_arn_from_s3_path(self):
        self.assertEqual(AwsArnUtils.get_s3_arn_from_s3_path("s3://bucket/path1/path2/path3"), "arn:aws:s3:::bucket/path1/path2/path3/")
        self.assertEqual(AwsArnUtils.get_s3_arn_from_s3_path("s3://bucket/"), "arn:aws:s3:::bucket/")
        self.assertEqual(AwsArnUtils.get_s3_arn_from_s3_path("s3://bucket/path1/path2/"), "arn:aws:s3:::bucket/path1/path2/")

if __name__ == '__main__':
    unittest.main()
