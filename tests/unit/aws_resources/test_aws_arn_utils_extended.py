import unittest

from aws_resources.aws_arn_utils import AwsArnUtils
from aws_resources.aws_resource_exceptions import InvalidArnException


class TestAwsArnUtilsEdgeCases(unittest.TestCase):
    """Extended tests for AwsArnUtils covering uncovered branches."""

    # --- isArn ---
    def test_isArn_none(self):
        self.assertFalse(AwsArnUtils.isArn(None))

    def test_isArn_empty(self):
        self.assertFalse(AwsArnUtils.isArn(""))

    def test_isArn_wrong_prefix(self):
        self.assertFalse(AwsArnUtils.isArn("not:an:arn:at:all:x"))

    def test_isArn_too_few_colons(self):
        self.assertFalse(AwsArnUtils.isArn("arn:aws:s3::"))
        self.assertTrue(AwsArnUtils.isArn("arn:aws:s3:::"))  # 5 colons is valid

    # --- getAwsObjectFromArn ---
    def test_getAwsObjectFromArn_invalid_arn_returns_none(self):
        self.assertIsNone(AwsArnUtils.getAwsObjectFromArn("not-an-arn"))

    def test_getAwsObjectFromArn_unknown_service_returns_none(self):
        self.assertIsNone(AwsArnUtils.getAwsObjectFromArn("arn:aws:ec2:us-east-1:123456:instance/i-123"))

    # --- get_s3_path_from_arn ---
    def test_get_s3_path_from_arn_none(self):
        self.assertIsNone(AwsArnUtils.get_s3_path_from_arn(None))

    def test_get_s3_path_from_arn_invalid_raises(self):
        with self.assertRaises(InvalidArnException):
            AwsArnUtils.get_s3_path_from_arn("not-an-arn")

    def test_get_s3_path_from_arn_non_s3_returns_none(self):
        result = AwsArnUtils.get_s3_path_from_arn("arn:aws:glue:us-east-1:123456:catalog")
        self.assertIsNone(result)

    # --- get_s3_arn_from_s3_path ---
    def test_get_s3_arn_from_s3_path_none(self):
        self.assertIsNone(AwsArnUtils.get_s3_arn_from_s3_path(None))

    def test_get_s3_arn_from_s3_path_invalid_raises(self):
        with self.assertRaises(InvalidArnException):
            AwsArnUtils.get_s3_arn_from_s3_path("http://not-s3/path")

    def test_get_s3_arn_from_s3a_path(self):
        result = AwsArnUtils.get_s3_arn_from_s3_path("s3a://bucket/path/")
        self.assertEqual(result, "arn:aws:s3:::bucket/path/")

    # --- isS3Arn / isGlueArn ---
    def test_isS3Arn_invalid_returns_false(self):
        self.assertFalse(AwsArnUtils.isS3Arn("not-an-arn"))

    def test_isGlueArn_invalid_returns_false(self):
        self.assertFalse(AwsArnUtils.isGlueArn("not-an-arn"))

    def test_isS3Arn_glue_returns_false(self):
        self.assertFalse(AwsArnUtils.isS3Arn("arn:aws:glue:us-east-1:123456:catalog"))

    def test_isGlueArn_s3_returns_false(self):
        self.assertFalse(AwsArnUtils.isGlueArn("arn:aws:s3:::mybucket"))

    # --- isS3BucketArn / isS3ObjectArn ---
    def test_isS3BucketArn_invalid_returns_false(self):
        self.assertFalse(AwsArnUtils.isS3BucketArn("not-an-arn"))

    def test_isS3ObjectArn_invalid_returns_false(self):
        self.assertFalse(AwsArnUtils.isS3ObjectArn("not-an-arn"))

    # --- isGlueCatalogArn ---
    def test_isGlueCatalogArn_invalid_returns_false(self):
        self.assertFalse(AwsArnUtils.isGlueCatalogArn("not-an-arn"))

    # --- isGlueDatabaseArn / isGlueTableArn raise on invalid ---
    def test_isGlueDatabaseArn_invalid_raises(self):
        with self.assertRaises(InvalidArnException):
            AwsArnUtils.isGlueDatabaseArn("not-an-arn")

    def test_isGlueTableArn_invalid_raises(self):
        with self.assertRaises(InvalidArnException):
            AwsArnUtils.isGlueTableArn("not-an-arn")

    # --- _split_arn ---
    def test_split_arn_invalid_raises(self):
        with self.assertRaises(InvalidArnException):
            AwsArnUtils._split_arn("not-an-arn")

    # --- generate_s3_bucket_arn ---
    def test_generate_s3_bucket_arn(self):
        self.assertEqual(AwsArnUtils.generate_s3_bucket_arn("mybucket"), "arn:aws:s3:::mybucket")

    # --- get_service_from_arn ---
    def test_get_service_from_glue_arn(self):
        self.assertEqual(AwsArnUtils.get_service_from_arn("arn:aws:glue:us-east-1:123456:catalog"), "glue")

    # --- table wildcard ARN ---
    def test_table_wildcard_arn(self):
        """arn:aws:glue:*:*:table/* should parse as table with wildcard db and table."""
        obj = AwsArnUtils.getAwsObjectFromArn("arn:aws:glue:*:*:table/*")
        self.assertIsNotNone(obj)
        self.assertEqual(obj.get_database(), "*")
        self.assertEqual(obj.get_name(), "*")


if __name__ == '__main__':
    unittest.main()
