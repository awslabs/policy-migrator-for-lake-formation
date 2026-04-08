import json
import unittest
from unittest.mock import Mock, MagicMock

import botocore.exceptions

from aws_resources.readers.s3_bucket_policy_reader import S3BucketPolicyPolicyReader


class TestS3BucketPolicyReader(unittest.TestCase):
    """Tests for S3BucketPolicyPolicyReader."""

    def _make_reader(self, s3_client):
        session = Mock()
        session.client.return_value = s3_client
        return S3BucketPolicyPolicyReader(session)

    def test_get_policy_reads_and_caches(self):
        policy = {"Version": "2012-10-17", "Statement": []}
        s3_client = Mock()
        s3_client.get_bucket_policy.return_value = {"Policy": json.dumps(policy)}

        reader = self._make_reader(s3_client)
        result = reader.get_policy("mybucket")
        self.assertEqual(result, policy)

        # Second call with the ARN key should use cache
        result2 = reader.get_policy("arn:aws:s3:::mybucket")
        self.assertEqual(result2, policy)
        # Only one API call — second was served from cache
        s3_client.get_bucket_policy.assert_called_once()

    def test_get_policy_no_policy_returns_none(self):
        s3_client = Mock()
        error_response = {"Error": {"Code": "NoSuchBucketPolicy", "Message": "No policy"}}
        s3_client.get_bucket_policy.side_effect = botocore.exceptions.ClientError(error_response, "GetBucketPolicy")

        reader = self._make_reader(s3_client)
        result = reader.get_policy("mybucket")
        self.assertIsNone(result)

    def test_get_policy_other_error_raises(self):
        s3_client = Mock()
        error_response = {"Error": {"Code": "AccessDenied", "Message": "Denied"}}
        s3_client.get_bucket_policy.side_effect = botocore.exceptions.ClientError(error_response, "GetBucketPolicy")

        reader = self._make_reader(s3_client)
        with self.assertRaises(botocore.exceptions.ClientError):
            reader.get_policy("mybucket")

    def test_get_all_policies_lists_and_reads(self):
        policy1 = {"Version": "2012-10-17", "Statement": [{"Effect": "Allow"}]}
        policy2 = {"Version": "2012-10-17", "Statement": [{"Effect": "Deny"}]}

        s3_client = Mock()
        s3_client.list_buckets.return_value = {
            "Buckets": [{"Name": "bucket1"}, {"Name": "bucket2"}]
        }
        s3_client.get_bucket_policy.side_effect = [
            {"Policy": json.dumps(policy1)},
            {"Policy": json.dumps(policy2)},
        ]

        reader = self._make_reader(s3_client)
        policies = dict(reader.get_all_policies())

        self.assertEqual(len(policies), 2)
        self.assertIn("arn:aws:s3:::bucket1", policies)
        self.assertIn("arn:aws:s3:::bucket2", policies)
        self.assertEqual(policies["arn:aws:s3:::bucket1"], policy1)

    def test_get_all_policies_skips_buckets_without_policy(self):
        policy1 = {"Version": "2012-10-17", "Statement": []}
        error_response = {"Error": {"Code": "NoSuchBucketPolicy", "Message": "No policy"}}

        s3_client = Mock()
        s3_client.list_buckets.return_value = {
            "Buckets": [{"Name": "bucket1"}, {"Name": "bucket2"}]
        }
        s3_client.get_bucket_policy.side_effect = [
            {"Policy": json.dumps(policy1)},
            botocore.exceptions.ClientError(error_response, "GetBucketPolicy"),
        ]

        reader = self._make_reader(s3_client)
        policies = dict(reader.get_all_policies())

        # Only bucket1 has a policy
        self.assertEqual(len(policies), 1)
        self.assertIn("arn:aws:s3:::bucket1", policies)

    def test_get_all_policies_caches_on_second_call(self):
        s3_client = Mock()
        s3_client.list_buckets.return_value = {"Buckets": []}

        reader = self._make_reader(s3_client)
        list(reader.get_all_policies())
        list(reader.get_all_policies())

        s3_client.list_buckets.assert_called_once()


if __name__ == '__main__':
    unittest.main()
