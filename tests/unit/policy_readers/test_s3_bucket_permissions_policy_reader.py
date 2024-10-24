import unittest

from aws_resources.actions.s3_action import S3Action
from aws_resources.glue_table import GlueTable
from lakeformation_utils.s3_to_table_mapper import S3ToTableMapper
from permissions.permissions_list import PermissionsList
from policy_readers.s3_bucket_permissions_policy_reader import S3BucketPermissionsPolicyReader
from tests.unit.helpers.permissions_list_test_helpers import PermissionsListTestHelper

import unittest.mock
from unittest.mock import Mock

class TestS3BucketPermissionsPolicyReader(unittest.TestCase):

    CATALOG_ID = PermissionsListTestHelper.test_catalog_id
    GLUE_DATA_CATALOG = PermissionsListTestHelper.create_glue_data_catalog()
    S3_TO_TABLE_MAPPER = S3ToTableMapper(GLUE_DATA_CATALOG)

    ALL_S3_ACTIONS = set(S3Action.get_s3_actions_with_wildcard("*"))

    def setUp(self):
        self._s3_bucket_policy_reader = Mock()
        self._appConfig = Mock()
        self._appConfig.get_glue_data_catalog.return_value = self.GLUE_DATA_CATALOG
        self._appConfig.get_s3_bucket_policy_reader.return_value = self._s3_bucket_policy_reader
        self._appConfig.get_s3_to_table_translator.return_value = self.S3_TO_TABLE_MAPPER

        iam_policy_reader = Mock()
        self._appConfig.get_iam_policy_reader.return_value = iam_policy_reader
        iam_policy_reader.get_all_principal_arns.return_value = ([], ["arn:aws:iam:012345678901::role/role1", "arn:aws:iam:012345678901::role/role2"])

    def test_s3_multiple_principals_policy(self):
        self._s3_bucket_policy_reader.get_all_policies.return_value = iter({ "arn:aws:s3:::mybucket" :
            {
	                "Version": "2012-10-17",
	                "Statement": [
                        {
                            "Effect": "Allow",
                            "Action": [ "s3:GetObject" ],
                            "Principal": {
                                "AWS" : [ 
                                "arn:aws:iam:012345678901::role/role1",
                                "arn:aws:iam:012345678901::role/role2"
                                ] 
                            },
                            "Resource": "arn:aws:s3:::mybucket/test_database/*"
                        }
	                ]
            }
         }.items())

        tables : set[GlueTable] = set(TestS3BucketPermissionsPolicyReader.S3_TO_TABLE_MAPPER.get_all_tables_from_s3_arn_prefix("arn:aws:s3:::mybucket/test_database/"))

        tablesList = list(tables)
        tablesList.sort()

        self.assertEqual(len(tables), 3)

        policy_reader = S3BucketPermissionsPolicyReader(self._appConfig, {})

        permissionsList : PermissionsList = policy_reader.read_policies()

        self.assertEqual(len(permissionsList.get_permissions()), 6)
        permissionsAsList = permissionsList.get_permissions()
        permissionsAsList.sort()

        self.assertSetEqual(permissionsAsList[0].permission_actions(), {"s3:GetObject"})
        self.assertEqual(permissionsAsList[0].principal_arn(), "arn:aws:iam:012345678901::role/role1")
        self.assertEqual(permissionsAsList[0].resource_arn(), "arn:aws:s3:::mybucket/test_database/test_table/*")

        self.assertSetEqual(permissionsAsList[1].permission_actions(), {"s3:GetObject"})
        self.assertEqual(permissionsAsList[1].principal_arn(), "arn:aws:iam:012345678901::role/role1")
        self.assertEqual(permissionsAsList[1].resource_arn(), "arn:aws:s3:::mybucket/test_database/test_table2/*")

        self.assertSetEqual(permissionsAsList[2].permission_actions(), {"s3:GetObject"})
        self.assertEqual(permissionsAsList[2].principal_arn(), "arn:aws:iam:012345678901::role/role1")
        self.assertEqual(permissionsAsList[2].resource_arn(), "arn:aws:s3:::mybucket/test_database/test_table3/*")

        self.assertSetEqual(permissionsAsList[3].permission_actions(), {"s3:GetObject"})
        self.assertEqual(permissionsAsList[3].principal_arn(), "arn:aws:iam:012345678901::role/role2")
        self.assertEqual(permissionsAsList[3].resource_arn(), "arn:aws:s3:::mybucket/test_database/test_table/*")

        self.assertSetEqual(permissionsAsList[4].permission_actions(), {"s3:GetObject"})
        self.assertEqual(permissionsAsList[4].principal_arn(), "arn:aws:iam:012345678901::role/role2")
        self.assertEqual(permissionsAsList[4].resource_arn(), "arn:aws:s3:::mybucket/test_database/test_table2/*")

        self.assertSetEqual(permissionsAsList[5].permission_actions(), {"s3:GetObject"})
        self.assertEqual(permissionsAsList[5].principal_arn(), "arn:aws:iam:012345678901::role/role2")
        self.assertEqual(permissionsAsList[5].resource_arn(), "arn:aws:s3:::mybucket/test_database/test_table3/*")

    def test_s3_action_resource_wildcard_principals_policy(self):
        self._s3_bucket_policy_reader.get_all_policies.return_value = iter({ "arn:aws:s3:::mybucket" :
            {
	                "Version": "2012-10-17",
	                "Statement": [
                        {
                            "Effect": "Allow",
                            "Action": "*",
                            "Principal": { "AWS" : "*" },
                            "Resource": "arn:aws:s3:::mybucket/test_database/*"
                        }
	                ]
            }
         }.items())

        tables : set[GlueTable] = set(TestS3BucketPermissionsPolicyReader.S3_TO_TABLE_MAPPER.get_all_tables_from_s3_arn_prefix("arn:aws:s3:::mybucket/test_database/"))

        tablesList = list(tables)
        tablesList.sort()

        self.assertEqual(len(tables), 3)

        policy_reader = S3BucketPermissionsPolicyReader(self._appConfig, {})

        permissionsList : PermissionsList = policy_reader.read_policies()

        self.assertEqual(len(permissionsList.get_permissions()), 6)
        permissionsAsList = permissionsList.get_permissions()
        permissionsAsList.sort()

        self.assertSetEqual(permissionsAsList[0].permission_actions(), TestS3BucketPermissionsPolicyReader.ALL_S3_ACTIONS)
        self.assertEqual(permissionsAsList[0].principal_arn(), "arn:aws:iam:012345678901::role/role1")
        self.assertEqual(permissionsAsList[0].resource_arn(), "arn:aws:s3:::mybucket/test_database/test_table/*")

        self.assertSetEqual(permissionsAsList[1].permission_actions(), TestS3BucketPermissionsPolicyReader.ALL_S3_ACTIONS)
        self.assertEqual(permissionsAsList[1].principal_arn(), "arn:aws:iam:012345678901::role/role1")
        self.assertEqual(permissionsAsList[1].resource_arn(), "arn:aws:s3:::mybucket/test_database/test_table2/*")

        self.assertSetEqual(permissionsAsList[2].permission_actions(), TestS3BucketPermissionsPolicyReader.ALL_S3_ACTIONS)
        self.assertEqual(permissionsAsList[2].principal_arn(), "arn:aws:iam:012345678901::role/role1")
        self.assertEqual(permissionsAsList[2].resource_arn(), "arn:aws:s3:::mybucket/test_database/test_table3/*")

        self.assertSetEqual(permissionsAsList[3].permission_actions(), TestS3BucketPermissionsPolicyReader.ALL_S3_ACTIONS)
        self.assertEqual(permissionsAsList[3].principal_arn(), "arn:aws:iam:012345678901::role/role2")
        self.assertEqual(permissionsAsList[3].resource_arn(), "arn:aws:s3:::mybucket/test_database/test_table/*")

        self.assertSetEqual(permissionsAsList[4].permission_actions(), TestS3BucketPermissionsPolicyReader.ALL_S3_ACTIONS)
        self.assertEqual(permissionsAsList[4].principal_arn(), "arn:aws:iam:012345678901::role/role2")
        self.assertEqual(permissionsAsList[4].resource_arn(), "arn:aws:s3:::mybucket/test_database/test_table2/*")

        self.assertSetEqual(permissionsAsList[5].permission_actions(), TestS3BucketPermissionsPolicyReader.ALL_S3_ACTIONS)
        self.assertEqual(permissionsAsList[5].principal_arn(), "arn:aws:iam:012345678901::role/role2")
        self.assertEqual(permissionsAsList[5].resource_arn(), "arn:aws:s3:::mybucket/test_database/test_table3/*")

    def test_s3_action_resource_wildcard_array_principals_policy(self):
        self._s3_bucket_policy_reader.get_all_policies.return_value = iter({ "arn:aws:s3:::mybucket" :
            {
	                "Version": "2012-10-17",
	                "Statement": [
                        {
                            "Effect": "Allow",
                            "Action": [ "*" ],
                            "Principal": { "AWS" : [ "*" ] },
                            "Resource": "arn:aws:s3:::mybucket/test_database/*"
                        }
	                ]
            }
         }.items())

        tables : set[GlueTable] = set(TestS3BucketPermissionsPolicyReader.S3_TO_TABLE_MAPPER.get_all_tables_from_s3_arn_prefix("arn:aws:s3:::mybucket/test_database/"))

        tablesList = list(tables)
        tablesList.sort()

        self.assertEqual(len(tables), 3)

        policy_reader = S3BucketPermissionsPolicyReader(self._appConfig, {})

        permissionsList : PermissionsList = policy_reader.read_policies()

        self.assertEqual(len(permissionsList.get_permissions()), 6)
        permissionsAsList = permissionsList.get_permissions()
        permissionsAsList.sort()

        self.assertSetEqual(permissionsAsList[0].permission_actions(), TestS3BucketPermissionsPolicyReader.ALL_S3_ACTIONS)
        self.assertEqual(permissionsAsList[0].principal_arn(), "arn:aws:iam:012345678901::role/role1")
        self.assertEqual(permissionsAsList[0].resource_arn(), "arn:aws:s3:::mybucket/test_database/test_table/*")

        self.assertSetEqual(permissionsAsList[1].permission_actions(), TestS3BucketPermissionsPolicyReader.ALL_S3_ACTIONS)
        self.assertEqual(permissionsAsList[1].principal_arn(), "arn:aws:iam:012345678901::role/role1")
        self.assertEqual(permissionsAsList[1].resource_arn(), "arn:aws:s3:::mybucket/test_database/test_table2/*")

        self.assertSetEqual(permissionsAsList[2].permission_actions(), TestS3BucketPermissionsPolicyReader.ALL_S3_ACTIONS)
        self.assertEqual(permissionsAsList[2].principal_arn(), "arn:aws:iam:012345678901::role/role1")
        self.assertEqual(permissionsAsList[2].resource_arn(), "arn:aws:s3:::mybucket/test_database/test_table3/*")

        self.assertSetEqual(permissionsAsList[3].permission_actions(), TestS3BucketPermissionsPolicyReader.ALL_S3_ACTIONS)
        self.assertEqual(permissionsAsList[3].principal_arn(), "arn:aws:iam:012345678901::role/role2")
        self.assertEqual(permissionsAsList[3].resource_arn(), "arn:aws:s3:::mybucket/test_database/test_table/*")

        self.assertSetEqual(permissionsAsList[4].permission_actions(), TestS3BucketPermissionsPolicyReader.ALL_S3_ACTIONS)
        self.assertEqual(permissionsAsList[4].principal_arn(), "arn:aws:iam:012345678901::role/role2")
        self.assertEqual(permissionsAsList[4].resource_arn(), "arn:aws:s3:::mybucket/test_database/test_table2/*")

        self.assertSetEqual(permissionsAsList[5].permission_actions(), TestS3BucketPermissionsPolicyReader.ALL_S3_ACTIONS)
        self.assertEqual(permissionsAsList[5].principal_arn(), "arn:aws:iam:012345678901::role/role2")
        self.assertEqual(permissionsAsList[5].resource_arn(), "arn:aws:s3:::mybucket/test_database/test_table3/*")

    def test_s3_multiple_policies_with_deny(self):
        self._s3_bucket_policy_reader.get_all_policies.return_value = iter({ "arn:aws:s3:::mybucket" :
            {
	                "Version": "2012-10-17",
	                "Statement": [
                        {
                            "Effect": "Deny",
                            "Action": [ "s3:GetObject" ],
                            "Principal": { "AWS" : [ "arn:aws:iam:012345678901::role/role1" ] },
                            "Resource": "arn:aws:s3:::mybucket/test_database/test_table3/*"
                        },
                        {
                            "Effect": "Allow",
                            "Action": [ "*" ],
                            "Principal": { "AWS" : [ "*" ] },
                            "Resource": "arn:aws:s3:::mybucket/test_database/*"
                        }
	                ]
            }
        }.items())

        tables : set[GlueTable] = set(TestS3BucketPermissionsPolicyReader.S3_TO_TABLE_MAPPER.get_all_tables_from_s3_arn_prefix("arn:aws:s3:::mybucket/test_database/"))

        tablesList = list(tables)
        tablesList.sort()

        self.assertEqual(len(tables), 3)

        policy_reader = S3BucketPermissionsPolicyReader(self._appConfig, {})

        permissionsList : PermissionsList = policy_reader.read_policies()

        self.assertEqual(len(permissionsList.get_permissions()), 6)
        permissionsAsList = permissionsList.get_permissions()
        permissionsAsList.sort()

        allS3ActionsExceptGetObject = TestS3BucketPermissionsPolicyReader.ALL_S3_ACTIONS.copy()
        allS3ActionsExceptGetObject.remove("s3:GetObject")

        self.assertSetEqual(permissionsAsList[0].permission_actions(), TestS3BucketPermissionsPolicyReader.ALL_S3_ACTIONS)
        self.assertEqual(permissionsAsList[0].principal_arn(), "arn:aws:iam:012345678901::role/role1")
        self.assertEqual(permissionsAsList[0].resource_arn(), "arn:aws:s3:::mybucket/test_database/test_table/*")

        self.assertSetEqual(permissionsAsList[1].permission_actions(), TestS3BucketPermissionsPolicyReader.ALL_S3_ACTIONS)
        self.assertEqual(permissionsAsList[1].principal_arn(), "arn:aws:iam:012345678901::role/role1")
        self.assertEqual(permissionsAsList[1].resource_arn(), "arn:aws:s3:::mybucket/test_database/test_table2/*")

        self.assertSetEqual(permissionsAsList[2].permission_actions(), allS3ActionsExceptGetObject)
        self.assertEqual(permissionsAsList[2].principal_arn(), "arn:aws:iam:012345678901::role/role1")
        self.assertEqual(permissionsAsList[2].resource_arn(), "arn:aws:s3:::mybucket/test_database/test_table3/*")

        self.assertSetEqual(permissionsAsList[3].permission_actions(), TestS3BucketPermissionsPolicyReader.ALL_S3_ACTIONS)
        self.assertEqual(permissionsAsList[3].principal_arn(), "arn:aws:iam:012345678901::role/role2")
        self.assertEqual(permissionsAsList[3].resource_arn(), "arn:aws:s3:::mybucket/test_database/test_table/*")

        self.assertSetEqual(permissionsAsList[4].permission_actions(), TestS3BucketPermissionsPolicyReader.ALL_S3_ACTIONS)
        self.assertEqual(permissionsAsList[4].principal_arn(), "arn:aws:iam:012345678901::role/role2")
        self.assertEqual(permissionsAsList[4].resource_arn(), "arn:aws:s3:::mybucket/test_database/test_table2/*")

        self.assertSetEqual(permissionsAsList[5].permission_actions(), TestS3BucketPermissionsPolicyReader.ALL_S3_ACTIONS)
        self.assertEqual(permissionsAsList[5].principal_arn(), "arn:aws:iam:012345678901::role/role2")
        self.assertEqual(permissionsAsList[5].resource_arn(), "arn:aws:s3:::mybucket/test_database/test_table3/*")

if __name__ == '__main__':
    unittest.main()
