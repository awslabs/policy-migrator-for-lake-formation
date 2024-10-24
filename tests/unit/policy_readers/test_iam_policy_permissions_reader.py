import unittest

from aws_resources.glue_table import GlueTable
from lakeformation_utils.s3_to_table_mapper import S3ToTableMapper
from permissions.permissions_list import PermissionsList
from policy_readers.iam_policy_permissions_reader import IamPolicyPermissionsReader
from tests.unit.helpers.permissions_list_test_helpers import PermissionsListTestHelper

import unittest.mock
from unittest.mock import Mock

import logging

class TestIAMPolicyPermissionsReader(unittest.TestCase):

    CATALOG_ID = PermissionsListTestHelper.test_catalog_id
    GLUE_DATA_CATALOG = PermissionsListTestHelper.create_glue_data_catalog()
    S3_TO_TABLE_MAPPER = S3ToTableMapper(GLUE_DATA_CATALOG)

    def setUp(self):
        self._iam_policy_reader = Mock()
        self._appConfig = Mock()
        self._appConfig.get_glue_data_catalog.return_value = self.GLUE_DATA_CATALOG
        self._appConfig.get_iam_policy_reader.return_value = self._iam_policy_reader
        self._appConfig.get_s3_to_table_translator.return_value = self.S3_TO_TABLE_MAPPER

    def test_glue_simple_statement_policy(self):
        logging.getLogger().setLevel(logging.DEBUG)
        self._iam_policy_reader.get_all_prinicial_policies.return_value = iter({ "arn:aws:iam::012345678901:role/role1" : [
            {
	                "Version": "2012-10-17",
	                "Statement": [
                        {
                            "Effect": "Allow",
                            "Action": [ "glue:GetTable", "glue:GetTables" ],
                            "Resource": f"arn:aws:glue:us-east-1:{TestIAMPolicyPermissionsReader.CATALOG_ID}:table/test_database/test_table"
                        }
	                ]
            }
         ]}.items())

        policy_reader = IamPolicyPermissionsReader(self._appConfig, {})

        permissionsList : PermissionsList = policy_reader.read_policies()

        self.assertEqual(len(permissionsList.get_permissions()), 1)
        self.assertSetEqual(permissionsList.get_permissions()[0].permission_actions(), {"glue:GetTable", "glue:GetTables"})
        self.assertEqual(permissionsList.get_permissions()[0].principal_arn(), "arn:aws:iam::012345678901:role/role1")
        self.assertEqual(permissionsList.get_permissions()[0].resource_arn(), f"arn:aws:glue:us-east-1:{TestIAMPolicyPermissionsReader.CATALOG_ID}:table/test_database/test_table")

    def not_test_glue_multiple_statements_policy(self):
        self._iam_policy_reader.get_all_prinicial_policies.return_value = iter({ "arn:aws:iam::012345678901:role/role1" : [
            {
	                "Version": "2012-10-17",
	                "Statement": [
                        {
                            "Effect": "Allow",
                            "Action": [ "glue:GetTable", "glue:GetTables" ],
                            "Resource": f"arn:aws:glue:us-east-1:{TestIAMPolicyPermissionsReader.CATALOG_ID}:table/test_database/test_table"
                        }
	                ]
            },
            {
	                "Version": "2012-10-17",
	                "Statement": [
                        {
                            "Effect": "Allow",
                            "Action": [ "glue:GetTable", "glue:GetTables" ],
                            "Resource": f"arn:aws:glue:us-east-1:{TestIAMPolicyPermissionsReader.CATALOG_ID}:table/test_database/test_table2"
                        }
	                ]
            }

         ]}.items())

        policy_reader = IamPolicyPermissionsReader(self._appConfig, {})

        permissionsList : PermissionsList = policy_reader.read_policies()

        self.assertEqual(len(permissionsList.get_permissions()), 2)
        permissionsAsList = permissionsList.get_permissions()
        permissionsAsList.sort()
        self.assertSetEqual(permissionsAsList[0].permission_actions(), {"glue:GetTable", "glue:GetTables"})
        self.assertEqual(permissionsAsList[0].principal_arn(), "arn:aws:iam::012345678901:role/role1")
        self.assertEqual(permissionsAsList[0].resource_arn(), f"arn:aws:glue:us-east-1:{TestIAMPolicyPermissionsReader.CATALOG_ID}:table/test_database/test_table")

        self.assertSetEqual(permissionsAsList[1].permission_actions(), {"glue:GetTable", "glue:GetTables"})
        self.assertEqual(permissionsAsList[1].principal_arn(), "arn:aws:iam::012345678901:role/role1")
        self.assertEqual(permissionsAsList[1].resource_arn(), f"arn:aws:glue:us-east-1:{TestIAMPolicyPermissionsReader.CATALOG_ID}:table/test_database/test_table2")

    def test_glue_one_allow_one_deny_statement_policy(self):
        self._iam_policy_reader.get_all_prinicial_policies.return_value = iter({ "arn:aws:iam::012345678901:role/role1" : [
            {
	                "Version": "2012-10-17",
	                "Statement": [
                        {
                            "Effect": "Allow",
                            "Action": [ "glue:GetTable", "glue:GetTables" ],
                            "Resource": f"arn:aws:glue:us-east-1:{TestIAMPolicyPermissionsReader.CATALOG_ID}:table/test_database/test_table"
                        }
	                ]
            },
            {
	                "Version": "2012-10-17",
	                "Statement": [
                        {
                            "Effect": "Deny",
                            "Action": [ "glue:GetTable", "glue:GetTables" ],
                            "Resource": f"arn:aws:glue:us-east-1:{TestIAMPolicyPermissionsReader.CATALOG_ID}:table/test_database/test_table"
                        }
	                ]
            }

         ]}.items())

        policy_reader = IamPolicyPermissionsReader(self._appConfig, {})

        permissionsList : PermissionsList = policy_reader.read_policies()

        self.assertEqual(len(permissionsList.get_permissions()), 0)

    def test_glue_with_wildcard_statement_policy(self):
        self._iam_policy_reader.get_all_prinicial_policies.return_value = iter({ "arn:aws:iam::012345678901:role/role1" : [
            {
	                "Version": "2012-10-17",
	                "Statement": [
                        {
                            "Effect": "Allow",
                            "Action": [ "glue:GetTable", "glue:GetTables" ],
                            "Resource": f"arn:aws:glue:us-east-1:{TestIAMPolicyPermissionsReader.CATALOG_ID}:table/*/test_table"
                        }
	                ]
            },
            {
	                "Version": "2012-10-17",
	                "Statement": [
                        {
                            "Effect": "Deny",
                            "Action": [ "glue:GetTable", "glue:GetTables" ],
                            "Resource": f"arn:aws:glue:us-east-1:{TestIAMPolicyPermissionsReader.CATALOG_ID}:table/test_database/test_table"
                        }
	                ]
            }

         ]}.items())

        policy_reader = IamPolicyPermissionsReader(self._appConfig, {})

        permissionsList : PermissionsList = policy_reader.read_policies()

        self.assertEqual(len(permissionsList.get_permissions()), 1)

        permissionsAsList = permissionsList.get_permissions()
        permissionsAsList.sort()

        self.assertSetEqual(permissionsAsList[0].permission_actions(), {"glue:GetTable", "glue:GetTables"})
        self.assertEqual(permissionsAsList[0].principal_arn(), "arn:aws:iam::012345678901:role/role1")
        self.assertEqual(permissionsAsList[0].resource_arn(), f"arn:aws:glue:us-east-1:{TestIAMPolicyPermissionsReader.CATALOG_ID}:table/test_database2/test_table")

    def test_s3_multiple_statements_policy(self):
        self._iam_policy_reader.get_all_prinicial_policies.return_value = iter({ "arn:aws:iam::012345678901:role/role1" : [
            {
	                "Version": "2012-10-17",
	                "Statement": [
                        {
                            "Effect": "Allow",
                            "Action": [ "s3:GetObject" ],
                            "Resource": "arn:aws:s3:::mybucket/test_database/*"
                        }
	                ]
            },
            {
	                "Version": "2012-10-17",
	                "Statement": [
                        {
                            "Effect": "Allow",
                            "Action": "s3:GetObject" ,
                            "Resource": [ "arn:aws:s3:::mybucket/test_database2/test_table/*", "arn:aws:s3:::mybucket/test_database2/test_table2/*" ]
                        }
	                ]
            }

         ]}.items())

        tables : set[GlueTable] = set(TestIAMPolicyPermissionsReader.S3_TO_TABLE_MAPPER.get_all_tables_from_s3_arn_prefix("arn:aws:s3:::mybucket/test_database/"))
        tables.update(TestIAMPolicyPermissionsReader.S3_TO_TABLE_MAPPER.get_all_tables_from_s3_arn_prefix("arn:aws:s3:::mybucket/test_database2/test_table/"))
        tables.update(TestIAMPolicyPermissionsReader.S3_TO_TABLE_MAPPER.get_all_tables_from_s3_arn_prefix("arn:aws:s3:::mybucket/test_database2/test_table2/"))

        tablesList = list(tables)
        tablesList.sort()

        self.assertEqual(len(tables), 5)

        policy_reader = IamPolicyPermissionsReader(self._appConfig, {})

        permissionsList : PermissionsList = policy_reader.read_policies()

        for permission in permissionsList:
            print(permission)

        self.assertEqual(len(permissionsList.get_permissions()), 5)
        permissionsAsList = permissionsList.get_permissions()
        permissionsAsList.sort()

        self.assertSetEqual(permissionsAsList[0].permission_actions(), {"s3:GetObject"})
        self.assertEqual(permissionsAsList[0].principal_arn(), "arn:aws:iam::012345678901:role/role1")
        self.assertEqual(permissionsAsList[0].resource_arn(), "arn:aws:s3:::mybucket/test_database/test_table/*")

        self.assertSetEqual(permissionsAsList[1].permission_actions(), {"s3:GetObject"})
        self.assertEqual(permissionsAsList[1].principal_arn(), "arn:aws:iam::012345678901:role/role1")
        self.assertEqual(permissionsAsList[1].resource_arn(), "arn:aws:s3:::mybucket/test_database/test_table2/*")

        self.assertSetEqual(permissionsAsList[2].permission_actions(), {"s3:GetObject"})
        self.assertEqual(permissionsAsList[2].principal_arn(), "arn:aws:iam::012345678901:role/role1")
        self.assertEqual(permissionsAsList[2].resource_arn(), "arn:aws:s3:::mybucket/test_database/test_table3/*")

        self.assertSetEqual(permissionsAsList[3].permission_actions(), {"s3:GetObject"})
        self.assertEqual(permissionsAsList[3].principal_arn(), "arn:aws:iam::012345678901:role/role1")
        self.assertEqual(permissionsAsList[3].resource_arn(), "arn:aws:s3:::mybucket/test_database2/test_table/*")

        self.assertSetEqual(permissionsAsList[4].permission_actions(), {"s3:GetObject"})
        self.assertEqual(permissionsAsList[4].principal_arn(), "arn:aws:iam::012345678901:role/role1")
        self.assertEqual(permissionsAsList[4].resource_arn(), "arn:aws:s3:::mybucket/test_database2/test_table2/*")

    def test_s3_with_multiple_wildcards_statement_policy(self):
        self._iam_policy_reader.get_all_prinicial_policies.return_value = iter({ "arn:aws:iam::012345678901:role/role1" : [
            {
	                "Version": "2012-10-17",
	                "Statement": [
                        {
                            "Effect": "Allow",
                            "Action": [ "s3:Get*" ],
                            "Resource": f"arn:aws:s3:::mybucket/test_database2/*"
                        }
	                ]
            }
         ]}.items())

        tables : list = TestIAMPolicyPermissionsReader.S3_TO_TABLE_MAPPER.get_all_tables_from_s3_arn_prefix("arn:aws:s3:::mybucket/test_database2/")
        self.assertEqual(len(tables), 2)

        policy_reader = IamPolicyPermissionsReader(self._appConfig, {})

        permissionsList : PermissionsList = policy_reader.read_policies()

        permissionsAsList = permissionsList.get_permissions()
        permissionsAsList.sort()

        self.assertEqual(len(permissionsAsList), 2)

        self.assertSetEqual(permissionsAsList[0].permission_actions(), {"s3:GetObject"})
        self.assertEqual(permissionsAsList[0].principal_arn(), "arn:aws:iam::012345678901:role/role1")
        self.assertEqual(permissionsAsList[0].resource_arn(), "arn:aws:s3:::mybucket/test_database2/test_table/*")

        self.assertSetEqual(permissionsAsList[1].permission_actions(), {"s3:GetObject"})
        self.assertEqual(permissionsAsList[1].principal_arn(), "arn:aws:iam::012345678901:role/role1")
        self.assertEqual(permissionsAsList[1].resource_arn(), "arn:aws:s3:::mybucket/test_database2/test_table2/*")

if __name__ == '__main__':
    unittest.main()
