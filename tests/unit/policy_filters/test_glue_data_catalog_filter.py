from aws_resources.aws_arn_utils import AwsArnUtils
from aws_resources.glue_data_catalog import GlueDataCatalog
from aws_resources.glue_data_catalog import GlueCatalog
from aws_resources.glue_data_catalog import GlueDatabase
from aws_resources.glue_data_catalog import GlueTable
from config.application_configuration import ApplicationConfiguration
from policy_filters.glue_data_catalog_filter import FilterNotInGlueCatalog
from permissions.permissions_list import PermissionsList
from lakeformation_utils.s3_to_table_mapper import S3ToTableMapper
from tests.unit.helpers.permissions_list_test_helpers import GlobalTestVariables, PermissionsListTestHelper

import unittest

# pylint: disable=all
class TestFilterNotInGlueCatalog(unittest.TestCase):

    def setUp(self):
        self._test_region = GlobalTestVariables.test_region
        self._test_catalog_id = GlobalTestVariables.test_catalog_id

        self._gdcCatalog = GlueDataCatalog()
        self._gdcCatalog.add_catalog(GlueCatalog(self._test_region, self._test_catalog_id))
        self._gdcCatalog.add_database(GlueDatabase(self._test_region, self._test_catalog_id, "test_database"))
        self._gdcCatalog.add_table(GlueTable(self._test_region, self._test_catalog_id, "test_database", "test_table", "s3://mybucket/test_table/"))
        self._gdcCatalog.add_table(GlueTable(self._test_region, self._test_catalog_id, "test_database", "test_table2", "s3://mybucket/test_table2/"))
        self._gdcCatalog.add_table(GlueTable(self._test_region, self._test_catalog_id, "test_database", "test_table3", "s3://mybucket/test_table3/"))
        self._gdcCatalog.add_database(GlueDatabase(self._test_region, self._test_catalog_id, "test_database2"))
        self._gdcCatalog.add_table(GlueTable(self._test_region, self._test_catalog_id, "test_database2", "test_table", "s3://mybucket2/test_table/"))
        self._gdcCatalog.add_table(GlueTable(self._test_region, self._test_catalog_id, "test_database2", "test_table2", "s3://mybucket2/test_table2/"))
        self._gdcCatalog.add_table(GlueTable(self._test_region, self._test_catalog_id, "test_database2", "test_table3", "s3://mybucket2/test_table3/"))
        
        self._s3_to_table_mapper : S3ToTableMapper = S3ToTableMapper(self._gdcCatalog)

    def test_filter_not_in_glue_catalog(self):
        permissionsList = PermissionsList()
        permissionsList.add_permission_record(PermissionsListTestHelper.create_permissions_record_catalog(self._test_catalog_id, {"glue:GetDatabases", "glue:DropDatabase"}))
        permissionsList.add_permission_record(PermissionsListTestHelper.create_permissions_record_database(self._test_catalog_id, "test_database", {"glue:GetDatabase", "glue:UpdateDatabase"}))
        permissionsList.add_permission_record(PermissionsListTestHelper.create_permissions_record_table(self._test_catalog_id, "test_database", "test_table", {"glue:UpdateTable"}))
        permissionsList.add_permission_record(PermissionsListTestHelper.create_permissions_record_table(self._test_catalog_id, "test_database", "test_table2", {"glue:UpdateTable"}))
        permissionsList.add_permission_record(PermissionsListTestHelper.create_permissions_record_table(self._test_catalog_id, "test_database", "test_table3", {"glue:UpdateTable"}))

        #Non-existant table
        permissionsList.add_permission_record(PermissionsListTestHelper.create_permissions_record_table(self._test_catalog_id, "test_database", "test_table4", {"glue:UpdateTable"}))

        #non-existant database (database3)
        permissionsList.add_permission_record(PermissionsListTestHelper.create_permissions_record_database(self._test_catalog_id, "test_database3", {"glue:GetDatabase"}))

        #non-existant table in non-existant database
        permissionsList.add_permission_record(PermissionsListTestHelper.create_permissions_record_table(self._test_catalog_id, "test_database3", "test_table5", {"glue:GetTable"}))
        permissionsList.add_permission_record(PermissionsListTestHelper.create_permissions_record_table(self._test_catalog_id, "test_database3", "test_table6", {"glue:GetTable"}))
        permissionsList.add_permission_record(PermissionsListTestHelper.create_permissions_record_table(self._test_catalog_id, "test_database3", "test_table7", {"glue:GetTable"}))

        appConfig = ApplicationConfiguration(args = {}, glue_data_catalog = self._gdcCatalog, 
                                                        s3_to_table_translator = self._s3_to_table_mapper)

        filterNotInGlueCatalog = FilterNotInGlueCatalog(appConfig, conf = {})

        filteredPermissions = filterNotInGlueCatalog.filter_policies(permissionsList)

        filteredPermissionsAsList = list(iter(filteredPermissions))

        self.assertEqual(len(filteredPermissionsAsList), 5)

    def test_filtering_s3_resources(self):
        permissionsList = PermissionsList()

        # S3 Paths that point to a GDC table
        permissionsList.add_permission_record(PermissionsListTestHelper.create_permissions_record_s3(self._test_catalog_id, 
                                            AwsArnUtils.get_s3_arn_from_s3_path("s3://mybucket/test_table/"),
                                            {"s3:GetObject", "s3:PutObject"}))
        permissionsList.add_permission_record(PermissionsListTestHelper.create_permissions_record_s3(self._test_catalog_id, 
                                            AwsArnUtils.get_s3_arn_from_s3_path("s3://mybucket/test_table2/"),
                                            {"s3:GetObject", "s3:PutObject"}))
        permissionsList.add_permission_record(PermissionsListTestHelper.create_permissions_record_s3(self._test_catalog_id, 
                                            AwsArnUtils.get_s3_arn_from_s3_path("s3://mybucket/test_table3/"),
                                            {"s3:GetObject", "s3:PutObject"}))

        # S3 Paths that dont point to a GDC table
        permissionsList.add_permission_record(PermissionsListTestHelper.create_permissions_record_s3(self._test_catalog_id, 
                                            AwsArnUtils.get_s3_arn_from_s3_path("s3://mybucket/test_table4/"),
                                            {"s3:GetObject", "s3:PutObject"}))
        permissionsList.add_permission_record(PermissionsListTestHelper.create_permissions_record_s3(self._test_catalog_id, 
                                            AwsArnUtils.get_s3_arn_from_s3_path("s3://mybucket/test_table5/"),
                                            {"s3:GetObject", "s3:PutObject"}))

        appConfig = ApplicationConfiguration(args = {}, glue_data_catalog = self._gdcCatalog, 
                                                        s3_to_table_translator = self._s3_to_table_mapper)

        filterNotInGlueCatalog = FilterNotInGlueCatalog(appConfig, conf = {})

        filteredPermissions = filterNotInGlueCatalog.filter_policies(permissionsList)

        filteredPermissionsAsList = list(iter(filteredPermissions))

        self.assertEqual(len(filteredPermissionsAsList), 2)

if __name__ == '__main__':
    unittest.main()
