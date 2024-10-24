from aws_resources.glue_catalog import GlueCatalog
from aws_resources.glue_database import GlueDatabase
from aws_resources.glue_table import GlueTable
from permissions.permissions_list import PermissionsList
from permissions.permissions_list import PermissionRecord
from aws_resources.glue_data_catalog import GlueDataCatalog

from tests.unit.helpers.global_test_variables import GlobalTestVariables

# pylint: disable=all

class PermissionsListTestHelper:

    test_catalog_id = GlobalTestVariables.test_catalog_id
    test_region = GlobalTestVariables.test_region

    @staticmethod
    def create_permissions_record_catalog(rolename, permission_actions : set) -> PermissionRecord:
        return PermissionRecord(f"arn:aws:iam::{PermissionsListTestHelper.test_catalog_id}:role/{rolename}",
                                f"arn:aws:glue:{PermissionsListTestHelper.test_region}:{PermissionsListTestHelper.test_catalog_id}:catalog",
                                permission_actions)
    @staticmethod
    def create_permissions_record_database(rolename, databasename, permission_actons : set) -> PermissionRecord:
        return PermissionRecord(f"arn:aws:iam::{PermissionsListTestHelper.test_catalog_id}:role/{rolename}",
                                f"arn:aws:glue:{PermissionsListTestHelper.test_region}:{PermissionsListTestHelper.test_catalog_id}:database/{databasename}",
                                permission_actons)

    @staticmethod
    def create_permissions_record_table(rolename, databasename, tablename, permission_actions : set) -> PermissionRecord:
        return PermissionRecord(f"arn:aws:iam::{PermissionsListTestHelper.test_catalog_id}:role/{rolename}",
                                f"arn:aws:glue:{PermissionsListTestHelper.test_region}:{PermissionsListTestHelper.test_catalog_id}:table/{databasename}/{tablename}",
                                permission_actions)

    @staticmethod
    def create_permissions_record_s3(rolename, s3_arn, permission_actions : set) -> PermissionRecord:
        return PermissionRecord(f"arn:aws:iam::{PermissionsListTestHelper.test_catalog_id}:role/{rolename}",
                                s3_arn,
                                permission_actions)

    @staticmethod
    def create_permissions_list_with_glue_permissions_one_database_three_tables() -> PermissionsList:
        test_catalog_id = PermissionsListTestHelper.test_catalog_id
        permissionsList = PermissionsList()

        permissionsList.add_permission(f"arn:aws:iam:{test_catalog_id}:role/role1", f"arn:aws:glue:{PermissionsListTestHelper.test_region}:{test_catalog_id}:catalog", "glue:GetTable")
        permissionsList.add_permission(f"arn:aws:iam:{test_catalog_id}:role/role1", f"arn:aws:glue:{PermissionsListTestHelper.test_region}:{test_catalog_id}:database/test_database", "glue:GetTable")
        permissionsList.add_permission(f"arn:aws:iam:{test_catalog_id}:role/role1", f"arn:aws:glue:{PermissionsListTestHelper.test_region}:{test_catalog_id}:database/test_database", "glue:AlterTable")
        permissionsList.add_permission(f"arn:aws:iam:{test_catalog_id}:role/role1", f"arn:aws:glue:{PermissionsListTestHelper.test_region}:{test_catalog_id}:table/test_database/test_table", "glue:GetTable")
        permissionsList.add_permission(f"arn:aws:iam:{test_catalog_id}:role/role1", f"arn:aws:glue:{PermissionsListTestHelper.test_region}:{test_catalog_id}:table/test_database/test_table", "glue:AlterTable")
        permissionsList.add_permission(f"arn:aws:iam:{test_catalog_id}:role/role1", f"arn:aws:glue:{PermissionsListTestHelper.test_region}:{test_catalog_id}:table/test_database/test_table", "glue:GetDatabase")
        permissionsList.add_permission(f"arn:aws:iam:{test_catalog_id}:role/role1", f"arn:aws:glue:{PermissionsListTestHelper.test_region}:{test_catalog_id}:table/test_database/test_table2", "glue:UpdateDatabase")
        permissionsList.add_permission(f"arn:aws:iam:{test_catalog_id}:role/role1", f"arn:aws:glue:{PermissionsListTestHelper.test_region}:{test_catalog_id}:table/test_database/test_table2", "glue:DropDatabase")
        permissionsList.add_permission(f"arn:aws:iam:{test_catalog_id}:role/role2", f"arn:aws:glue:{PermissionsListTestHelper.test_region}:{test_catalog_id}:table/test_database/test_table2", "glue:DropDatabase")
        permissionsList.add_permission(f"arn:aws:iam:{test_catalog_id}:role/role2", f"arn:aws:glue:{PermissionsListTestHelper.test_region}:{test_catalog_id}:table/test_database/test_table2", "glue:GetCatalog")
        permissionsList.add_permission(f"arn:aws:iam:{test_catalog_id}:role/role2", f"arn:aws:glue:{PermissionsListTestHelper.test_region}:{test_catalog_id}:table/test_database/test_table2", "glue:DropDatabase")
        permissionsList.add_permission(f"arn:aws:iam:{test_catalog_id}:role/role2", f"arn:aws:glue:{PermissionsListTestHelper.test_region}:{test_catalog_id}:table/test_database/test_table2", "glue:GetCatalog")

        return permissionsList

    @staticmethod
    def create_permissions_list_with_s3_permissions_one_bucket_three_objects() -> PermissionsList:
        test_catalog_id = PermissionsListTestHelper.test_catalog_id
        permissionsList = PermissionsList()

        permissionsList.add_permission(f"arn:aws:iam:{test_catalog_id}:role/role1", f"arn:aws:s3:::mybucket", "s3:HeadBucket")
        permissionsList.add_permission(f"arn:aws:iam:{test_catalog_id}:role/role1", f"arn:aws:s3:::mybucket", "s3:ListBucket")

        #DB1 Table 1
        permissionsList.add_permission(f"arn:aws:iam:{test_catalog_id}:role/role1", f"arn:aws:s3:::mybucket/test_database/test_table/object1.parq", "s3:GetObject")
        permissionsList.add_permission(f"arn:aws:iam:{test_catalog_id}:role/role1", f"arn:aws:s3:::mybucket/test_database/test_table/object2.parq", "s3:GetObject")
        permissionsList.add_permission(f"arn:aws:iam:{test_catalog_id}:role/role1", f"arn:aws:s3:::mybucket/test_database/test_table/object3.parq", "s3:PutObject")

        #DB1 Table 2
        permissionsList.add_permission(f"arn:aws:iam:{test_catalog_id}:role/role1", f"arn:aws:s3:::mybucket/test_database/test_table2/object1.parq", "s3:GetObject")
        permissionsList.add_permission(f"arn:aws:iam:{test_catalog_id}:role/role1", f"arn:aws:s3:::mybucket/test_database/test_table2/object2.parq", "s3:GetObject")
        permissionsList.add_permission(f"arn:aws:iam:{test_catalog_id}:role/role1", f"arn:aws:s3:::mybucket/test_database/test_table2/object3.parq", "s3:PutObject")

        #DB2 Table 1
        permissionsList.add_permission(f"arn:aws:iam:{test_catalog_id}:role/role1", f"arn:aws:s3:::mybucket/test_database2/test_table/object1.parq", "s3:GetObject")
        permissionsList.add_permission(f"arn:aws:iam:{test_catalog_id}:role/role1", f"arn:aws:s3:::mybucket/test_database2/test_table/object2.parq", "s3:GetObject")

        return permissionsList

    @staticmethod
    def create_glue_data_catalog() -> GlueDataCatalog:
        test_catalog_id = PermissionsListTestHelper.test_catalog_id
        test_region = PermissionsListTestHelper.test_region

        gdcCatalog = GlueDataCatalog()
        gdcCatalog.add_catalog(GlueCatalog(test_region, test_catalog_id))
        
        #Database 1 (test_database)
        gdcCatalog.add_database(GlueDatabase(test_region, test_catalog_id, "test_database", "s3://mybucket/test_database/"))
        gdcCatalog.add_table(GlueTable(test_region, test_catalog_id, "test_database", "test_table", "s3://mybucket/test_database/test_table/"))
        gdcCatalog.add_table(GlueTable(test_region, test_catalog_id, "test_database", "test_table2", "s3://mybucket/test_database/test_table2/"))
        gdcCatalog.add_table(GlueTable(test_region, test_catalog_id, "test_database", "test_table3", "s3://mybucket/test_database/test_table3/"))

        #Database 2 (test_database2)
        gdcCatalog.add_database(GlueDatabase(test_region, test_catalog_id, "test_database2", "s3://mybucket/test_database2_location/" ))
        gdcCatalog.add_table(GlueTable(test_region, test_catalog_id, "test_database2", "test_table", "s3://mybucket/test_database2/test_table"))
        gdcCatalog.add_table(GlueTable(test_region, test_catalog_id, "test_database2", "test_table2", "s3://mybucket/test_database2/test_table2"))
        gdcCatalog.add_table(GlueTable(test_region, test_catalog_id, "test_database2", "test_table3", "s3://mybucket/test_database2_abc/test_table3"))

        # Different database location in Database 2
        gdcCatalog.add_table(GlueTable(test_region, test_catalog_id, "test_database2", "test_table4", "s3://mybucket_2/test_database2_abc/test_table4"))
        gdcCatalog.add_table(GlueTable(test_region, test_catalog_id, "test_database2", "test_table5", "s3://mybucket_2/test_database2_abc/test_table5"))
        gdcCatalog.add_table(GlueTable(test_region, test_catalog_id, "test_database2", "test_table6", "s3://mybucket_2/test_database2_abc/test_table6"))

        gdcCatalog.add_table(GlueTable(test_region, test_catalog_id, "test_database2", "test_table7", "s3://mybucket_2/test_database3_def/test_table7"))
        gdcCatalog.add_table(GlueTable(test_region, test_catalog_id, "test_database2", "test_table8", "s3://mybucket_2/test_database3_def/test_table8"))
        gdcCatalog.add_table(GlueTable(test_region, test_catalog_id, "test_database2", "test_table9", "s3://mybucket_2/test_database3_def/test_table9"))

        # Different Bucket location in Database 2
        gdcCatalog.add_table(GlueTable(test_region, test_catalog_id, "test_database2", "test_table10", "s3://mybucket_3/test_database4/test_table10"))
        gdcCatalog.add_table(GlueTable(test_region, test_catalog_id, "test_database2", "test_table11", "s3://mybucket_3/test_database4/test_table11"))
        gdcCatalog.add_table(GlueTable(test_region, test_catalog_id, "test_database2", "test_table12", "s3://mybucket_3/test_database4/test_table12"))

        return gdcCatalog
