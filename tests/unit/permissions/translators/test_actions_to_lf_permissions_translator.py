import unittest

from lakeformation_utils.s3_to_table_mapper import S3ToTableMapper
from permissions.permissions_list import PermissionsList
from permissions.translators.actions_to_lakeformation_permissions_translator import ActionsToLFPermissionsTranslator

from permissions.lakeformation_permissions.lakeformation_permissions import LakeFormationPermissions

from tests.unit.helpers.permissions_list_test_helpers import PermissionsListTestHelper

# pylint: disable=all

class TestActionsToLFPermissionsTranslator(unittest.TestCase):
    def test_translate_actions_to_lf_permissions(self):

        gluePermissionsList = PermissionsListTestHelper.create_permissions_list_with_glue_permissions_one_database_three_tables()
        s3PermissionsList = PermissionsListTestHelper.create_permissions_list_with_s3_permissions_one_bucket_three_objects()
        gdcCatalog = PermissionsListTestHelper.create_glue_data_catalog()
        s3ToTableMapper = S3ToTableMapper(gdcCatalog)
        
        allPermissions = PermissionsList()
        allPermissions.add_permissions_from_list(gluePermissionsList)
        allPermissions.add_permissions_from_list(s3PermissionsList)

        translator = ActionsToLFPermissionsTranslator(s3ToTableMapper)

        lfpermissions = translator.translate_iam_permissions_to_lf_permissions(allPermissions)

        lfpermissionsAsList = list(lfpermissions.get_permissions())
        lfpermissionsAsList.sort()

        self.assertEqual(len(lfpermissionsAsList), 5)

        self.assertEqual(lfpermissionsAsList[0].principal_arn(), "arn:aws:iam:123456789012:role/role1")
        self.assertEqual(lfpermissionsAsList[0].resource_arn(), "arn:aws:glue:us-east-1:123456789012:catalog")
        self.assertSetEqual(lfpermissionsAsList[0].permission_actions(), {LakeFormationPermissions.DESCRIBE})
        
        self.assertEqual(lfpermissionsAsList[1].principal_arn(), "arn:aws:iam:123456789012:role/role1")
        self.assertEqual(lfpermissionsAsList[1].resource_arn(), "arn:aws:glue:us-east-1:123456789012:database/test_database")
        self.assertSetEqual(lfpermissionsAsList[1].permission_actions(), {LakeFormationPermissions.DESCRIBE})

        self.assertEqual(lfpermissionsAsList[2].principal_arn(), "arn:aws:iam:123456789012:role/role1")
        self.assertEqual(lfpermissionsAsList[2].resource_arn(), "arn:aws:glue:us-east-1:123456789012:table/test_database/test_table")
        self.assertSetEqual(lfpermissionsAsList[2].permission_actions(), {LakeFormationPermissions.DESCRIBE, 
                                                                          LakeFormationPermissions.INSERT, LakeFormationPermissions.SELECT})

        self.assertEqual(lfpermissionsAsList[3].principal_arn(), "arn:aws:iam:123456789012:role/role1")
        self.assertEqual(lfpermissionsAsList[3].resource_arn(), "arn:aws:glue:us-east-1:123456789012:table/test_database/test_table2")
        self.assertSetEqual(lfpermissionsAsList[3].permission_actions(), {LakeFormationPermissions.ALTER, LakeFormationPermissions.INSERT, LakeFormationPermissions.SELECT})

        self.assertEqual(lfpermissionsAsList[4].principal_arn(), "arn:aws:iam:123456789012:role/role1")
        self.assertEqual(lfpermissionsAsList[4].resource_arn(), "arn:aws:glue:us-east-1:123456789012:table/test_database2/test_table")
        self.assertSetEqual(lfpermissionsAsList[4].permission_actions(), {LakeFormationPermissions.SELECT})

if __name__ == '__main__':
    unittest.main()
