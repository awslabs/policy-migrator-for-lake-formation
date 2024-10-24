from aws_resources.actions.glue_action import GlueAction
from aws_resources.actions.s3_action import S3Action
from config.application_configuration import ApplicationConfiguration
from permissions.permissions_list import PermissionsList
from permissions.permission_record import PermissionRecord
from policy_filters.filter_invalid_actions_to_resources import FilterInvalidActionsToResources

from tests.unit.helpers.permissions_list_test_helpers import GlobalTestVariables

import unittest

# pylint: disable=all
class TestFilterInvalidActionsToResources(unittest.TestCase):
    def test_filter_invalid_actions_for_glue_resources(self):
        permissions_list : PermissionsList = PermissionsList()

        all_glue_permissions : list[str] = GlueAction.get_glue_actions_with_wildcard("*")
        all_s3_permissions : list[str] = S3Action.get_s3_actions_with_wildcard("*")
        additional_s3_permissions = all_s3_permissions.copy()
        additional_s3_permissions.append("s3:CreateAccessGrant")
        additional_s3_permissions.append("s3:CreateAccessPoint")
        additional_s3_permissions.append("s3:CreateBucket")

        permissions_list.add_permission_record(PermissionRecord("p1", f"arn:aws:glue:{GlobalTestVariables.test_region}:{GlobalTestVariables.test_catalog_id}:catalog", 
                                                                set(all_glue_permissions)))
        permissions_list.add_permission_record(PermissionRecord("p1", f"arn:aws:glue:{GlobalTestVariables.test_region}:{GlobalTestVariables.test_catalog_id}:database/abc", 
                                                                set(all_glue_permissions)))
        permissions_list.add_permission_record(PermissionRecord("p1", f"arn:aws:glue:{GlobalTestVariables.test_region}:{GlobalTestVariables.test_catalog_id}:table/abc/abc2", 
                                                                set(all_glue_permissions)))
        permissions_list.add_permission_record(PermissionRecord("p1", "arn:aws:s3:::mybucket/abc/abc2", 
                                                                set(additional_s3_permissions)))

        filter : FilterInvalidActionsToResources = FilterInvalidActionsToResources(ApplicationConfiguration(""), {})
        filtered_permissions_list : PermissionsList = filter.filter_policies(permissions_list)

        self.assertEqual(filtered_permissions_list.get_permissions_count(), 4)

        permissions_list.remove_permissions(filtered_permissions_list)

        filtered_permissions_sorted_list : list[PermissionRecord] = [permission for permission in permissions_list]
        filtered_permissions_sorted_list.sort()

        #catalog
        self.assertEqual(len(filtered_permissions_sorted_list[0].permission_actions()), len(GlueAction.get_catalog_level_actions()))
        self.assertSetEqual(filtered_permissions_sorted_list[0].permission_actions(), set(["glue:" + action.value for action in GlueAction.get_catalog_level_actions()]))

        #database
        self.assertEqual(len(filtered_permissions_sorted_list[1].permission_actions()), len(GlueAction.get_database_level_actions()))
        self.assertSetEqual(filtered_permissions_sorted_list[1].permission_actions(), set(["glue:" + action.value for action in GlueAction.get_database_level_actions()]))

        #table
        self.assertEqual(len(filtered_permissions_sorted_list[2].permission_actions()), len(GlueAction.get_table_level_actions()))
        self.assertSetEqual(filtered_permissions_sorted_list[2].permission_actions(), set(["glue:" + action.value for action in GlueAction.get_table_level_actions()]))

        #s3
        self.assertEqual(len(filtered_permissions_sorted_list[3].permission_actions()), len(all_s3_permissions))
        self.assertSetEqual(filtered_permissions_sorted_list[3].permission_actions(), set(all_s3_permissions))


if __name__ == '__main__':
    unittest.main()
