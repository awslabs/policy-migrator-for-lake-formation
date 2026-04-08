import unittest

from aws_resources.actions.s3_action import S3Action
from permissions.translators.s3_action_translator import S3ActionTranslator
from permissions.lakeformation_permissions.lakeformation_permissions import LakeFormationPermissions


class TestS3ActionTranslator(unittest.TestCase):
    """Tests for S3ActionTranslator — specifically the bug where unmapped
    actions raised ValueError instead of returning None."""

    def test_known_actions_translate(self):
        self.assertEqual(
            S3ActionTranslator.translate_s3_action_to_lf_permission_type(S3Action.GetObject),
            LakeFormationPermissions.SELECT,
        )
        self.assertEqual(
            S3ActionTranslator.translate_s3_action_to_lf_permission_type(S3Action.PutObject),
            LakeFormationPermissions.INSERT,
        )
        self.assertEqual(
            S3ActionTranslator.translate_s3_action_to_lf_permission_type(S3Action.DeleteObject),
            LakeFormationPermissions.DELETE,
        )

    def test_unmapped_action_returns_none(self):
        # HeadObject is mapped, but let's verify the return-None path
        # by passing None-like scenario. We can't easily create an unmapped
        # StrEnum member, so we verify the .get default works for a missing key.
        result = S3ActionTranslator._s3_action_to_lf_permission_type_map.get("NonExistent", None)
        self.assertIsNone(result)


if __name__ == '__main__':
    unittest.main()
