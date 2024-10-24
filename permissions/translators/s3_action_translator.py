from aws_resources.actions.s3_action import S3Action
from permissions.lakeformation_permissions.lakeformation_permissions import LakeFormationPermissions

class S3ActionTranslator:
    """
    Class to translate S3 actions to LakeFormation permissions
    """

    _s3_action_to_lf_permission_type_map = {
        S3Action.GetObject: LakeFormationPermissions.SELECT,
        S3Action.HeadObject: LakeFormationPermissions.SELECT,
        S3Action.PutObject: LakeFormationPermissions.INSERT,
        S3Action.CreateMultipartUpload: LakeFormationPermissions.INSERT,
        S3Action.UploadPart: LakeFormationPermissions.INSERT,
        S3Action.DeleteObject: LakeFormationPermissions.DELETE
    }

    @staticmethod
    def translate_s3_action_to_lf_permission_type(s3Action: S3Action) -> LakeFormationPermissions:
        permission = S3ActionTranslator._s3_action_to_lf_permission_type_map.get(s3Action, None)
        if permission is None:
            raise ValueError(f"No permission found for {s3Action}")
        return permission
