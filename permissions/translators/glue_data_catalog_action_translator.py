from aws_resources.actions.glue_action import GlueAction
from permissions.lakeformation_permissions.lakeformation_permissions import LakeFormationPermissions

class GlueDataCatalogActionTranslator:
    '''
        Translates Glue actions to LakeFormation permissions
    '''

    _glue_action_to_lf_permission_type_map = {

        # Catalog Permissions
        GlueAction.GetDatabases: LakeFormationPermissions.LIST_DBS,
        GlueAction.CreateDatabase: LakeFormationPermissions.CREATE_DATABASE,

        # database permissions
        GlueAction.GetDatabase: LakeFormationPermissions.DESCRIBE,
        GlueAction.DeleteDatabase: LakeFormationPermissions.DROP,
        GlueAction.UpdateDatabase: LakeFormationPermissions.ALTER,
        GlueAction.CreateTable: LakeFormationPermissions.CREATE_TABLE,

        # Table permissions
        GlueAction.UpdateTable: LakeFormationPermissions.ALTER,
        GlueAction.DeleteTable: LakeFormationPermissions.DROP,

        GlueAction.GetTables: LakeFormationPermissions.DESCRIBE,
        GlueAction.GetTable: LakeFormationPermissions.DESCRIBE,
        GlueAction.DeleteTableVersion: LakeFormationPermissions.ALTER,
        GlueAction.BatchDeleteTableVersion: LakeFormationPermissions.ALTER,
        GlueAction.CreatePartition: LakeFormationPermissions.ALTER,
        GlueAction.UpdatePartition: LakeFormationPermissions.ALTER,
        GlueAction.DeletePartition: LakeFormationPermissions.ALTER,
        GlueAction.GetPartition: LakeFormationPermissions.DESCRIBE,
        GlueAction.GetPartitions: LakeFormationPermissions.DESCRIBE,
        GlueAction.BatchGetPartition: LakeFormationPermissions.DESCRIBE,
        GlueAction.BatchCreatePartition: LakeFormationPermissions.ALTER,
        GlueAction.BatchUpdatePartition: LakeFormationPermissions.ALTER,
        GlueAction.BatchDeletePartition: LakeFormationPermissions.ALTER,
        GlueAction.GetPartitionIndexes: LakeFormationPermissions.DESCRIBE,
    }

    @staticmethod
    def translate_glue_action_to_lf_permission_type(glueAction: GlueAction) -> LakeFormationPermissions:
        return GlueDataCatalogActionTranslator._glue_action_to_lf_permission_type_map.get(glueAction, None)
