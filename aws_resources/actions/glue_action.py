from enum import StrEnum, verify, UNIQUE
import re
@verify(UNIQUE)
class GlueAction(StrEnum):
    '''
        Enum to represent Glue actions
    '''

    GetDatabase = "GetDatabase"
    CreateDatabase = "CreateDatabase"
    UpdateDatabase = "UpdateDatabase"
    DeleteDatabase = "DeleteDatabase"
    GetDatabases = "GetDatabases"

    GetTables = "GetTables"
    GetTable = "GetTable"
    CreateTable = "CreateTable"
    UpdateTable = "UpdateTable"
    DeleteTable = "DeleteTable"
    GetTableVersion = "GetTableVersion"
    GetTableVersions = "GetTableVersions"
    DeleteTableVersion = "DeleteTableVersion"
    BatchDeleteTableVersion = "BatchDeleteTableVersion"
    BatchCreatePartition = "BatchCreatePartition"
    GetPartition = "GetPartition"
    GetPartitions = "GetPartitions"
    BatchGetPartition = "BatchGetPartition"
    CreatePartition = "CreatePartition"
    DeletePartition = "DeletePartition"
    BatchDeletePartition = "BatchDeletePartition"
    UpdatePartition = "UpdatePartition"
    BatchUpdatePartition = "BatchUpdatePartition"
    GetPartitionIndexes = "GetPartitionIndexes"
    CreatePartitionIndex = "CreatePartitionIndex"
    DeletePartitionIndex = "DeletePartitionIndex"

    @staticmethod
    def translate_glue_action_to_enum(glue_action_str):
        if not glue_action_str.startswith("glue:"):
            raise ValueError(f"Glue Action String doesn't start with glue: {glue_action_str}")

        glue_action_str = glue_action_str[5:]

        if glue_action_str not in GlueAction:
            return None

        return GlueAction[glue_action_str]

    @staticmethod
    def get_glue_actions_with_wildcard(action_with_wildcard : str) -> list[str]:
        actions : list[str] = []
        for action in GlueAction:
            if re.fullmatch(action_with_wildcard.replace("*", ".*"), action):
                actions.append("glue:" + action.value)
        return actions

    @staticmethod
    def get_table_level_actions() -> set:
        return {GlueAction.GetTable, GlueAction.UpdateTable, GlueAction.DeleteTable,
                                 GlueAction.GetTableVersion, GlueAction.GetTableVersions, GlueAction.DeleteTableVersion, GlueAction.BatchDeleteTableVersion, 
                                 GlueAction.BatchCreatePartition, GlueAction.CreatePartition, GlueAction.GetPartition, GlueAction.GetPartitions, 
                                 GlueAction.BatchGetPartition, GlueAction.CreatePartition,
                                 GlueAction.DeletePartition, GlueAction.BatchDeletePartition, GlueAction.UpdatePartition, GlueAction.BatchUpdatePartition, 
                                 GlueAction.GetPartitionIndexes, GlueAction.CreatePartitionIndex, GlueAction.DeletePartitionIndex}

    @staticmethod
    def get_database_level_actions() -> set:
        return {GlueAction.GetDatabase, GlueAction.UpdateDatabase, GlueAction.DeleteDatabase, GlueAction.GetTables}

    @staticmethod
    def get_catalog_level_actions() -> set:
        return {GlueAction.CreateDatabase, GlueAction.GetDatabases}

    @staticmethod
    def get_filtered_out_table_level_actions(actions : list[str]) -> list[str]:
        return [action for action in actions if GlueAction.translate_glue_action_to_enum(action) not in GlueAction.get_table_level_actions()]

    @staticmethod
    def get_filtered_out_database_level_actions(actions : list[str]) -> list[str]:
        return [action for action in actions if GlueAction.translate_glue_action_to_enum(action) not in GlueAction.get_database_level_actions()]

    @staticmethod
    def get_filtered_out_catalog_level_actions(actions : list[str]) -> list[str]:
        return [action for action in actions if GlueAction.translate_glue_action_to_enum(action) not in GlueAction.get_catalog_level_actions()]
