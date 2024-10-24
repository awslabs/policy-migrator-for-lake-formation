from enum import StrEnum, verify, UNIQUE

#Individual Permission Verbs
@verify(UNIQUE)
class LakeFormationPermissions(StrEnum):
    '''
        Enum to represent LakeFormation Permissions
    '''
    SELECT = "SELECT"
    ALTER = "ALTER"
    DROP = "DROP"
    DELETE = "DELETE"
    INSERT = "INSERT"
    DESCRIBE = "DESCRIBE"
    SUPER = "SUPER"
    CREATE_DATABASE = "CREATE_DATABASE"
    CREATE_TABLE = "CREATE_TABLE"
    DATA_LOCATION_ACCESS = "DATA_LOCATION_ACCESS"
    CREATE_LF_TAG = "CREATE_LF_TAG"
    ASSOCIATE = "ASSOCIATE"
    GRANT_WITH_LF_TAG_EXPRESSION = "GRANT_WITH_LF_TAG_EXPRESSION"
    LIST_DBS = "LIST_DBS"
    LIST_DB = "LIST_DB"
