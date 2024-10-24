from enum import StrEnum, verify, UNIQUE

#Permission Types
@verify(UNIQUE)
class LakeFormationPermissionResourceType(StrEnum):
    '''
        Enum to represent LF Resource Types
    '''
    CATALOG_PERMISSION = "CATALOG_PERMISSION"
    DATABASE_PERMISSION = "DATABASE_PERMISSION"
    TABLE_PERMISSION = "TABLE_PERMISSION"
    DATA_LOCATION_PERMISSION = "DATA_LOCATION_PERMISSION"
    TAG_PERMISSION = "TAG_PERMISSION"
