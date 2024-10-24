from enum import StrEnum, verify, UNIQUE
import re

@verify(UNIQUE)
class S3Action(StrEnum):
    '''
        Enum for S3 Actions
    '''
    GetObject = "GetObject"
    HeadObject = "HeadObject"
    PutObject = "PutObject"
    CreateMultipartUpload = "CreateMultipartUpload"
    UploadPart = "UploadPart"
    DeleteObject = "DeleteObject"

    @staticmethod
    def translate_s3_action_to_enum(s3_action_str):
        if not s3_action_str.startswith("s3:"):
            raise ValueError(f"S3 Action String doesn't start with s3: {s3_action_str}")

        s3_action_str = s3_action_str[3:]

        if s3_action_str not in S3Action:
            return None

        return S3Action[s3_action_str]

    @staticmethod
    def get_s3_actions_with_wildcard(action_with_wildcard : str) -> list[str]:
        actions : list[str] = []
        for action in S3Action:
            if re.fullmatch(action_with_wildcard.replace("*", ".*"), action):
                actions.append("s3:" + action)
        return actions

    @staticmethod
    def get_s3_table_level_actions() -> set[str]:
        return { S3Action.GetObject, S3Action.HeadObject, S3Action.PutObject, S3Action.CreateMultipartUpload, S3Action.UploadPart, S3Action.DeleteObject}

    @staticmethod
    def get_filtered_out_s3_table_level_actions(actions : list[str]) -> list[str]:
        return [action for action in actions if S3Action.translate_s3_action_to_enum(action) not in S3Action.get_s3_table_level_actions()]
