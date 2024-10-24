
import boto3

from config.config_helper import ConfigHelper

class Boto3Factory:
    '''
    Factory class that creates Boto3 Sessions from Application Configuration
    '''

    @staticmethod
    def createBoto3SessionFromConfiguration(app_args : dict[str]) -> boto3.Session:
        boto3configuration = ConfigHelper.get_section(app_args, "boto3", None)
        if boto3configuration is None:
            return boto3.Session()

        return boto3.Session(aws_access_key_id=ConfigHelper.get_config_string(boto3configuration, "aws_access_key_id"),
                                      aws_secret_access_key=ConfigHelper.get_config_string(boto3configuration, "aws_secret_access_key"),
                                      aws_session_token=ConfigHelper.get_config_string(boto3configuration, "aws_session_token"),
                                      region_name=ConfigHelper.get_config_string(boto3configuration, "region_name"),
                                      profile_name=ConfigHelper.get_config_string(boto3configuration, "profile_name"))
