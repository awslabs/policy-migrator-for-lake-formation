from permissions.permissions_list import PermissionsList
from config.application_configuration import ApplicationConfiguration

class PolicyReaderInterface:
    """An interface to read policies from some source."""

    def __init__(self, applicationConfig : ApplicationConfiguration, config : dict[str]):
        self._appConfig = applicationConfig
        self._config = config

    def read_policies(self) -> PermissionsList:
        '''
        Reads policies and returns a PermissionsList. All Actions need to be IAM actions, ie "<service>:<action>", and 
        all resources need to be ARN's. 
        '''

    @classmethod
    def get_name(cls) -> str:
        '''
        Returns the name of the Policy Reader.
        '''

    @classmethod
    def get_required_configuration(cls) -> dict:
        '''
        Returns a dictionary of required configuration keys and their descriptions. This is used to validate the configuration file.
        '''

    @classmethod
    def get_config_section(cls) -> str:
        '''
        Returns the section name in the configuration file where the configuration for this reader is stored.
        '''
