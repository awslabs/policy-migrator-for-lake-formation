
from permissions.permissions_list import PermissionsList
from config.application_configuration import ApplicationConfiguration

class PostProcessingPluginInterface:
    '''
    Post Processing Plugins take Lake Formation permissions and apply post processing them, to do things
    like add or remove permissions. 
    '''

    def __init__(self, appConfig : ApplicationConfiguration, conf : dict[str]):
        self._appConfig = appConfig
        self._config = conf

    def process(self, lf_permissions: PermissionsList) -> PermissionsList:
        pass

    @classmethod
    def get_name(cls) -> str:
        pass

    @classmethod
    def get_required_configuration(cls) -> dict:
        pass

    @classmethod
    def get_config_section(cls) -> str:
        pass
