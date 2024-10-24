

class ConfigurationInvalidException(Exception):
    '''
    Exception raised when the configuration is invalid.
    '''
    def __init__(self, message):
        super().__init__(message)
