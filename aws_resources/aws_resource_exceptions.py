

class CatalogEntityAlreadyExistsException(Exception):
    '''
    Exception raised when a catalog entity already exists.
    '''
    def __init__(self, message):
        super().__init__(message)

class CatalogEntityNotFoundException(Exception):
    '''
    Exception raised when a catalog entity is not found.
    '''
    def __init__(self, message):
        super().__init__(message)

class CatalogEntityMismatchException(Exception):
    '''
    Exception raised when either the catalog id or database id does not match when expected to.
    '''
    def __init__(self, message):
        super().__init__(message)

class InvalidArnException(Exception):
    '''
    Exception raised when an ARN is invalid.
    '''
    def __init__(self, message):
        super().__init__(message)