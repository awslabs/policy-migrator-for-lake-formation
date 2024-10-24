

class PermissionRecord:
    '''
        Represents a single permission record which contains Principal Arn, Resource Arn, and Permission Actions 
        (which can be IAM actions or Lake Formation Actions)
    '''

    def __init__(self, principal_arn : str, resource_arn : str, permission_actions : set[str]):
        assert isinstance(permission_actions, set)
        assert isinstance(resource_arn, str)
        assert isinstance(principal_arn, str)

        self._principal_arn = principal_arn
        self._resource_arn = resource_arn
        self._permission_actions = permission_actions

    def principal_arn(self) -> str:
        return self._principal_arn

    def resource_arn(self) -> str:
        return self._resource_arn

    def permission_actions(self) -> set[str]:
        return self._permission_actions

    def __str__(self) -> str:
        return f'PrincipalArn: "{self._principal_arn}" ResourceArn: "{self._resource_arn}" PermissionActions: [{[action for action in self._permission_actions]}]'

    def __lt__(self, other) -> bool:
        if other is None or not isinstance(other, PermissionRecord):
            return False

        return self.principal_arn() < other.principal_arn() and \
            self.resource_arn() < other.resource_arn() and \
            self.permission_actions() < other.permission_actions()
