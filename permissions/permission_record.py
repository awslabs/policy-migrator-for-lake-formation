

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
        return f'PrincipalArn: "{self._principal_arn}" ResourceArn: "{self._resource_arn}" PermissionActions: [{list(self._permission_actions)}]'

    def __eq__(self, other) -> bool:
        return isinstance(other, PermissionRecord) and \
            self._principal_arn == other._principal_arn and \
            self._resource_arn == other._resource_arn and \
            self._permission_actions == other._permission_actions

    def __hash__(self) -> int:
        return hash((self._principal_arn, self._resource_arn, frozenset(self._permission_actions)))

    def __lt__(self, other) -> bool:
        if not isinstance(other, PermissionRecord):
            return NotImplemented
        return (self._principal_arn, self._resource_arn, sorted(self._permission_actions)) < \
               (other._principal_arn, other._resource_arn, sorted(other._permission_actions))
