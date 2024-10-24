from .permission_record import PermissionRecord

import logging
logger = logging.getLogger(__name__)

class PermissionsList:
    ''' Holds a list of permissions. 
       _permissions : dict where its keys are [principal_arn] = dict() and its key 
                   is [resoucre_arn] that holds an array of permissionActions
       _permissions[principal_arn][resource_arn][permissions_list]'''

    def __init__(self):
        self._permissions = {}
        self._permissions_count = 0

    def add_permission(self, principal_arn : str,
                       resource_arn : str, iam_action : str) -> bool:
        assert isinstance(iam_action, str)
        assert isinstance(resource_arn, str)
        assert isinstance(principal_arn, str)

        actions = self._permissions.setdefault(principal_arn, dict()).setdefault(resource_arn, set())
        if not actions:
            self._permissions_count += 1
        if iam_action not in actions:
            actions.add(iam_action)
            logger.debug(f"Added Permission: {principal_arn}, Resource: {resource_arn}, Action: {iam_action}")
            return True
        return False

    def add_permission_record(self, permissionRecord : PermissionRecord) -> bool:
        """
        Adds the permissions from a Permission Record to this Permissions List. If a permission
        already exists, it will ignore it. 
        Returns: True if a permission was added, False if a permission already existed
        """
        actions = self._permissions.setdefault(permissionRecord.principal_arn(), dict()).setdefault(permissionRecord.resource_arn(), set())
        if not actions:
            self._permissions_count += 1
        for action in permissionRecord.permission_actions():
            if action not in actions:
                actions.update(permissionRecord.permission_actions())
            logger.debug(f"Added Permission: {permissionRecord.principal_arn()}, Resource: {permissionRecord.resource_arn()}, Actions: {actions}")
            return True
        return False

    def get_permissions_count(self):
        return self._permissions_count

    def delete_permission(self, prinicpal_arn : str, resource_arn : str):
        principal_perms = self._permissions.get(prinicpal_arn)
        if principal_perms is None:
            return

        if resource_arn in principal_perms:
            del principal_perms[resource_arn]
            self._permissions_count -= 1

        if not principal_perms:
            del self._permissions[prinicpal_arn]

    def get_permission_actions(self, prinicpal_arn : str,
                       resource_arn : str) -> list:
        return self._get_actions(prinicpal_arn, resource_arn)

    def get_permissions_for_principal(self, principal_arn : str):
        permissions = self._permissions.get(principal_arn)
        if permissions is None:
            return iter([])
        return iter(_ResourcesIterator(principal_arn, permissions))

    def get_permissions(self) -> list:
        all_permissions = []
        for permission in self:
            all_permissions.append(permission)
        return all_permissions

    def get_principal_arns(self):
        return list(self._permissions.keys())

    def add_permissions_from_list(self, permissions_list):
        for permission in permissions_list:
            self.add_permission_record(permission)

    def remove_permission(self, prinicpal_arn : str,
                       resource_arn : str, iam_actions : set[str]):
        actions = self._get_actions(prinicpal_arn, resource_arn)
        if actions is None:
            return

        actions.difference_update(iam_actions)

        if not actions:
            del self._permissions[prinicpal_arn][resource_arn]
            self._permissions_count -= 1
            if not self._permissions[prinicpal_arn]:
                del self._permissions[prinicpal_arn]

    def remove_permissions(self, permissions_list):
        for permission_to_remove in permissions_list:
            self.remove_permission(permission_to_remove.principal_arn(),
                                    permission_to_remove.resource_arn(),
                                    permission_to_remove.permission_actions()
                                    )

    def _get_actions(self, principal_arn : str, resource_arn : str):
        permissions = self._permissions.get(principal_arn)
        if permissions is None:
            return None

        record = permissions.get(resource_arn, None)
        if record is None:
            return None
        return record

    def __str__(self):
        output = []
        for permission in self:
            output.append(str(permission))

        return "\n".join(output)

    def __iter__(self):
        return _PermissionsIterator(self._permissions)

class _ResourcesIterator:
    def __init__(self, principal_arn : str, resources):
        self._resources = resources
        self._principal_arn = principal_arn
        self._resources_iter = iter(self._resources.items())

    def __iter__(self):
        return self

    def __next__(self):
        resource_arn, resource_actions = next(self._resources_iter)
        return PermissionRecord(self._principal_arn,
                                resource_arn,
                                resource_actions)


class _PermissionsIterator:

    def __init__(self, permissions):
        self._permissions = permissions
        self._principal_arns_iter = iter(self._permissions.items())
        self._principal_arn = None
        self._resources_iter = None

    def __next__(self) -> PermissionRecord:
        if self._principal_arn is None:
            self._principal_arn, resources = next(self._principal_arns_iter)
            self._resources_iter = iter(_ResourcesIterator(self._principal_arn, resources))

        try:
            return next(self._resources_iter)

        except StopIteration:
            self._principal_arn, resources = next(self._principal_arns_iter)
            self._resources_iter = iter(_ResourcesIterator(self._principal_arn, resources))
            return self.__next__()

    def __iter__(self):
        return self
