
import boto3
import logging

logger = logging.getLogger(__name__)

class DataLakeLocationCommitter:
    '''
    Commits Data Lake Locations to Lake Formation. It will always use Hybrid Access mode as to not 
    disrupt existing processing. 
    '''

    def __init__(self, boto3_session : boto3.Session, locations : list, registration_role : str):
        self._lf_client = boto3_session.client('lakeformation')
        self._locations = locations
        self._registration_role = registration_role

    def register_locations(self):
        if self._registration_role is None or "/aws-service-role/" in self._registration_role:
            useServiceLinkedRole = True
        else:
            useServiceLinkedRole = False

        for location in self._locations:
            try:
                if useServiceLinkedRole:
                    self._lf_client.register_resource(
                        ResourceArn=location,
                        UseServiceLinkedRole=True,
                        HybridAccessEnabled=True
                    )
                else:
                    self._lf_client.register_resource(
                        ResourceArn=location,
                        UseServiceLinkedRole=False,
                        RoleArn=self._registration_role,
                        HybridAccessEnabled=True
                    )
            except self._lf_client.exceptions.AlreadyExistsException:
                logger.warning(f"Location already registered: {location}")
            except self._lf_client.exceptions.ResourceNumberLimitExceededException as e:
                logger.error("Number of locations registered in Lake Formation as been exceeded.")
                raise e
            except Exception as e:
                logger.error(f"Error registering location: {location} with error: {e}")
                raise e
