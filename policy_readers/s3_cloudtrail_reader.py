

from policy_readers.policy_reader_interface import PolicyReaderInterface
from permissions.permissions_list import PermissionsList
from permissions.permissions_list import PermissionRecord

from config.application_configuration import ApplicationConfiguration
from config.configuration_exceptions import ConfigurationInvalidException

import awswrangler as wr
import pandas as pd

import logging
logger = logging.getLogger(__name__)

class S3CloudTrailDataEventsReader(PolicyReaderInterface):
    """This class executes an Athena query against a CloudTrail table to derive equalivant Lake Formation permissions for S3 Data Events. 
        All the required configuration must be included. You can read what the required configuration is by calling get_required_configuration.
        
    Limitations:
        - Only IAM User, IAM Role are the identity types that are currently supported.
    """

    _REQUIRED_CONFIGURATION : dict = {
            "athena_workgroup": "The workgroup to execute Athenas queries in.",
            "athena_cloudtrail_database": "The database in which CloudTrail table exists.",
            "athena_cloudtrail_table": "The table in which CloudTrail table exists.",
            "athena_query_results_location": "The output location of the query results."
        }

    _CONFIGURATION_SECTION : str = "policy_reader_s3_event_cloudtrail"

    def __init__(self, appConfig : ApplicationConfiguration, conf : dict[str]):
        assert appConfig is not None
        super().__init__(appConfig, conf)

        self._boto3_session = appConfig.get_boto3_session()
        self._s3_to_table_mapper = appConfig.get_s3_to_table_translator()

    def read_policies(self) -> PermissionsList:
        """This function reads policies from CloudTrail by executing an Athena query for S3 Data Events. """
        self._validate_application_conf()
        logger.info("Reading policies from CloudTrail for S3 access.")

        sql = f"""WITH
                    cloudtrail AS (
                        SELECT 
                            CASE WHEN useridentity.type = 'IAMUser' THEN
                                    useridentity.arn
                                WHEN useridentity.type = 'AssumedRole' THEN
                                    useridentity.sessioncontext.sessionissuer.arn
                                ELSE NULL
                            END as principal_arn,
                            eventname,
                            CONCAT('arn:aws:s3:::', s3_bucket, '/',
                                -- if there is a '/' character
                                IF (strpos(s3_key, '/') <> 0,
                                    substr(s3_key, 1, length(s3_key) - strpos(reverse(s3_key), '/') + 1),
                                    -- else return blank. its an object at the base directory.
                                    ''
                                )
                            ) as s3_path
                        FROM 
                            ( SELECT *, json_extract_scalar(requestparameters, '$.key') as s3_key, 
                                    json_extract_scalar(requestparameters, '$.bucketName') as s3_bucket
                                FROM \"{self._config["athena_cloudtrail_database"]}\".\"{self._config["athena_cloudtrail_table"]}\" )
                                    cloudtrail
                        WHERE eventname in ('GetObject','HeadObject','PutObject','CreateMultipartUpload', 'UploadPart','UploadPartCopy','DeleteObject')
                            AND errorcode IS NULL
                            AND requestparameters NOT LIKE '%.hive-staging_%'
                        GROUP BY 1, 2, 3
                        )
                    SELECT
                        principal_arn,
                        s3_path,
                        array_distinct(array_agg(eventname)) as events
                    FROM
                        cloudtrail
                    where principal_arn is not null
                    GROUP BY 1, 2
                """

        logger.debug(f"Running query: {sql}")

        try:
            results_df = wr.athena.read_sql_query(
                    sql,
                    database=self._config["athena_cloudtrail_database"],
                    ctas_approach=False,
                    s3_output=self._config["athena_query_results_location"],
                    boto3_session=self._boto3_session,
                    workgroup=self._config["athena_workgroup"]
            )
        except Exception as e:
            logger.error(f"Was not able to run Athena Query with SQL: {sql} Exception: {e}")
            raise

        # Columns: user_arn, eventname, permission, resource_level, resource, database_name, table_name
        permissions_list = PermissionsList()

        for _, row in results_df.iterrows():
            if self._has_nulls(row, row['principal_arn'], row['s3_path']):
                logger.error(f"Found unexpected null value in either ARN or S3 Path row: {row}")
                continue

            # Filter out any s3 locations that do not map to a Glue Table.
            tables = self._s3_to_table_mapper.get_tables_from_s3_location_postfix(row['s3_path'])
            if not tables:
                logger.debug(f"S3 Location {row['s3_path']} doesn't have any glue tables.")
                continue

            permission_record = PermissionRecord(
                    row['principal_arn'],
                    row["s3_path"],
                    self._parse_events(row['events'])
                )
            logger.debug(f"Adding permission record for table: {permission_record}")
            permissions_list.add_permission_record(permission_record)
        return permissions_list

    def _validate_application_conf(self):
        for key in self._REQUIRED_CONFIGURATION:
            if key not in self._config:
                raise ConfigurationInvalidException("GlueCloudTrail Reader: Missing configuration for " + key)

    @classmethod
    def get_name(cls):
        """Gets the name of this reader. """
        return S3CloudTrailDataEventsReader.__name__

    @classmethod
    def get_required_configuration(cls) -> dict:
        """Returns the required configuration for this reader. """
        return S3CloudTrailDataEventsReader._REQUIRED_CONFIGURATION

    @classmethod
    def get_config_section(cls) -> str:
        return S3CloudTrailDataEventsReader._CONFIGURATION_SECTION

    def _has_nulls(self, row, *values):
        for item in values:
            if pd.isna(item):
                with pd.option_context('expand_frame_repr', False):
                    logger.debug(f"---------------------> Found unexpected null value in row: \n{row}")
                return True
        return False

    def _parse_events(self, events):
        """Parses the events from a CloudTrail row. """
        events = events[1:-1]
        events = events.split(',')
        events = [f"s3:{event.strip()}" for event in events]
        return events
