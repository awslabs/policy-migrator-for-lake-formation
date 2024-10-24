from permissions.permissions_list import PermissionsList
from policy_readers.policy_reader_interface import PolicyReaderInterface
from config.application_configuration import ApplicationConfiguration
from config.configuration_exceptions import ConfigurationInvalidException

import awswrangler as wr
import pandas as pd
import boto3

import logging
logger = logging.getLogger(__name__)

class GlueEventCloudTrailPolicyReader(PolicyReaderInterface):
    """This class executes an Athena query against a CloudTrail table to derive equalivant Lake Formation permissions. All the required configuration must be included. 
       You can read what the required configuration is by calling get_required_configuration.
        
    Limitations:
    - For calls like BatchCreatePartitions, if the request parameters are too large, we wont be able to extract database or table names because CloudTrail truncates the records.
    - Only IAM User, IAM Role are the identity types that are currently supported.
    - For some cases, Database Name may be missing from CloudTrail for GetTable calls. 
    - If a user has created a database, then dropped it, and then another user re-creates the database, the original user will still have SUPER permissions on the database.
    """

    _REQUIRED_CONFIGURATION : dict = {
            "athena_workgroup": "The workgroup to execute Athenas queries in.",
            "athena_cloudtrail_database": "The database in which CloudTrail table exists.",
            "athena_cloudtrail_table": "The table in which CloudTrail table exists.",
            "athena_query_results_location": "The output location of the query results."
        }

    _CONFIGURATION_SECTION : str = "policy_reader_glue_event_cloudtrail"

    def __init__(self, appConfig : ApplicationConfiguration, conf : dict[str]):
        super().__init__(appConfig, conf)
        self._boto3_session : boto3.Session = appConfig.get_boto3_session()

    def read_policies(self) -> PermissionsList:
        """This function reads policies from CloudTrail by executing an Athena query. """
        self._validate_application_conf()
        logger.info("Reading policies from CloudTrail from Glue Data Catalog access.")

        sql = f"""WITH cloudtrail as (
                SELECT *, 
                    CASE
                        WHEN eventname in ('CreateDatabase', 'GetDatabases') THEN 'CATALOG'
                        WHEN eventname in ('GetDatabase','UpdateDatabase','DeleteDatabase','CreateTable','GetTables') THEN 'DATABASE'
                        WHEN eventname in ('GetTable','GetTablesVersion','GetTablesVersions','GetPartition','GetUnfilteredPartition','GetInternalUnfilteredPartition','GetInternalUnfilteredPartitions','GetPartitions','GetUnfilteredPartitions','BatchGetPartition','GetPartitionIndexes','DESCRIBE','UpdateTable','DeleteTableVersion','BatchDeleteTableVersion','BatchCreatePartition','CreatePartition','DeletePartition','BatchDeletePartition','UpdatePartition','BatchUpdatePartition','CreatePartitionIndex','DeletePartitionIndex','DeleteTable') THEN 'TABLE'
                    END as resource_level
                    FROM
                    \"{self._config["athena_cloudtrail_database"]}\".\"{self._config["athena_cloudtrail_table"]}\"
                    WHERE
                    eventsource = 'glue.amazonaws.com' and 
                    eventname in ('GetDatabase',
                                    'GetDatabases',
                                    'UpdateDatabase',
                                    'DeleteDatabase',
                                    'CreateTable',
                                    'CreateDatabase',
                                    'GetTable',
                                    'GetTables',
                                    'GetTablesVersion',
                                    'GetTablesVersions',
                                    'GetPartition',
                                    'GetUnfilteredPartition',
                                    'GetInternalUnfilteredPartition',
                                    'GetInternalUnfilteredPartitions',
                                    'GetPartitions',
                                    'GetUnfilteredPartitions',
                                    'BatchGetPartition',
                                    'GetPartitionIndexes',
                                    'UpdateTable',
                                    'DeleteTableVersion',
                                    'BatchDeleteTableVersion',
                                    'BatchCreatePartition',
                                    'CreatePartition',
                                    'DeletePartition',
                                    'BatchDeletePartition',
                                    'UpdatePartition',
                                    'BatchUpdatePartition',
                                    'CreatePartitionIndex',
                                    'DeletePartitionIndex',
                                    'DeleteTable') 
                    and errorcode IS NULL
                    -- We only support these useridentity types for now
                    and useridentity.type in ('IAMUser', 'AssumedRole')
                )
                SELECT DISTINCT
                    CASE WHEN useridentity.type = 'IAMUser' THEN useridentity.arn
                         WHEN useridentity.type = 'AssumedRole' THEN useridentity.sessioncontext.sessionissuer.arn
                         ELSE NULL
                    END as user_arn, 
                    eventname,
                    -- the following translation is not used, rather we use GlueDataCatalogActionTranslator instead.
                    CASE
                        -- Database level permissions
                        WHEN eventname in ('GetDatabase') THEN 'DESCRIBE'
                        WHEN eventname in ('UpdateDatabase') THEN 'ALTER'
                        WHEN eventname in ('DeleteDatabase') THEN 'DROP'
                        WHEN eventname in ('CreateTable') THEN 'CREATE_TABLE'

                        WHEN eventname in ('CreateDatabase') THEN 'CREATE_DATABASE'

                        -- These action have no permission requirements.
                        -- WHEN eventname in ('GetDatabases') THEN 'LIST_DBS'
                        WHEN eventname in ('GetTables') THEN 'DESCRIBE'

                        -- Table Level permissions
                        
                        WHEN eventname in ('GetTable','GetTablesVersion','GetTablesVersions','GetPartition','GetUnfilteredPartition','GetInternalUnfilteredPartition','GetInternalUnfilteredPartitions','GetPartitions','GetUnfilteredPartitions','BatchGetPartition','GetPartitionIndexes') THEN 'DESCRIBE'
                        WHEN eventname in ('UpdateTable','DeleteTableVersion','BatchDeleteTableVersion','BatchCreatePartition','CreatePartition','DeletePartition','BatchDeletePartition','UpdatePartition','BatchUpdatePartition','CreatePartitionIndex','DeletePartitionIndex') THEN 'ALTER'
                        WHEN eventname in ('DeleteTable') THEN 'DROP'
                        ELSE 'UNKNOWN'
                    END as permission,
                    resource_level,
                    requestparameters as resource,
                    awsRegion,
                    CASE
                        WHEN json_extract_scalar(requestparameters, '$.catalogId') IS NOT NULL THEN json_extract_scalar(requestparameters, '$.catalogId')
                        ELSE useridentity.accountid
                    END as aws_account_id,
                    CASE 
                        WHEN resource_level = 'DATABASE' THEN
                            CASE WHEN eventname in ('GetTables', 'CreateTable') THEN json_extract_scalar(requestparameters, '$.databaseName')
                            ELSE json_extract_scalar(requestparameters, '$.name')
                            END
                        WHEN eventname = 'CreateDatabase' THEN json_extract_scalar(requestparameters, '$.databaseInput.name')
                        ELSE json_extract_scalar(requestparameters, '$.databaseName')
                    END as database_name,
                    CASE 
                        WHEN resource_level = 'TABLE' THEN 
                            CASE WHEN json_extract_scalar(requestparameters, '$.tableName') IS NULL THEN json_extract_scalar(requestparameters, '$.name')
                            ELSE json_extract_scalar(requestparameters, '$.tableName')
                            END
                        WHEN eventname = 'CreateTable' THEN json_extract_scalar(requestparameters, '$.tableInput.name')
                    END as table_name
                FROM 
                    cloudtrail
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
            logger.error(f"Was not able to run Athena Query with SQL: {sql} with error: {e}")
            raise e

        # Columns: user_arn, eventname, permission, resource_level, resource, database_name, table_name
        permissions_list = PermissionsList()

        for _, row in results_df.iterrows():
            if (self._has_nulls(row, row['resource_level'], row['user_arn'], ['awsRegion'], row['aws_account_id'], row['eventname'])): continue
            if row['resource_level'] == 'CATALOG':
                permissions_list.add_permission(
                    row['user_arn'],
                    f"arn:aws:glue:{row['awsRegion']}:{row['aws_account_id']}:catalog",
                    f"glue:{row['eventname']}"
                )
            elif row['resource_level'] == 'DATABASE':
                if (self._has_nulls(row, row['database_name'])):
                    continue
                # Permission on the database
                permissions_list.add_permission(
                    row['user_arn'],
                    f"arn:aws:glue:{row['awsRegion']}:{row['aws_account_id']}:database/{row['database_name']}",
                    f"glue:{row['eventname']}"
                )
            elif row['resource_level'] == 'TABLE':
                if (self._has_nulls(row, row['database_name'], row['table_name'])):
                    continue
                permissions_list.add_permission(
                    row['user_arn'],
                    f"arn:aws:glue:{row['awsRegion']}:{row['aws_account_id']}:table/{row['database_name']}/{row['table_name']}",
                    f"glue:{row['eventname']}"
                )
            else:
                logger.warning(f"Unknown resource type: {row['resource']}")
                continue

        return permissions_list

    def _validate_application_conf(self):
        for key in self._REQUIRED_CONFIGURATION:
            if key not in self._config:
                raise ConfigurationInvalidException("GlueCloudTrail Reader: Missing configuration for " + key)

    @classmethod
    def get_name(cls):
        """Gets the name of this reader. """
        return GlueEventCloudTrailPolicyReader.__name__

    @classmethod
    def get_required_configuration(cls) -> dict:
        """Returns the required configuration for this reader. """
        return GlueEventCloudTrailPolicyReader._REQUIRED_CONFIGURATION

    @classmethod
    def get_config_section(cls) -> str:
        return GlueEventCloudTrailPolicyReader._CONFIGURATION_SECTION

    def _has_nulls(self, row, *values):
        for item in values:
            if pd.isna(item):
                with pd.option_context('expand_frame_repr', False):
                    logger.debug(f"---------------------> Found unexpected null value in row: \n{row}")
                return True
        return False
