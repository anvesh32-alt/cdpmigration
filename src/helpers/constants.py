PROJECT = 'dbce-c360-isl-preprod-d942'

DAG_BUCKET = 'us-east4-isl-composer-2-7238166d-bucket'

CONFIG_FILE_PATH = 'dags/cdp2.0-migration/src/configurations/'

RETENTION_CONFIG_FILE = 'retentionconfig.json'

TEMP_LOCATION = 'gs://dbce-c360-isl-preprod-d942-migration-88fakc29/temp'

TEST_PUBSUB_TOPIC = 'projects/dbce-c360-isl-preprod-d942/topics/cdp.migration-test'

REGISTRATION_PUBSUB_TOPIC = 'projects/dbce-c360-isl-preprod-d942/topics/segment-to-cdp-registration'

CLK_PUBSUB_TOPIC = 'projects/dbce-c360-isl-preprod-d942/topics/segment-to-cdp-clickstream'

CLT_PUBSUB_TOPIC = 'projects/dbce-c360-isl-preprod-d942/topics/segment-to-cdp-clt'

OTHER_PUBSUB_TOPIC = 'projects/dbce-c360-isl-preprod-d942/topics/segment-to-cdp-other-events'

REGISTRATION_BIGQUERY_DATASET = 'cdp2_registration_data'

CLICKSTREAM_BIGQUERY_DATASET = 'cdp2_clickstream_data'

CLT_BIGQUERY_DATASET = 'cdp2_clt_data'

ID_CONSENT_PUBSUB = {'id_service': 'projects/dbce-c360-isl-prod-8859/topics/cdp-transformation-ids-migration',
                     'consent_service': 'projects/dbce-c360-isl-prod-8859/topics/cdp-transformation-consents-migration'}

DLQ_DATASET_NAME = 'Migration_DLQ'

DLQ_TIMESTAMP_CONFIG = 'dags/dlq_timestamp_config/dlq_remediation_timestamp.json'

REGISTRATION_PILLAR_PUBSUB_TOPIC = 'projects/dbce-c360-isl-preprod-d942/topics/cdp-registrations'

FRONT_LOAD_VALIDATION_TABLE_SCHEMA = 'message_id:STRING,raw_payload:STRING,mpn:STRING,' \
                                     'dq_validation_id:STRING,dq_validation_description:STRING,error_details:STRING,' \
                                      'reported_at:TIMESTAMP'

PG_ID_UPDATE_DATASET = 'CDP_PG_ID_UPDATE'

MDM_PRE_PROD_PROJECT_ID = 'dbce-c360-mdm-pre-prod-4bfd'

MDM_STG_PROD_PROJECT_ID = 'dbce-c360-mdm-stg-e2a7'

MIGRATION_BUCKET = 'dbce-c360-isl-preprod-d942-migration-88fakc29'

MIGRATION_DATASET = 'cdp_migration'

SECRET_URL = 'projects/610207534327/secrets/{}/versions/latest'

MAPP_SFTP_SECRET_KEY = 'migration-crm-downstream-sftp-token_mapp'

RESCI_SFTP_SECRET_KEY = 'migration-crm-downstream-sftp-token_resci'

LYTICS_SFTP_SECRET_KEY = 'migration-crm-downstream-sftp-token_lytics'

AIRFLOW_LOCAL_TMP_PATH = '/tmp/migration_pg_id_update/'

RESCI_REMOTE_PATH = '/uploads/Data Transfer'

ISL_PROD_PROJECT = 'dbce-c360-isl-prod-8859'

PROJECT_TABLES = {"AMA":"dbce-c360-segamaws-prod-9669", "EU":"dbce-c360-segglblws-prod-b902", "EU_corp":"dbce-c360-segglblws-prod-b902","US":"dbce-c360-segamerws-prod-35ac"}

RETENTION_DATASET = {"AMA":"CDP_Retention_AMA", "EU":"CDP_Retention", "EU_corp":"CDP_Retention", "US":"CDP_Retention"}

CONNECTION_TYPE = {"preprod":"-pre-prod","preprodlive":"-pre-prod","prod":""}

AUDIENCE_DASHBOARD_TABLE_NAME = {"preprod" : "hypercare_audience_counts_preprod", "preprodlive" : "hypercare_audience_counts_preprod", "prod" : "hypercare_audience_counts_prod_main"}

AUDIENCE_DUMPS_TABLE = {"preprod" : "preprod", "preprodlive" : "preprod", "prod" : "prod"}

UNMERGE_TABLE = {"preprod" : "pre_prod", "preprodlive" : "pre_prod", "prod" : "prod"}

BIGQUERY_REGION_VARIABLE = {"AMA" : "ama", "EU" : "eu", "EU_corp" : "eu", "US" : "us"}

UNMERGE_DATASET = {"AMA" : "CDP_unmerge_east4", "EU" : "CDP_unmerge", "EU_corp" : "CDP_unmerge", "US" : "CDP_unmerge"}