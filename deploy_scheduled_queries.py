from google.cloud import bigquery_datatransfer,bigquery_datatransfer_v1
import json
import os
from dotenv import load_dotenv
from google.oauth2 import service_account
import yaml

load_dotenv()
service_account_info = json.loads(os.getenv('GCP_SA_KEY'))
credentials = service_account.Credentials.from_service_account_info(service_account_info)

# Initialize a BigQuery Data Transfer Service client
transfer_client = bigquery_datatransfer.DataTransferServiceClient(credentials=credentials)

# Your Google Cloud project id and dataset id where the scheduled query will be stored
project_id = os.getenv('GCP_PROJECT_ID')
dataset_id = "nba_scheduled"
location = "US"  # set to your dataset's location

# The parent resource where the transfer configuration will be created.
parent = transfer_client.common_project_path(project_id)

configs = transfer_client.list_transfer_configs(parent=parent)


def deploy_scheduled_queries(config_file):

    project_id = os.getenv('GCP_PROJECT_ID')
    parent = transfer_client.common_project_path(project_id)

    with open(config_file, 'r') as file:
        config = yaml.safe_load(file)
    
    for query in config['scheduled_queries']:
        with open(query['queryFilePath'], 'r') as sql_file:
            sql_query = sql_file.read()

        if query['name'] not in [config.display_name for config in configs]:

            transfer_config = bigquery_datatransfer.TransferConfig(
                destination_dataset_id=query['destinationDataset'],
                display_name=query['name'],
                data_source_id="scheduled_query",
                params={
                    "query": sql_query,
                    "destination_table_name_template": query['destinationTable'],
                    "write_disposition": query['writeDisposition'],
                    "partitioning_field": query['partitioningField'],
                },
                schedule=query['schedule'],
                schedule_options=bigquery_datatransfer_v1.types.ScheduleOptions(
                    start_time=query['startTime'],
                    end_time=query['endTime'],
                )
            )

            transfer_config = transfer_client.create_transfer_config(
                bigquery_datatransfer.CreateTransferConfigRequest(
                    parent=parent,
                    transfer_config=transfer_config,
                )
            )

            print(f"Created scheduled query '{transfer_config.name}'")


deploy_scheduled_queries('config/scheduled_queries.yaml')


