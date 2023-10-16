from jinja2 import Environment, FileSystemLoader
from etl_project.connectors.postgresql import PostgreSqlClient
from etl_project.connectors.airbyte import AirbyteClient
from dotenv import load_dotenv
import os 

from etl_project.assets.pipeline_logging import PipelineLogging
from etl_project.assets.metadata_logging import MetaDataLogging, MetaDataLoggingStatus
from sqlalchemy.exc import SQLAlchemyError

if __name__ == "__main__":
    load_dotenv()
    LOGGING_SERVER_NAME = os.environ.get("LOGGING_SERVER_NAME")
    LOGGING_DATABASE_NAME = os.environ.get("LOGGING_DATABASE_NAME")
    LOGGING_USERNAME = os.environ.get("LOGGING_USERNAME")
    LOGGING_PASSWORD = os.environ.get("LOGGING_PASSWORD")
    LOGGING_PORT = os.environ.get("LOGGING_PORT")

    postgresql_logging_client = PostgreSqlClient(
        server_name=LOGGING_SERVER_NAME,
        database_name=LOGGING_DATABASE_NAME,
        username=LOGGING_USERNAME,
        password=LOGGING_PASSWORD,
        port=LOGGING_PORT
    )

    metadata_logging = MetaDataLogging(pipeline_name="northwind", postgresql_client=postgresql_logging_client)
    pipeline_logging = PipelineLogging(pipeline_name="northwind", log_folder_path="etl_project/logs")

    AIRBYTE_USERNAME = os.environ.get("AIRBYTE_USERNAME")
    AIRBYTE_PASSWORD = os.environ.get("AIRBYTE_PASSWORD")
    AIRBYTE_SERVER_NAME = os.environ.get("AIRBYTE_SERVER_NAME")

    try:
        metadata_logging.log() # start run
        pipeline_logging.logger.info("Creating target client")

        airbyte_client = AirbyteClient(server_name=AIRBYTE_SERVER_NAME, username=AIRBYTE_USERNAME, password=AIRBYTE_PASSWORD)
        if airbyte_client.valid_connection(): 
            airbyte_client.trigger_sync(connection_id="bf8d8579-8004-4e9d-8774-7bf4f0f2a381")

    except SQLAlchemyError as e:
        pipeline_logging.logger.error(f"Pipeline failed with exception {e}")
        metadata_logging.log(status=MetaDataLoggingStatus.RUN_FAILURE, logs=pipeline_logging.get_logs()) 
        pipeline_logging.logger.handlers.clear()
