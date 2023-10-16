
from elt_project.connectors.airbyte import AirbyteClient
from dotenv import load_dotenv
import os 


if __name__ == "__main__":
    load_dotenv()

    AIRBYTE_USERNAME = os.environ.get("AIRBYTE_USERNAME")
    AIRBYTE_PASSWORD = os.environ.get("AIRBYTE_PASSWORD")
    AIRBYTE_SERVER_NAME = os.environ.get("AIRBYTE_SERVER_NAME")


    airbyte_client = AirbyteClient(server_name=AIRBYTE_SERVER_NAME, username=AIRBYTE_USERNAME, password=AIRBYTE_PASSWORD)
    if airbyte_client.valid_connection(): 
        airbyte_client.trigger_sync(connection_id='a8119660-0a74-4428-813a-f550bfc9cec6')
