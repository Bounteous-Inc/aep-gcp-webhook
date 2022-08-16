from logging import exception
import functions_framework
from google.cloud import bigquery
from google.cloud import pubsub_v1
import json
import urllib.request
import os

@functions_framework.http
def webhook(request):
    if request.method == 'POST':
        event = request.json

        ##After Python3.7 Google deprecated the GCP_PROJECT env variable, and requires a request to the metadata server to get the project_id.   
        url = "http://metadata.google.internal/computeMetadata/v1/project/project-id"
        req = urllib.request.Request(url)
        req.add_header("Metadata-Flavor", "Google")
        PROJECT_ID = urllib.request.urlopen(req).read().decode()
        
        #Retrieve the BQ and pubsub details from the env variables
        BQ_DATASET = os.environ.get('BQ_DATASET')
        BQ_TABLE = os.environ.get('BQ_TABLE')
        PUBSUB_TOPIC_ID = os.environ.get('PUBSUB_TOPIC')

        BQ = bigquery.Client()

        ## Re-map the incoming dict into the BQ event_log structure
        row = {
            "event_id": event["event_id"],
            "recipient_client_id": event.get("recipient_client_id"),
            "batch_id": event["event"].get("xdm:ingestionId"),
            "completed": event["event"].get("xdm:completed"),
            "parent_ingestion_id": event["event"].get("xdm:parentIngestionId"),
            "dataset_id": event["event"].get("xdm:datasetId"),
            "event_code": event["event"].get("xdm:eventCode"),
            "sandbox_name": event["event"].get("xdm:sandboxName"),
            "successful_records": event["event"].get("xdm:successfulRecords"),
            "failed_records": event["event"].get("xdm:failedRecords")
        }

        table = BQ.dataset(BQ_DATASET).table(BQ_TABLE)
        errors = BQ.insert_rows_json(table,json_rows=[row])
        if errors != []:
            print(errors)

        ## If the event was a successful ingestion load, then publish to pub/sub to further process
        if row.get("event_code") == "ing_load_success":
            ##Publish message to pubsub topic for further processing
            publisher = pubsub_v1.PublisherClient()
            topic_path = publisher.topic_path(PROJECT_ID, PUBSUB_TOPIC_ID)
            data_str = json.dumps(row)
            data = data_str.encode('utf-8')
            future = publisher.publish(
                topic_path, data, origin="python-sample", username="gcp"
            )
            print(future.result())
 
            print(f"published messages with custom attributes to {topic_path}.")
            
        return(event, 200, None)

    elif request.method == 'GET':
        request_json = request.get_json(silent=True)
        request_args = request.args

        ### if there is a challenge query parameter present, then echo that back in the response
        if request_args and 'challenge' in request_args:
            response = request_args.get('challenge')
        else:
            response = 'Get Successful!'
        return(response, 200, None)
    
    else:
        return('error: not implemented', 403, None)

