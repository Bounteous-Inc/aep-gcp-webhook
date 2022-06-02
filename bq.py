from google.cloud import bigquery
import json

PROJECT_ID = 'maximal-symbol-232200'
BQ_DATASET = 'webhook'
BQ_TABLE = 'event_log'
BQ = bigquery.Client()

event = {
  "event_id": "5ef3316c-8873-429e-8d1d-a8c2042f6224",
  "event": {
    "xdm:ingestionId": "01G3WG4RZ3E6MZ8MXYPX1ZEJY0",
    "xdm:customerIngestionId": "999m-300089574_export_122042419_1-1_1220425021834982.zdw.xz_2022-05-25_02-18-39",
    "xdm:imsOrg": "0B96B03459707BE40A495C70@AdobeOrg",
    "xdm:completed": 1653447022310,
    "xdm:datasetId": "6244a97e78823f1949b98c47",
    "xdm:eventCode": "ing_load_success",
    "xdm:sandboxName": "adobe-ajo",
    "xdm:successfulRecords": 5422,
    "xdm:failedRecords": 0
  },
  "recipient_client_id": "4a83f975781a4169b7974a642d42f0e8"
}

#event = json.load(sample_event)

""" Target Table structure 

CREATE TABLE webhook.event_log (
  event_id string not null,
  recipient_client_id string,
  batch_id string not null,
  completed int64,
  parent_ingestion_id string,
  dataset_id string not null,
  event_code string not null,
  sandbox_name string not null,
  successful_records int64,
  failed_records int64
);
"""

row = {
    "event_id": event["event_id"],
    "recipient_client_id": event["recipient_client_id"],
    "batch_id": event["event"]["xdm:ingestionId"],
    "completed": event["event"]["xdm:completed"],
    "parent_ingestion_id": event["event"].get("xdm:parentIngestionId"),
    "dataset_id": event["event"]["xdm:datasetId"],
    "event_code": event["event"]["xdm:eventCode"],
    "sandbox_name": event["event"]["xdm:sandboxName"]
}

print(row)

table = BQ.dataset(BQ_DATASET).table(BQ_TABLE)
errors = BQ.insert_rows_json(table,json_rows=[row])
if errors != []:
    print(errors)