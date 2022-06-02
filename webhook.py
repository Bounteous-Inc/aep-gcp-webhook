import functions_framework
from google.cloud import bigquery
import json
import jwt #pyjwt
import requests
import os.path
import datetime

@functions_framework.http
def webhook(request):
    if request.method == 'POST':
        event = request.json

        PROJECT_ID = 'maximal-symbol-232200'
        BQ_DATASET = 'webhook'
        BQ_TABLE = 'event_log'
        BQ = bigquery.Client()

        ## Re-map the incoming dict/JSON into the BQ event_log structure
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

        if row.get("event_code") == "ing_load_success":
            credentials = _get_access_token(row.get("sandbox_name"))
            _download_batch_data_files(credentials, row.get("batch_id"))

        return(event, 200, None)
    else:
        request_json = request.get_json(silent=True)
        request_args = request.args

        ### if there is a challenge query parameter present, then echo that back in the response
        if request_args and 'challenge' in request_args:
            response = request_args.get('challenge')
        else:
            response = 'Get Successful!'
        return(response, 200, None)
        
def _get_access_token(sandbox_name):
    
    credentials_file = os.path.join(".credentials", "credentials.json")
    private_key_file = os.path.join(".credentials", "private.key")
    with open(credentials_file) as f:
        creds = json.load(f)

    required_credential_keys = ('ORG_ID','TECHNICAL_ACCOUNT_ID', 'CLIENT_SECRET', 'API_KEY')
    
    ##Validate the supplied dictionary has all the required values for authentication 
    missing_keys = []
    for key in required_credential_keys:
        if key not in creds.keys():
            missing_keys.append(key)

    if len(missing_keys) != 0:
        raise ValueError(f"ERROR: The values {missing_keys} are missing from {credentials_file}")

    ##This is generally static and not provided in the JSON file export from adobe.io, so setting it here
    creds['IMS'] = "ims-na1.adobelogin.com"

    ##== Get Access Token
    ### Use creds to get access key
    url = f"https://{creds['IMS']}/ims/exchange/jwt/"
    jwtPayload = { 
        "iss": creds['ORG_ID'],
        "sub": creds['TECHNICAL_ACCOUNT_ID'],
        "aud": f"https://{creds['IMS']}/c/{creds['API_KEY']}",
        f"https://{creds['IMS']}/s/ent_dataservices_sdk": True,
    }

    jwtPayload["exp"] = int((datetime.datetime.utcnow() + datetime.timedelta(seconds=30)).timestamp())
    keyfile = open(private_key_file,'r') 
    private_key = keyfile.read()

    #Encode the jwt Token
    jwttoken = jwt.encode(jwtPayload, private_key, algorithm='RS256')

    accessTokenPayload={
        'client_id': creds['API_KEY'],
        'client_secret': creds['CLIENT_SECRET'],
    }
    accessTokenPayload['jwt_token'] = jwttoken

    response = requests.post(url, data=accessTokenPayload)
    result = json.loads(response.text)
    creds['ACCESS_TOKEN'] = result['access_token']
    ##Put the sandbox name in the cred dict for easier access later
    creds['SANDBOX'] = sandbox_name

    return creds

def _download_batch_data_files(creds, batch_id):
    GCS_STORAGE = "somebucket"

    url = f"https://platform.adobe.io/data/foundation/export/batches/{batch_id}/files"
 
    payload={}
    headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'Authorization': f"Bearer {creds['ACCESS_TOKEN']}",
            'x-api-key': creds['API_KEY'],
            'x-gw-ims-org-id': creds['ORG_ID'],
            'x-sandbox-name': creds['SANDBOX']
        }
    
    response = requests.request("GET", url, headers=headers, data=payload)
    if response.status_code == 200:
        files = json.loads(response.text)
        """Sample Response
        {
            "data": [
                {
                "dataSetFileId": "01G2TE5FPCNE7XYEHCZ6R19HPG-1",
                "dataSetViewId": "627c20e747c1b41949d9d8b7",
                "version": "1.0.0",
                "created": "1652302326675",
                "updated": "1652303396481",
                "isValid": false,
                "_links": {
                    "self": {
                    "href": "https://platform.adobe.io:443/data/foundation/export/files/01G2TE5FPCNE7XYEHCZ6R19HPG-1"
                    }
                }
                }
            ],
            "_page": {
                "limit": 100,
                "count": 1
            }
        }
        """
        for file in files["data"]:
            file_href = file["_links"]["self"]["href"]

            url = file_href
            headers = {
                'Accept': 'application/json',
                'Content-Type': 'application/json',
                'Authorization': f"Bearer {creds['ACCESS_TOKEN']}",
                'x-api-key': creds['API_KEY'],
                'x-gw-ims-org-id': creds['ORG_ID'],
                'x-sandbox-name': creds['SANDBOX']
            }
            
            response = requests.request("GET", url, headers=headers, data=payload) 
            """Sample Response
            {
                "data": [
                    {
                    "name": "00000-916147-e7058284-1452-412a-8561-9a3136136c5c-00001.parquet",
                    "length": "4299",
                    "_links": {
                        "self": {
                        "href": "https://platform.adobe.io:443/data/foundation/export/files/01G2TE5FPCNE7XYEHCZ6R19HPG-1?path=00000-916147-e7058284-1452-412a-8561-9a3136136c5c-00001.parquet"
                        }
                    }
                    }
                ],
                "_page": {
                    "limit": 100,
                    "count": 1
                }
            }
            """
            if response.status_code == 200:
                paths = json.loads(response.text)
            
                for path in paths["data"]:
                    file_path_href = path["_links"]["self"]["href"]

                    print(file_path_href)

                    url = file_path_href

                    payload={}
                    headers = {
                        'Accept': 'application/json, application/octet-stream',
                        'Authorization': f"Bearer {creds['ACCESS_TOKEN']}",
                        'x-api-key': creds['API_KEY'],
                        'x-gw-ims-org-id': creds['ORG_ID'],
                        'x-sandbox-name': creds['SANDBOX']
                    }
                    response = requests.request("GET", url, headers=headers, data=payload)
 
                    ##This is the actual parquet file that needs to be written to gcs
                    ##print(response.content)
