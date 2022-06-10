import functions_framework
from google.cloud import storage
from google.cloud import pubsub_v1
import json
import jwt #pyjwt
import requests
import os
import datetime
import urllib 
import base64

# Triggered from a message on a Cloud Pub/Sub topic.
@functions_framework.cloud_event
def subscribe(cloud_event):
    # Print out the data from Pub/Sub, to prove that it worked
    data = base64.b64decode(cloud_event.data["message"]["data"])
    print(data)

    event = json.loads(data)

    sandbox = event.get('sandbox')
    batch_id = event.get('batch_id')

    creds = _get_access_token(sandbox)
    print(creds)
    _download_batch_data_files(creds, batch_id)

def _get_access_token(sandbox_name):
    
    credentials_file = os.path.join(".credentials", "config", "credentials.json")
    private_key_file = os.path.join(".credentials", "config", "private.key")
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
    if response.status_code == 200:
        creds['ACCESS_TOKEN'] = result['access_token']
        ##Put the sandbox name in the cred dict for easier access later
        creds['SANDBOX'] = sandbox_name
    else:
        raise ValueError(f"unable to retrieve access token: {response.text}")

    return creds

def _download_batch_data_files(creds, batch_id):
    GCS_STORAGE_BUCKET = "aep-webhook-poc"
    local_testing = True

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

                    # parse the filename from the href, the filename is in the path qs
                    filename = urllib.parse.parse_qs(urllib.parse.urlparse(file_path_href).query)['path'][0]

                    payload={}
                    headers = {
                        'Accept': 'application/json, application/octet-stream',
                        'Authorization': f"Bearer {creds['ACCESS_TOKEN']}",
                        'x-api-key': creds['API_KEY'],
                        'x-gw-ims-org-id': creds['ORG_ID'],
                        'x-sandbox-name': creds['SANDBOX']
                    }
                    response = requests.request("GET", url, headers=headers, data=payload)
 
                    print(f"retrieving file status code: {response.status_code}")

                    if os.environ.get('GCP_PROJECT'):
                        base_path = "/tmp"
                    else:
                        base_path = r"D:\tmp"

                    file = os.path.join(base_path,filename)
                    with open(file, "wb") as f:
                        f.write(response.content)

                    ##Copy file written to local storage to gcs
                    # Cloud Storage Client
                    storage_client = storage.Client()
                    bucket_name = GCS_STORAGE_BUCKET
                    bucket = storage_client.get_bucket(bucket_name)
                    blob = bucket.blob(filename)
                    blob.upload_from_filename(file)
