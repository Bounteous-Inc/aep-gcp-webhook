"""get_access_token generates a signed JWT token from the adobe endpoint using an RSA private key"""

import json
import requests
import datetime
import jwt #pyjwt

def get_access_token(credentials_file):
    
    with open(credentials_file) as f:
        creditional_dict = json.load(f)
    
    required_credential_keys = ('ORG_ID','TECHNICAL_ACCOUNT_ID', 'CLIENT_SECRET', 'API_KEY', 'PRIVATE_KEY_FILE_LOCATION')

    ##Validate the supplied dictionary has all the required values for authentication 
    missing_keys = []
    for key in required_credential_keys:
        if key not in creditional_dict.keys():
            missing_keys.append(key)

    if len(missing_keys) != 0:
        raise ValueError(f"ERROR: The values {missing_keys} are missing from {credentials_file}")

    ##This is generally static and not provided in the JSON file export from adobe.io, so setting it here
    creditional_dict['IMS'] = "ims-na1.adobelogin.com"
    
    ##== Get Access Token
    ### Use creds to get access key
    url = f"https://{creditional_dict['IMS']}/ims/exchange/jwt/"
    jwtPayload = { 
        "iss": creditional_dict['ORG_ID'],
        "sub": creditional_dict['TECHNICAL_ACCOUNT_ID'],
        "aud": f"https://{creditional_dict['IMS']}/c/{creditional_dict['API_KEY']}",
        f"https://{creditional_dict['IMS']}/s/ent_dataservices_sdk": True,
    }

    jwtPayload["exp"] = int((datetime.datetime.utcnow() + datetime.timedelta(seconds=30)).timestamp())
    keyfile = open(creditional_dict['PRIVATE_KEY_FILE_LOCATION'],'r') 
    private_key = keyfile.read()

    #Encode the jwt Token
    jwttoken = jwt.encode(jwtPayload, private_key, algorithm='RS256')

    accessTokenPayload={
        'client_id': creditional_dict['API_KEY'],
        'client_secret': creditional_dict['CLIENT_SECRET'],
    }
    accessTokenPayload['jwt_token'] = jwttoken

    response = requests.post(url, data=accessTokenPayload)
    result = json.loads(response.text)
    creditional_dict['ACCESS_TOKEN'] = result['access_token']

    return creditional_dict