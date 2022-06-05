from google.cloud import storage 

bucket_name = "aep-webhook-poc"

storage_client = storage.Client()

my_bucket = storage_client.get_bucket(bucket_name)

print(vars(my_bucket))