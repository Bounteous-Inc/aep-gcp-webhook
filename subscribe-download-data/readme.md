## Deploying the function to gcp

```gcloud beta functions deploy aep-pubsub-function-test \
 --gen2 \
 --runtime python39 \
 --trigger-topic aep-webook \
 --entry-point subscribe \
 --source . \
 --memory=512MB \
 --timeout=540 \
 --set-env-vars GCS_STORAGE_BUCKET=aep-webhook-poc
 ```

Note: this is using the gen2 functions which as of publishing are in beta. Once they are out of beta, the ```glcoud beta``` command line can be swapped out for just ```gcloud```.