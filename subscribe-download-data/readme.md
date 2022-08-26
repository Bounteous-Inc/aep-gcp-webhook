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