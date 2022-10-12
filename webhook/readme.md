## GCP Pre-requisites 

1) Create PubSub Topic
```
gcloud pubsub topics create aep-webook
```

[Optional] Create BQ Table
```
bq mk \
  --table \
  webhook.event_log \
   schema.json
```
If you don't want to use the BQ table, make sure and comment out the BQ insert from main.py

## Deploy function to gcp
```
gcloud beta functions deploy aep-webhook-test \
--gen2 \
--runtime python39 \
--trigger-http \
--entry-point webhook \
--allow-unauthenticated \
--source . \
--set-env-vars BQ_DATASET=webhook,BQ_TABLE=event_log,PUBSUB_TOPIC=aep-webook
```

Note: this is using the gen2 functions which as of publishing are in beta. Once they are out of beta, the ```glcoud beta``` command line can be swapped out for just ```gcloud```.