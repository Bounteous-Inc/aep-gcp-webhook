Pre-reqs

Create PubSub Topic
```
gcloud pubsub topics create aep-webook
```

Create BQ Table
```
bq mk \
  --table \
  webhook.event_log \
   schema.json
```

Deploy Function

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