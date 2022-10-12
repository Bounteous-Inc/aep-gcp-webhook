# AEP to GCP Pipeline Proof of Concept

This was created for a blog post to illustrate how to use Adobe Experience Platform (AEP) Webhook combined with the PLatform Data Access API to replicate data out of the AEP data lake to Google Cloud (GCP).

Blog Post: [Configuring Automated Data Export in Adobe Experience Platform ](https://www.bounteous.com/insights/2022/10/17/configuring-automated-data-export-adobe-experience-platform)

## Organization and Deployment

1) <strong>webhook</strong> - this is a simple webhook intended to run on a Google Cloud function. It's only job is to receive the webhook events, log them in BigQuery, then push to a Pub/Sub topic for processing
2) <strong>subscribe-download-data</strong> - this is also intened to run on a Google Cloud function, it is a subscriber to the Pub/Sub topic and performs the extract from the AEP datalake into a GCS bucket.

Each directory has its own readme files with details on those functions and gcp cli to deploy the infrastrucure

## Notes

This is not intended to be a complete production ready solution. It does not follow all the best practices for securing keys or data storage, but is designed to show what is possible.

## License
Licensed under the MIT License. For a complete copy of the license, please refer to the LICENSE.md file included with this repository.
