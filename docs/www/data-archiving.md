---
title: Data archiving
order: 0
---

# Data archiving

When producers send events to Redpanda, Redpanda stores the events on a local storage volume.
Then, Redpanda replicates the data to other nodes,
according to the requirements defined by the producer.
These persistence and replication processes protect your data in the event of a node or cluster failure,
but archiving data to cloud storage gives your data additional durability.

After you configure data archiving, your data is uploaded to cloud storage.
In the event of a data center failure, data corruption, or cluster migration,
your data is safe in the cloud for you to recover to your cluster.

> **_Note:_** Archiving to Amazon S3 and Google Cloud Storage is supported.

## How it works

The data archiving process runs on each node.
Every 10 seconds (by default), the archiving process identifies log segments that are ready for archiving.
Log segments are uploaded when they are closed for new batches
because they contain only batches with offsets that are less than the committed offset of the partition.

Every upload job handles a signal partition for which the node is a leader.
When the leadership for the patition changes,
the reconciliation process cancels all upload jobs that are no longer running on a leader
and starts new upload jobs for new partitions.

The objects that Redpanda uploads to S3 bucket are:

- Partition manifests
- Topic manifests
- Log segments

Partition and topic manifests use *.json extension, and log segments use *.log extension.

- Log paths match the paths in the Redpanda data folder in the format
    `<random-prefix>/<namespace>/<topic>/<partition-id>_<revision-id>/<logfile-name>.log`.
    For example, a log segment path looks like `/06160a28/kafka/redpanda-test/0_2/39459-1-v1.log`.
    The random characters improve performance with S3 data transfers. 

- Partition and topic manifests use prefixes with all symbols set to ‘0’ except the first one.
    For example, a manifest path looks like `/00000000/meta/kafka/redpanda-test/topic_manifest.json` or `/00000000/meta/kafka/redpanda-test/6_2/manifest.json`.

Redpanda adds the `rp-type` tag to all objects in S3:

- Partition manifests - `rp-type=partition-manifest`
- Log segments - `rp-type=segment`
- Topic manifests - `rp-type=topic-manifest`

These tags can be used to selectively delete data from S3.
Both partition manifests and log segments can be safely deleted from S3 bucket when they get old (for instance, after one year).
We recommend that you keep topic manifests in order to recover the corresponding topic.

## Configuring data archiving

1. Create the cloud storage bucket and prepare it according to these requirements:

    - Amazon AWS S3
        - (Optional) Specify expiration rules for the files that are based on the `rp-type` file tags.
        - Use the IAM service to create a user to access S3.
            - Grant this user permission to read and create objects.
            - Copy the access key and secret key to the Redpanda configuration options `cloud_storage_access_key` and `cloud_storage_secret_key`, respectively.

    - Google Cloud Storage
        - Choose a uniform access control when you create the bucket.
            - Use bucket level permissions for all objects.
            - Use a Google managed encryption key.
            - Set a [default project](https://cloud.google.com/storage/docs/migrating#defaultproj).
        - [Create a service user with HMAC keys](https://cloud.google.com/storage/docs/authentication/managing-hmackeys)
            and copy the keys to the Redpanda configuration options `cloud_storage_access_key` and `cloud_storage_secret_key`, respectively. 
        - Set `cloud_storage_api_endpoint` to `storage.googleapis.com`
    - MinIO (for local testing)
        - Use the `MINIO_ROOT_PASSWORD` and `MINIO_ROOT_USER` environment variables for the access key (`cloud_storage_access_key`) and secret key (`cloud_storage_secret_key`), respectively.
        - Use the `MINIO_REGION_NAME` environment variable for region name (`cloud_storage_region`).
        - Set the `MINIO_DOMAIN` environment variable. Redpanda uses virtual-hosted style endpoints but MinIO only supports them if the custom domain name is provided.
        - Set the custom API endpoint (`cloud_storage_api_endpoint`) and port (`cloud_storage_api_endpoint_port`) and either:
          - Require TLS - Set `cloud_storage_disable_tls` to `disabled` to disable TLS.

    > **_Note:_** The secret and access keys are stored in plain text in configuration files.

2. Configure these parameters in the Redpanda configuration:

    | Parameter name                                | Type         | Descripion                                              |
    |-----------------------------------------------|--------------|---------------------------------------------------------|
    | `cloud_storage_enabled`                       | boolean      | Enables archival storage feature                        |
    | `cloud_storage_access_key`                    | string       | Access key                                           |
    | `cloud_storage_secret_key`                    | string       | Secret key                                           |
    | `cloud_storage_region`                        | string       | Cloud region                                              |
    | `cloud_storage_bucket`                        | string       | Bucket name                                              |
    | `cloud_storage_api_endpoint`                  | string       | Cloud storage api endpoint (Default: S3)     |
    | `cloud_storage_api_endpoint_port`             | string       | Cloud storage api endpoint port number (Default: 443)    |
    | `cloud_storage_trust_file`                    | string       | Alternative location of the CA certificate (Default: /etc/pki/tls/cert.pem) |
    | `cloud_storage_disable_tls`                   | boolean      | Disable TLS for cloud storage connections               |

3. (Optional) You can tune the data transfer with these parameters:

    | Parameter name                                | Type         | Descripion                                              |
    |-----------------------------------------------|--------------|---------------------------------------------------------|
    | `cloud_storage_reconciliation_interval_ms`    | milliseconds | Reconciliation period (Default: 10s)                   |
    | `cloud_storage_max_connections`               | integer      | Number of simultaneous uploads per shard (Default: 20) |
