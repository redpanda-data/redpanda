---
title: Archival
---
# Archival
## Overview

Archival subsystem can be used to upload data to S3 (or compatible) storage. This task is performed in the background without interrupting the service. At this point only upload is implemented. This data can be used to restore redpanda cluster from the ground up (this is in progress).

Archival storage work on every node. It uploads log segments that can’t receive new batches. These segments contain only batches with offsets that are less than the committed offset of the partition. Only non-compacted log segments are uploaded. Indexes are not uploaded. Only the leader of the partition can upload partition data.

To make sure that every node uploads the right set of partitions, the archival subsystem runs the reconciliation process. This process is responsible for maintaining the right set of upload jobs for every node. Every upload job handles signal partition for which the node is a leader. When the leadership changes the reconciliation process cancels all upload jobs that are no longer running on a leader and starts new upload jobs for new partitions. This process is performed periodically (by default every 10 seconds)

Archival subsystem also maintains a set of manifest files on S3 to account for all uploaded data.

## Data Stored in S3

There are three types of objects that redpanda uploads to S3 bucket.
Partition manifests
Topic manifests
Log segments
Partition and topic manifests have *.json extension and log segments have *.log extension. Log paths mimic the paths in the redpanda data folder. The path format is:

```
<8-symbol-random-prefix>/<namespace>/<topic>/<partition-id>_<revision-id>/<logfile-name>.log
```

The random prefix is needed because S3 shards data using object name prefixes. It’s allows redpanda to get good performance out of S3. The real log segment path can look like this: `/06160a28/kafka/redpanda-test/0_2/39459-1-v1.log`.

Partition and topic manifests use prefixes with all symbols set to ‘0’ except the first one. This allows us to quickly find all manifests in the bucket. Example: `/00000000/meta/kafka/redpanda-test/topic_manifest.json` or `/00000000/meta/kafka/redpanda-test/6_2/manifest.json`.

Redpanda adds tag `rp-type` to all objects in S3:
`rp-type=partition-manifest` is added to partition manifests
`rp-type=segment` is added to log segments
`rp-type=topic-manifest` is added to all topic manifests
These tags could be used to delete data from S3 selectively. Both partition manifests and log segments can be safely deleted from S3 bucket when they get old (for instance, after one year). But topic manifests should be kept indefinitely, otherwise it won’t be possible to recover the corresponding topic.

## Cloud Storage Configuration

On AWS no special configuration is needed for the S3 bucket. You can just create a bucket and optionally specify expiration rules based on tags described above. Next, you may use IAM service to create a user to access S3. You will need to grant this user permission to read and create objects. You will also need to copy access key and secret key to the redpanda configuration options `archival_storage_s3_access_key` and `archival_storage_s3_secret_key`

On Google Cloud Storage you should choose a uniform access control while creating a bucket. Bucket level permissions should be used for all objects. You should also use google managed encryption key. Next, you will need to create a service user with HMAC keys. This HMAC keys should be added to redpanda configuration. 

## Redpanda Configuration

| Config parameter name                       | Type         | Descripion                                              |
|---------------------------------------------|--------------|---------------------------------------------------------|
| archival_storage_enabled                    | boolean      | Enables archival storage feature                        |
| archival_storage_s3_access_key              | string       | S3 access key                                           |
| archival_storage_s3_secret_key              | string       | S3 secret key                                           |
| archival_storage_s3_region                  | string       | AWS region                                              |
| archival_storage_s3_bucket                  | string       | S3 bucket                                               |
| archival_storage_reconciliation_interval_ms | milliseconds | Reconciliation period (default - 10s)                   |
| archival_storage_max_connections            | integer      | Number of simultaneous uploads per shard (default - 20) |

Note that the bucket should be created in advance. Also, please be aware that the secret and access keys are stored in plain text in configuration files. Parameters `reconciliation_interval_ms` and `max_connections` can be omitted since both have reasonable defaults.
