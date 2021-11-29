---
title: Data Management
order: 1
---

# Data Management 

One of the key aspects of working with Kafka is how you manage your data.

Creating and maintaining backup scripts is always a pain in the neck and although it can improve reliability it doesn't bring value to the customer directly.

This is the type of work that we would like to avoid and Redpanda can help you with that.

Introducing **Shadow Indexing**.

## Shadow Indexing

**Shadow Indexing** is a unique capability in Redpanda that allows you to archive your data to object storage. It pushes your log segments based on your configurations and creates indexes from it, for all your historic data. 

This capability unlocks new features such as replicating topics from one cluster to another, disaster recovery, etc. without burdening your clusters with local storage and compute.

It uses the basic subset of the S3 api (Put and Get requests) so it's compatible with most cloud providers. We officially support Amazon's S3 and Google Cloud Storage. 

You just have to configure your cloud credentials and we'll do the hard work for you. 

With **Shadow Indexing**, Redpanda facilitates your disaster recovery plan by integrating with these infinitely scalable storage systems. It will push the your segments as fast as it can onto the cloud and fetch them when clients need it, with almost no risk to data loss.  

On the surface, this sounds like the obvious solution in a cloud native world, but what it does for enterprises is that it unifies the way you access and manage both historical and real-time data without changing a single line of code in your application!

We call this technology Shadow Indexing because it doesn’t keep the historical data on local disks, instead it keeps a location reference of how and where to fetch the data should a Kafka-API client request it. 

What enterprises are the most excited about, is the fact that you can spin up a new Redpanda cluster and replay petabytes of data to retrain your machine learning model without affecting your production cluster or changing any code. 

In a way, this presents a true disaggregation of compute and store for streaming data.

## How it works

When you're setting up your Redpanda broker, enable your cloud credentials and flags at `redpanda.yaml`. More on that down below. 

Then when you create a topic, set the flags for remote read/write.

If everything is correct, as soon as possible your data will be pushed onto the object store of your choosing.

We use Kafka's object `LastStableOffset(LSO)` to determine which data should be pushed into the cloud. Therefore, as soon as possible, the data is pushed into the object storage and subsequently evicted from the local machine.

Your data is always uploaded to the remote storage first and only after that it can be deleted based on your retention policy. 

Deletion can be done based on time (remove everything older than 2w, for example) or space (keep, say, 20GB of data per partition). 

So when the deletion (space or time based) is enabled, Redpanda will guarantee that the log segments are deleted only if they're already uploaded successfully. 

So there is a guarantee that the data is always available locally or remotely.

Please note that it’s not possible to provide outside data directly from the object store to Redpanda. You have to process your data through Redpanda for it to be accessible. After this, you can enjoy all the features listed here. 

## How to get started

It is very simple to activate Shadow Indexing in Redpanda. 

You have to enable `x-redpanda-remote-read` and `x-redpanda-remote-write` flags for remote read/write.

Here's a short command to enable it when you're creating a topic:

```bash
rpk topic create <topic-name> -c x-redpanda-remote-read=true -c x-redpanda-remote-write=true
```

If your topic already exists, you can set them using this command:

```bash
rpk topic alter-config <topic-name> --set x-redpanda-remote-read=true --set x-redpanda-remote-write=true
```

Then configure these flags in `redpanda.yaml` and restart your service.

<tabs>
<tab id="AWS S3" >

```bash
cloud_storage_enabled: true
cloud_storage_access_key: |your-access-key|
cloud_storage_secret_key: |your-secret-key|
cloud_storage_region: |your-region|
cloud_storage_bucket: |your-bucket|
cloud_storage_segment_max_upload_interval_sec: 300
```

</tab>

<tab id="Google Cloud Storage">

```bash
cloud_storage_enabled: true
cloud_storage_access_key: |your-access-key|
cloud_storage_secret_key: |your-secret-key|
cloud_storage_api_endpoint: storage.googleapis.com
cloud_storage_region: |your-region|
cloud_storage_bucket: |your-bucket|
```
</tab>
</tabs>

> **_Note_** - Remember to change these values to adjust to your preferences. These are the minimum configuration flags for the process to work. You can check further down below a list with all the available flags.

## Other features

### Topic Archival

This is the process that constantly uploads log segments to the cloud.

It's created per-partition and always running on a leader node of the partition.

It uploads only those segments that contain batches with offsets which are smaller than `LastStableOffset(LSO)`.

The archival process is enabled per topic using the `x-redpanda-remote-write` flag. If this flag is not set, the partition won't be uploaded.

To enable it when you're creating a topic you can run:

```bash
rpk topic create |topic-name| -c x-redpanda-archival=true
```

If your topic is already created, run:

```bash
rpk topic alter-config |topic-name| --set x-redpanda-archival=true
```

### Topic recovery

Topic recovery can be invoked during topic creation. 

If the newly created topic has `x-redpanda-recovery` flag set Redpanda will download its data from the cloud upon creation. 

Only the data that match the retention policy of the topic will be downloaded. 

The data which is not downloaded from the cloud during topic creation will be accessible through Shadow Indexing.

Topic recovery can restore a topic which was created in the cluster and deleted. Also, the topic created in another cluster can also be restored using this mechanism.

You can do this operation with this command:

```bash
rpk topic create |topic-name| -c x-redpanda-recovery=true
```

### Idle timeout  

It's possible to configure idle timeout for the download. 

The configuration parameter is called `cloud_storage_segment_max_upload_interval_sec`. 

If it's set to some value, the upload will be started if the configured number of seconds passed since the previous upload (and if the partition has any new data). 

This is needed in a situation where the ingestion rate is low and the segments are kept open for long periods of time. 

To limit possible data loss in case of catastrophic failure we can set this parameter and let Redpanda to upload data every X seconds. 

This adds guarantee that we can lose only X seconds of data and not more.

### Reconciliation

The Shadow Indexing subsystem has a reconciliation loop which runs constantly and checks which partition replicas are leaders on the node. 

If a new leader gets elected on a node the upload process is started for that partition on a node. 

If an existing leader loses leadership the reconciliation loop stops the upload process for the node.

### Retries and backoff

The cloud provider can reply with an error message that indicates that the server is busy and in this case Redpanda will retry the request. 

Redpanda always uses exponential backoff with cloud connections. 

Some errors are never retried (e.g. `NoSuchKey` error). You can check what errors are retried in the table down below. 

### Http and TLS implementation

Shadow Indexing subsystem uses connection pool creed per CPU. 

It also uses persistent HTTP connections with configurable max idle time. 

Custom HTTP clients are used for that. You can check our [guide about Authorization & Authentication](https://vectorized.io/docs/acls) to learn more about how Redpanda handles security configs.

### Upload controller

The Shadow Indexing subsystem uses a PD controller (Proportional Derivative). 

The goal of the controller is to provide 0 upload backlog size. 

Upload backlog is all data which is not yet uploaded but has to be uploaded eventually. 

If the upload backlog grows unbounded Redpanda will eventually run out of space. This is because Redpanda can't evict log segments which are not uploaded to the cloud (only if archival upload is enabled for the topic). 

So if archival upload process can't keep up Redpanda needs to increase priority for the archival subsystem. By default archival runs with low priority.

This is achieved using the upload controller. Upload controller measures the size of the upload backlog periodically and tunes the priority of the archival process.

### Caching

When the Kafka client fetches data from some segment stored in the cloud, Redpanda first downloads the segment to the cache directory. 

This directory is separate from the Redpanda data directory. 

By default it's created inside the Redpanda data directory, but it can also be placed anywhere in the system. 

For instance, it can be placed to the dedicated volume with cheaper storage.

The cache is checked periodically and if the size of the stored data is larger than the configured limit the eviction process starts. The eviction process removes segments which weren't accessed recently until the size of the cache will drop down 20%.

## Flag table

<table>
<tbody>
<tr>
<td>
cloud_storage_enabled
</td>
<td>
Global flag that enables archival and Shadow Indexing.
</td>
</tr>
<tr>
<td>
cloud_storage_access_key
</td>
<td>
Cloud access key.
</td>
</tr>
<tr>
<td>
cloud_storage_secret_key
</td>
<td>
Cloud secret key.
</td>
</tr>
<tr>
<td>
cloud_storage_region
</td>
<td>
AWS region.
</td>
</tr>
<tr>
<td>
cloud_storage_bucket
</td>
<td>
S3 bucket name.
</td>
</tr>
<tr>
<td>
cloud_storage_api_endpoint
</td>
<td>
API endpoint, can be left blank for AWS (it's generated automatically using region and bucket), for GCS storage.googleapis.com should be used.
</td>
</tr>
<tr>
<td>
cloud_storage_reconciliation_interval_ms
</td>
<td>
Interval used to reconcile partitions that need to be uploaded.
</td>
</tr>
<tr>
<td>
cloud_storage_max_connections
</td>
<td>
Max number of connections to the cloud provider on a node per CPU.
</td>
</tr>
<tr>
<td>
cloud_storage_disable_tls
</td>
<td>
Forcefully disable TLS encryption. Use this if TLS is done by the proxy or for testing purposes. Not recommended for production environments.&nbsp; Disable TLS encryption (can be set if TLS termination is done by the proxy).
</td>
</tr>
<tr>
<td>
cloud_storage_api_endpoint_port
</td>
<td>
Override default port for tls connection (443).
</td>
</tr>
<tr>
<td>
cloud_storage_trust_file
</td>
<td>
Public certificate used to validate TLS connection to S3 (can be left empty, Redpanda will use Public Key Infrastructure.
in this case).
</td>
</tr>
<tr>
<td>
cloud_storage_initial_backoff_ms
</td>
<td>
Time interval used for exponential backoff algorithm if it faces some cloud related errors.<br />Possible errors: <br />- connection refused
- connection reset by peer
- connection timed out
- 'SlowDown' REST API error<br /><br />
</td>
</tr>
<tr>
<td>
cloud_storage_segment_upload_timeout_ms
</td>
<td>
Segment upload timeout.
</td>
</tr>
<tr>
<td>
cloud_storage_manifest_upload_timeout_ms
</td>
<td>
Manifest upload timeout.
</td>
</tr>
<tr>
<td>
cloud_storage_max_connection_idle_time_ms
</td>
<td>
Max idle time for persistent HTTP connections. AWS S3 by default uses a 5s interval. GCS uses a longer than a minute interval.much larger one.
</td>
</tr>
<tr>
<td>
cloud_storage_segment_max_upload_interval_sec
</td>
<td>
Max idle time between uploads for the same partition.
</td>
</tr>
<tr>
<td>
cloud_storage_upload_ctrl_update_interval_ms
</td>
<td>
Recompute interval for upload controller.
</td>
</tr>
<tr>
<td>
cloud_storage_upload_ctrl_update_interval_ms
</td>
<td>
Proportional coefficient for upload controller.
</td>
</tr>
<tr>
<td>
cloud_storage_upload_ctrl_update_interval_ms
</td>
<td>
Derivative coefficient for upload controller.
</td>
</tr>
<tr>
<td>
cloud_storage_ctrl_min_shares
</td>
<td>
Min number of I/O and CPU shares that archival can use.
</td>
</tr>
<tr>
<td>
cloud_storage_upload_ctrl_max_shares
</td>
<td>
Max number of I/O and CPU shares that archival can use.
</td>
</tr>
<tr>
<td>
cloud_storage_cache_check_interval
</td>
<td>
Check interval. Cache is checked every X ms, between the checks the size of the cache can grow uncontrollably, so it makes sense to have a small interval, on the other hand having it too small will consume a lot of resources.
</td>
</tr>
<tr>
<td>
cloud_storage_cache_directory
</td>
<td>
Directory for the cache for Shadow Indexing
</td>
</tr>
</tbody>
</table>
