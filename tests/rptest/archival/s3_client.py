import threading
import logging

from rptest.archival.shared_client_utils import key_to_topic

import boto3

from botocore import UNSIGNED
from botocore.config import Config
from botocore.exceptions import ClientError
from google.cloud import storage as gcs

from concurrent.futures import ThreadPoolExecutor
import datetime
from enum import Enum
from functools import wraps
from itertools import islice
from time import sleep
from typing import Iterator, NamedTuple, Union, Optional
from ducktape.utils.util import wait_until


class SlowDown(Exception):
    pass


class ObjectMetadata(NamedTuple):
    key: str
    bucket: str
    etag: str
    content_length: int


class S3AddressingStyle(str, Enum):
    VIRTUAL = 'virtual'
    PATH = 'path'


def retry_on_slowdown(tries=4, delay=1.0, backoff=2.0):
    """Retry with an exponential backoff if SlowDown exception was triggered."""
    def retry(fn):
        @wraps(fn)
        def do_retry(*args, **kwargs):
            ntries, sec_delay = tries, delay
            while ntries > 1:
                try:
                    return fn(*args, **kwargs)
                except SlowDown:
                    sleep(sec_delay)
                    ntries -= 1
                    sec_delay *= backoff
            # will stop filtering SlowDown exception when quota is reache
            return fn(*args, **kwargs)

        return do_retry

    return retry


class S3Client:
    """Simple S3 client"""
    def __init__(self,
                 region,
                 access_key,
                 secret_key,
                 logger,
                 endpoint=None,
                 disable_ssl=True,
                 signature_version='s3v4',
                 before_call_headers=None,
                 use_fips_endpoint=False,
                 addressing_style: S3AddressingStyle = S3AddressingStyle.PATH):

        logger.debug(
            f"Constructed S3Client in region {region}, endpoint {endpoint}, key is set = {access_key is not None}, fips mode {use_fips_endpoint}, addressing style {addressing_style}"
        )

        self._use_fips_endpoint = use_fips_endpoint
        self._region = region
        self._access_key = access_key
        self._secret_key = secret_key
        self._endpoint = endpoint
        self._disable_ssl = False if self._use_fips_endpoint else disable_ssl
        if signature_version.lower() == "unsigned":
            self._signature_version = UNSIGNED
        else:
            self._signature_version = signature_version
        self._before_call_headers = before_call_headers
        self.logger = logger
        self.update_boto3_loggers()
        self._addressing_style = addressing_style
        self._cli = self.make_client()
        self.register_custom_events()

    def update_boto3_loggers(self):
        """Configure loggers related to boto3 to emit messages
           with FileHandlers similar to ones from ducktape
           using same filenames for corresponding log levels
           
           loggers updated: boto3, botocore
           
           loggers list that can be included can be found in ticket: PESDLC-876         
           
        """
        def populate_handler(filename, level):
            # If something really need debugging, add 'urllib3'
            loggers_list = ['boto3', 'botocore']
            # get logger, configure it and set handlers
            for logger_name in loggers_list:
                l = logging.getLogger(logger_name)
                l.setLevel(level)
                handler = logging.FileHandler(filename)
                fmt = logging.Formatter('[%(levelname)-5s - %(asctime)s - '
                                        f'{logger_name} - %(module)s - '
                                        '%(funcName)s - lineno:%(lineno)s]: '
                                        '%(message)s')
                handler.setFormatter(fmt)
                l.addHandler(handler)

        # Extract info from ducktape loggers
        # Assume that there is only one DEBUG and one INFO handler
        #
        for h in self.logger.handlers:
            if isinstance(h, logging.FileHandler):
                if h.level == logging.INFO or h.level == logging.DEBUG:
                    populate_handler(h.baseFilename, h.level)

    def make_client(self):
        cfg = Config(
            region_name=self._region,
            signature_version=self._signature_version,
            retries={
                'max_attempts': 10,
                'mode': 'adaptive'
            },
            s3={'addressing_style': f'{self._addressing_style}'},
            use_fips_endpoint=True if self._use_fips_endpoint else None)
        cl = boto3.client('s3',
                          config=cfg,
                          aws_access_key_id=self._access_key,
                          aws_secret_access_key=self._secret_key,
                          endpoint_url=self._endpoint,
                          use_ssl=not self._disable_ssl)
        if self._before_call_headers is not None:
            event_system = cl.meta.events
            event_system.register('before-call.s3.*', self._add_header)
        return cl

    def register_custom_events(self):
        # src: https://stackoverflow.com/questions/58828800/adding-custom-headers-to-all-boto3-requests
        def process_custom_arguments(params, context, **kwargs):
            if (custom_headers := params.pop("custom_headers", None)):
                context["custom_headers"] = custom_headers

        # Here we extract the headers from the request context and actually set them
        def add_custom_headers(params, context, **kwargs):
            if (custom_headers := context.get("custom_headers")):
                params["headers"].update(custom_headers)

        event_system = self._cli.meta.events
        # Right now, there is an issue when running inside GCP
        # when bucket is actually an S3-like bucket inside Google Clooud
        # and boto3 adds own header instead of "x-goog-copy-source"

        # Example, boto3 default:
        # "x-amz-copy-source": "panda-bucket-bee49752-c10c-11ee-9d4e-ff8d5ff47a80/002fef90/kafka/test/0_24/194-257-1055502-1-v1.log.1",

        # This adds custom header kwargs handling and it can be used like this
        #         custom_headers = {"x-goog-copy-source": src_uri}
        #         return self._cli.copy_object(Bucket=bucket,
        #                                      Key=dst,
        #                                      CopySource=src_uri,
        #                                      custom_headers=custom_headers)
        # Similar event callbacks can be added to other functions when needed
        # Wildcards supported.
        event_system.register('before-parameter-build.s3.CopyObject',
                              process_custom_arguments)
        event_system.register('before-call.s3.CopyObject', add_custom_headers)

        return

    def create_bucket(self, name):
        """Create bucket in S3"""
        try:
            res = self._cli.create_bucket(
                Bucket=name,
                CreateBucketConfiguration={'LocationConstraint': self._region})

            self.logger.debug(res)
            assert res['ResponseMetadata']['HTTPStatusCode'] == 200
        except ClientError as err:
            if err.response['Error']['Code'] != 'BucketAlreadyOwnedByYou':
                raise err

        def bucket_is_listable():
            try:
                self._cli.list_objects_v2(Bucket=name)
            except Exception as e:
                self.logger.warning(
                    f"Listing {name} failed after creation: {e}")

                return False
            else:
                self.logger.info(
                    f"Listing bucket {name} succeeded after creation")
                return True

        # Wait until ListObjectsv2 requests start working on the newly created
        # bucket.  It appears that AWS S3 has an undocumented behavior where
        # certain verbs (including ListObjectsv2) may return NoSuchBucket
        # for some time after creating the bucket, even though the creation
        # returned 200 and other methods work on the bucket.
        # Related: https://github.com/redpanda-data/redpanda/issues/8490
        wait_until(
            bucket_is_listable,
            timeout_sec=300,
            backoff_sec=5,
            err_msg=
            f"Bucket {name} didn't become visible to ListObjectsvv2 requests")

    def empty_and_delete_bucket(self, name, parallel=False):
        failed_deletions = self.empty_bucket(name, parallel=parallel)

        assert len(failed_deletions) == 0
        self.delete_bucket(name)

    def delete_bucket(self, name):
        self.logger.info(f"Deleting bucket {name}...")
        try:
            self._cli.delete_bucket(Bucket=name)
        except Exception as e:
            self.logger.error(f"Error deleting bucket {name}: {e}")
            self.logger.warn(f"Contents of bucket {name}:")
            for o in self.list_objects(name):
                self.logger.warn(f"  {o.key}")
            raise

    def empty_bucket(self, name, parallel=False):
        """Empty bucket, return list of keys that wasn't deleted due
        to error"""

        # If we are asked to run in parallel, create 256 tasks to share
        # out the keyspace, then run using a fixed number of workers.  Worker
        # count has to be modest to avoid hitting a lot of AWS SlowDown responses.
        max_workers = 4 if parallel else 1
        hash_prefixes = list(f"{i:02x}" for i in range(0, 256))
        prefixes = hash_prefixes + ["cluster_metadata"] if parallel else [""]

        def empty_bucket_prefix(prefix):
            self.logger.debug(
                f"empty_bucket: running on {name} prefix={prefix}")
            failed_keys = []

            # boto3 client objects are not thread safe: create one for each worker thread
            local = threading.local()
            if not hasattr(local, "client"):
                local.client = self.make_client()

            def deletion_batches():
                it = self.list_objects(bucket=name, prefix=prefix)
                while True:
                    batch = list(islice(it, 1000))
                    if not batch:
                        return
                    yield batch

            deleted_count = 0
            try:
                for obj_batch in deletion_batches():
                    # Materialize a list so that we can re-use it in logging after using
                    # it in the deletion op
                    key_list = list(o.key for o in obj_batch)

                    try:
                        # GCS does not support bulk delete operation through S3 complaint clients
                        # https://cloud.google.com/storage/docs/migrating#methods-comparison
                        if self._is_gcs:
                            for k in key_list:
                                local.client.delete_object(Bucket=name, Key=k)
                        else:
                            local.client.delete_objects(Bucket=name,
                                                        Delete={
                                                            'Objects':
                                                            [{
                                                                'Key': k
                                                            }
                                                             for k in key_list]
                                                        })
                    except:
                        self.logger.exception(
                            f"empty_bucket: delete request failed for keys {key_list[0]}..{key_list[-1]}"
                        )
                        failed_keys.extend(key_list)
                    else:
                        deleted_count += len(key_list)
            except Exception as e:
                # Expected to fail if bucket doesn't exist
                self.logger.debug(f"empty_bucket error on {name}: {e}")

            self.logger.debug(
                f"empty_bucket: deleted {deleted_count} keys (prefix={prefix})"
            )

            return deleted_count, failed_keys

        all_failed_keys = []
        all_deleted_count = 0
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            results = executor.map(empty_bucket_prefix, prefixes)
            for (dc, fk) in results:
                all_deleted_count += dc
                all_failed_keys.extend(fk)

        # In parallel mode we delete using hash prefixes at it doesn't cover
        # cluster manifest.
        if len(prefixes) > 1:
            dc, fk = empty_bucket_prefix("")
            all_deleted_count += dc
            all_failed_keys.extend(fk)

        self.logger.debug(
            f"empty_bucket: deleted {all_deleted_count} keys (all prefixes)")

        return all_failed_keys

    def delete_object(self, bucket, key, verify=False):
        """Remove object from S3"""
        res = self._delete_object(bucket, key)
        if verify:
            self._wait_no_key(bucket, key)
        return res

    def _wait_no_key(self, bucket, key, timeout_sec=10):
        """Wait for the key to apper in the bucket"""
        deadline = datetime.datetime.now() + datetime.timedelta(
            seconds=timeout_sec)
        try:
            # Busy wait until the object is actually
            # deleted
            while True:
                self._head_object(bucket, key)
                # aws boto3 uses 5s interval for polling
                # using head_object API call
                now = datetime.datetime.now()
                if now > deadline:
                    raise TimeoutError()
                sleep(5)
        except ClientError as err:
            self.logger.debug(f"error response while polling {bucket}: {err}")

    def _wait_key(self, bucket, key, timeout_sec=30):
        """Wait for the key to apper in the bucket"""
        # Busy wait until the object is available
        deadline = datetime.datetime.now() + datetime.timedelta(
            seconds=timeout_sec)
        while True:
            try:
                meta = self._head_object(bucket, key)
                self.logger.debug(f"object {key} is available, head: {meta}")
                return
            except ClientError as err:
                self.logger.debug(
                    f"error response while polling {bucket}: {err}")
            now = datetime.datetime.now()
            if now > deadline:
                raise TimeoutError()
            # aws boto3 uses 5s interval for polling
            # using head_object API call
            sleep(5)

    @retry_on_slowdown()
    def _delete_object(self, bucket, key):
        """Remove object from S3"""
        try:
            return self._cli.delete_object(Bucket=bucket, Key=key)
        except ClientError as err:
            self.logger.debug(f"error response {err}")
            if err.response['Error']['Code'] == 'SlowDown':
                raise SlowDown()
            else:
                raise

    @retry_on_slowdown()
    def _get_object(self, bucket, key):
        """Get object from S3"""
        try:
            return self._cli.get_object(Bucket=bucket, Key=key)
        except ClientError as err:
            self.logger.debug(f"error response getting {bucket}/{key}: {err}")
            if err.response['Error']['Code'] == 'SlowDown':
                raise SlowDown()
            else:
                raise

    @retry_on_slowdown()
    def _head_object(self, bucket, key):
        """Get object from S3"""
        try:
            return self._cli.head_object(Bucket=bucket, Key=key)
        except ClientError as err:
            self.logger.debug(f"error response heading {bucket}/{key}: {err}")
            if err.response['Error']['Code'] == 'SlowDown':
                raise SlowDown()
            else:
                raise

    @retry_on_slowdown()
    def _put_object(self, bucket, key, content, is_bytes=False):
        """Put object to S3"""
        try:
            if not is_bytes:
                payload = bytes(content, encoding='utf-8')
            else:
                payload = content
            return self._cli.put_object(Bucket=bucket, Key=key, Body=payload)
        except ClientError as err:
            self.logger.debug(f"error response putting {bucket}/{key} {err}")
            if err.response['Error']['Code'] == 'SlowDown':
                raise SlowDown()
            else:
                raise

    @retry_on_slowdown()
    def _copy_single_object(self, bucket, src, dst):
        """Copy object to another location within the bucket"""
        try:
            src_uri = f"{bucket}/{src}"
            custom_headers = {"x-goog-copy-source": src_uri}
            return self._cli.copy_object(Bucket=bucket,
                                         Key=dst,
                                         CopySource=src_uri,
                                         custom_headers=custom_headers)
        except ClientError as err:
            self.logger.debug(f"error response copying {bucket}/{src}: {err}")
            if err.response['Error']['Code'] == 'SlowDown':
                raise SlowDown()
            else:
                raise

    def get_object_data(self, bucket, key):
        resp = self._get_object(bucket, key)
        return resp['Body'].read()

    def put_object(self, bucket, key, data, is_bytes=False):
        self._put_object(bucket, key, data, is_bytes)

    def copy_object(self,
                    bucket,
                    src,
                    dst,
                    validate=False,
                    validation_timeout_sec=30):
        """Copy object inside a bucket and optionally poll the destination until
        the copy will be available or timeout passes."""
        self._copy_single_object(bucket, src, dst)
        if validate:
            self._wait_key(bucket, dst, validation_timeout_sec)

    def move_object(self,
                    bucket,
                    src,
                    dst,
                    validate=False,
                    validation_timeout_sec=30):
        """Move object inside a bucket and optionally poll the destination until
        the new location will be available and old will disappear or timeout is reached."""
        self._copy_single_object(bucket, src, dst)
        self._delete_object(bucket, src)
        if validate:
            self._wait_key(bucket, dst, validation_timeout_sec)
            self._wait_no_key(bucket, src, validation_timeout_sec)

    def get_object_meta(self, bucket, key):
        """Get object metadata without downloading it"""
        resp = self._get_object(bucket, key)
        # Note: ETag field contains md5 hash enclosed in double quotes that have to be removed
        return ObjectMetadata(bucket=bucket,
                              key=key,
                              etag=resp['ETag'][1:-1],
                              content_length=resp['ContentLength'])

    def write_object_to_file(self, bucket, key, dest_path):
        """Get object and write it to file"""
        resp = self._get_object(bucket, key)
        with open(dest_path, 'wb') as f:
            body = resp['Body']
            for chunk in body.iter_chunks(chunk_size=0x1000):
                f.write(chunk)

    @retry_on_slowdown()
    def _list_objects(self,
                      *,
                      bucket,
                      token=None,
                      limit=1000,
                      prefix: Optional[str] = None,
                      client):
        try:
            if token is not None:
                return client.list_objects_v2(Bucket=bucket,
                                              MaxKeys=limit,
                                              ContinuationToken=token,
                                              Prefix=prefix if prefix else "")
            else:
                return client.list_objects_v2(Bucket=bucket,
                                              MaxKeys=limit,
                                              Prefix=prefix if prefix else "")
        except ClientError as err:
            self.logger.debug(f"error response listing {bucket}: {err}")
            if err.response['Error']['Code'] == 'SlowDown':
                raise SlowDown()
            else:
                raise

    def list_objects(self,
                     bucket,
                     topic: Optional[str] = None,
                     prefix: Optional[str] = None,
                     client=None) -> Iterator[ObjectMetadata]:
        """
        :param bucket: S3 bucket name
        :param topic: Optional, if set then only return objects belonging to this topic
        """

        if client is None:
            client = self._cli

        token = None
        truncated = True
        while truncated:
            try:
                res = self._list_objects(bucket=bucket,
                                         token=token,
                                         limit=100,
                                         prefix=prefix,
                                         client=client)
            except:
                # For debugging NoSuchBucket errors in tests: if we can't list
                # this bucket, then try to list what buckets exist.
                # Related: https://github.com/redpanda-data/redpanda/issues/8490
                self.logger.error(
                    f"Error in list_objects '{bucket}', listing all buckets")
                for k, v in self.list_buckets(client=client).items():
                    self.logger.error(f"Listed bucket {k}: {v}")
                raise

            token = res.get('NextContinuationToken')
            truncated = bool(res['IsTruncated'])
            if 'Contents' in res:
                for item in res['Contents']:
                    # Apply optional topic filtering
                    if topic is not None and key_to_topic(
                            item['Key']) != topic:
                        self.logger.debug(f"Skip {item['Key']} for {topic}")
                        continue

                    yield ObjectMetadata(bucket=bucket,
                                         key=item['Key'],
                                         etag=item['ETag'][1:-1],
                                         content_length=item['Size'])

    def list_buckets(self, client=None) -> dict[str, Union[list, dict]]:
        if client is None:
            client = self._cli
        try:
            return client.list_buckets()
        except Exception as ex:
            self.logger.error(f'Error listing buckets: {ex}')
            raise

    def create_expiration_policy(self, bucket: str, days: int):
        if self._is_gcs:
            self._gcp_create_expiration_policy(bucket, days)
        else:
            self._aws_create_expiration_policy(bucket, days)

    @retry_on_slowdown()
    def _aws_create_expiration_policy(self, bucket: str, days: int):
        try:
            self._cli.put_bucket_lifecycle_configuration(
                Bucket=bucket,
                LifecycleConfiguration={
                    "Rules": [{
                        "Expiration": {
                            "Days": days
                        },
                        "Filter": {},
                        "ID": f"{bucket}-ducktape-one-day-expiration",
                        "Status": "Enabled"
                    }]
                })
        except ClientError as err:
            if err.response['Error']['Code'] == 'SlowDown':
                self.logger.debug(
                    f"Got SlowDown code when creating bucket lifecycle configuration for {bucket}"
                )
                raise SlowDown()

            self.logger.error(
                f"Failed to set lifecycle configuration for {bucket}: {err}")
            raise err

    def _gcp_create_expiration_policy(self, bucket: str, days: int):
        gcs_client = gcs.Client()
        gcs_bucket = gcs_client.get_bucket(bucket)
        gcs_bucket.add_lifecycle_delete_rule(age=days)
        gcs_bucket.patch()

    def _add_header(self, model, params, request_signer, **kwargs):
        params['headers'].update(self._before_call_headers)

    @property
    def _is_gcs(self):
        """
        For most interactions we use GCS via the S3 compatible API. However,
        for some management operations we need to apply custom logic.
        https://cloud.google.com/storage/docs/migrating#methods-comparison
        """
        return self._endpoint is not None and 'storage.googleapis.com' in self._endpoint
