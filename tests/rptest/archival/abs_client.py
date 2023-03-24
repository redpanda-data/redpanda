from rptest.archival.s3_client import ObjectMetadata
from rptest.archival.shared_client_utils import key_to_topic

from azure.storage.blob import BlobClient, BlobServiceClient, BlobType, ContainerClient
from itertools import islice

import time
import datetime
from logging import Logger
from typing import Iterator, Optional


def build_connection_string(proto: str, endpoint: Optional[str],
                            storage_account: str, key: str) -> str:
    assert proto in ["http", "https"
                     ], f"Invalid protocol supplied to ABS client: {proto}"

    parts = [
        f"DefaultEndpointsProtocol={proto}", f"AccountName={storage_account}",
        f"AccountKey={key}"
    ]

    if proto == "http":
        parts += [f"BlobEndpoint={endpoint}"]
    else:
        parts += ["EndpointSuffix=core.windows.net"]

    return ";".join(parts)


class ABSClient:
    def __init__(self,
                 logger: Logger,
                 storage_account: str,
                 shared_key: str,
                 endpoint: Optional[str] = None):
        self.logger = logger

        if endpoint is None:
            proto = "https"
        else:
            proto = "http"

        self.conn_str = build_connection_string(proto, endpoint,
                                                storage_account, shared_key)

    def _wait_no_key(self,
                     blob_client: BlobServiceClient,
                     timeout_sec: float = 10):
        deadline = datetime.datetime.now() + datetime.timedelta(
            seconds=timeout_sec)

        try:
            while blob_client.exists():
                now = datetime.datetime.now()
                if now > deadline:
                    raise TimeoutError(
                        f"Blob was not deleted within {timeout_sec}s")
                time.sleep(2)
        except Exception as err:
            if isinstance(err, TimeoutError):
                raise err

            self.logger.debug(f"error ocurred while polling blob {err}")

    def create_bucket(self, name: str):
        blob_service = BlobServiceClient.from_connection_string(self.conn_str)
        blob_service.create_container(name)

    def delete_bucket(self, name: str):
        blob_service = BlobServiceClient.from_connection_string(self.conn_str)
        blob_service.delete_container(name)

    def empty_bucket(self, name: str, parallel=False):
        container_client = ContainerClient.from_connection_string(
            self.conn_str, container_name=name)
        blob_names_generator = container_client.list_blob_names()

        if parallel:

            def chunks(generator):
                it = iter(generator)
                # 'delete_blobs' can only handle batches
                # of up to 256 elements
                while (batch := list(islice(it, 256))):
                    yield batch

            for chunk in chunks(blob_names_generator):
                container_client.delete_blobs(*chunk)
        else:
            for blob in blob_names_generator:
                container_client.delete_blob(blob)

        return []

    def move_object(self,
                    bucket: str,
                    src: str,
                    dst: str,
                    validate: bool = False,
                    validation_timeout_sec=30):
        # `validate` and `validation_timeout_sec` are unused, only present
        # for compatibility with S3Client
        container_client = ContainerClient.from_connection_string(
            self.conn_str, container_name=bucket)

        # There is no server-side "move" in ABS: this is a crude implementation,
        # but good enough for tiny objects
        content = container_client.download_blob(src).readall()
        container_client.upload_blob(dst, content)
        container_client.delete_blob(src)

    def delete_object(self, bucket: str, key: str, verify=False):
        blob_client = BlobClient.from_connection_string(self.conn_str,
                                                        container_name=bucket,
                                                        blob_name=key)
        blob_client.delete_blob()

        if verify:
            self._wait_no_key(blob_client)

    def get_object_data(self, bucket: str, key: str) -> bytes:
        blob_client = BlobClient.from_connection_string(self.conn_str,
                                                        container_name=bucket,
                                                        blob_name=key)
        return blob_client.download_blob().content_as_bytes()

    def put_object(self, bucket: str, key: str, data: str):
        container_client = ContainerClient.from_connection_string(
            self.conn_str, container_name=bucket)
        blob_client = container_client.upload_blob(
            name=key,
            data=bytes(data, encoding="utf-8"),
            blob_type=BlobType.BLOCKBLOB,
            overwrite=True)

        assert blob_client.exists(), f"Failed to upload blob {key}"

    def get_object_meta(self, bucket: str, key: str) -> ObjectMetadata:
        blob_client = BlobClient.from_connection_string(self.conn_str,
                                                        container_name=bucket,
                                                        blob_name=key)
        props = blob_client.get_blob_properties()

        assert props.deleted == False

        # Note that we return the hexified md5 hash computed by Azure
        # as the 'etag'. This is done in order to mimic the S3 behaviour
        # and keep parametrised tests passing without making them aware
        # of the cloud storage client being used.
        return ObjectMetadata(bucket=props.container,
                              key=props.name,
                              etag=props.content_settings.content_md5.hex(),
                              content_length=props.size)

    def list_objects(self,
                     bucket: str,
                     topic: Optional[str] = None) -> Iterator[ObjectMetadata]:
        container_client = ContainerClient.from_connection_string(
            self.conn_str, container_name=bucket)
        for blob_props in container_client.list_blobs():
            if topic is not None and key_to_topic(blob_props.name) != topic:
                self.logger.debug(f"Skip {blob_props.name} for {topic}")
                continue

            yield ObjectMetadata(
                bucket=blob_props.container,
                key=blob_props.name,
                etag=blob_props.content_settings.content_md5.hex(),
                content_length=blob_props.size)
