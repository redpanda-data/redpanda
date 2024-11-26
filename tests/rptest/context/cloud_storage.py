import abc
from enum import IntEnum
from typing import Optional

from ducktape.tests.test import TestContext

# Injected args
ARG_CLOUD_STORAGE_TYPE_KEY = "cloud_storage_type"

# Cloud storage generic keys
GLOBAL_CLOUD_STORAGE_CRED_SOURCE_KEY = "cloud_store_cred_source"
GLOBAL_CLOUD_PROVIDER = "cloud_provider"

# S3 specific keys
GLOBAL_S3_ACCESS_KEY = "s3_access_key"
GLOBAL_S3_SECRET_KEY = "s3_secret_key"
GLOBAL_S3_REGION_KEY = "s3_region"

# ABS specific keys
GLOBAL_ABS_STORAGE_ACCOUNT = "abs_storage_account"
GLOBAL_ABS_SHARED_KEY = "abs_shared_key"


class CloudStorageType(IntEnum):
    # Use (AWS, GCP) S3 compatible API on dedicated nodes, or minio in docker
    S3 = 1
    # Use Azure ABS on dedicated nodes, or azurite in docker
    ABS = 2


class Credentials(abc.ABC):
    """
    Typed credentials for cloud storage.
    """
    @staticmethod
    def from_context(test_context: TestContext) -> 'Credentials':
        if not test_context.injected_args or ARG_CLOUD_STORAGE_TYPE_KEY not in test_context.injected_args:
            raise ValueError(
                f"Test must be parametrized with {ARG_CLOUD_STORAGE_TYPE_KEY} to use this method"
            )

        type = test_context.injected_args[ARG_CLOUD_STORAGE_TYPE_KEY]
        source = test_context.globals.get(GLOBAL_CLOUD_STORAGE_CRED_SOURCE_KEY,
                                          'config_file')
        cloud_provider = test_context.globals.get(GLOBAL_CLOUD_PROVIDER, 'aws')

        if type == CloudStorageType.S3:
            if source == 'config_file':
                return Credentials._s3_credentials(
                    cloud_provider=cloud_provider,
                    region=test_context.globals.get(GLOBAL_S3_REGION_KEY),
                    access_key=test_context.globals.get(GLOBAL_S3_ACCESS_KEY),
                    secret_key=test_context.globals.get(GLOBAL_S3_SECRET_KEY))
            elif source in ['aws_instance_metadata', 'gcp_instance_metadata']:
                return Credentials._instance_metadata_credentials(
                    cloud_provider=cloud_provider)
            else:
                raise ValueError(
                    f"Unsupported credential source: {source} for type: {type}"
                )
        elif type == CloudStorageType.ABS:
            if source == 'config_file':
                return Credentials._abs_credentials(
                    account_name=test_context.globals.get(
                        GLOBAL_ABS_STORAGE_ACCOUNT),
                    account_key=test_context.globals.get(
                        GLOBAL_ABS_SHARED_KEY))
            else:
                raise ValueError(
                    f"Unsupported credential source: {source} for type: {type}"
                )
        else:
            raise ValueError(f"Unsupported cloud storage type: {type}")

    @staticmethod
    def _s3_credentials(*, cloud_provider: str, region: Optional[str],
                        access_key: Optional[str],
                        secret_key: Optional['str']) -> 'Credentials':
        if access_key or secret_key:
            # If we have at least one explicit credential, assume real
            # credentials are provided.
            assert access_key and secret_key, "Both access_key and secret_key must be provided"

            # Adjust endpoint for GCP.
            endpoint = None
            if cloud_provider == 'gcp':
                endpoint = "https://storage.googleapis.com"

            return S3Credentials(endpoint=endpoint,
                                 region=region,
                                 access_key=access_key,
                                 secret_key=secret_key)
        else:
            # Fallback to local minio.
            return S3Credentials(endpoint="http://minio-s3:9000",
                                 region="panda-region",
                                 access_key="panda-user",
                                 secret_key="panda-secret")

    @staticmethod
    def _instance_metadata_credentials(*,
                                       cloud_provider: str) -> 'Credentials':
        if cloud_provider == 'aws':
            return AWSInstanceMetadataCredentials()
        elif cloud_provider == 'gcp':
            return GCPInstanceMetadataCredentials()
        else:
            raise ValueError(f"Unsupported cloud provider: {cloud_provider}")

    @staticmethod
    def _abs_credentials(*, account_name: Optional[str],
                         account_key: Optional[str]) -> 'Credentials':
        if account_name or account_key:
            # If we have at least one explicit credential, assume real
            # credentials are provided.
            assert account_name and account_key, "Both account_name and account_key must be provided"

            return ABSSharedKeyCredentials(
                # Hardcoded dfs (Data Lake Storage Gen2) endpoint.
                endpoint=f"{account_name}.dfs.core.windows.net",
                account_name=account_name,
                account_key=account_key)
        else:
            # Fallback to local azurite.
            # https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azurite?tabs=visual-studio%2Cblob-storage#well-known-storage-account-and-key
            account_name = "devstoreaccount1"
            account_key = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="

            return ABSSharedKeyCredentials(
                # Hardcoded blob endpoint as azurite doesn't Data Lake Storage Gen2.
                # https://github.com/Azure/Azurite/issues/553
                endpoint=f"{account_name}.blob.localhost",
                account_name=account_name,
                account_key=account_key)


class S3Credentials(Credentials):
    """
    Explicitly provided S3 credentials
    """
    def __init__(self, *, endpoint: Optional[str], region: Optional[str],
                 access_key: str, secret_key: str):
        self.endpoint = endpoint
        self.region = region
        self.access_key = access_key
        self.secret_key = secret_key


class AWSInstanceMetadataCredentials(Credentials):
    """
    Use the instance metadata service to fetch credentials. This is used when
    running tests on AWS EC2 instances.
    We defer to individual services to self-configure/fetch credentials.
    Usually done by SDKs.
    """
    def __init__(self):
        pass


class GCPInstanceMetadataCredentials(Credentials):
    """
    Use the instance metadata service to fetch credentials. This is used when
    running tests on GCP instances.
    We defer to individual services to self-configure/fetch credentials.
    Usually done by SDKs.
    """
    def __init__(self):
        pass


class ABSSharedKeyCredentials(Credentials):
    def __init__(self, endpoint: str, account_name: str, account_key: str):
        self.endpoint = endpoint
        self.account_name = account_name
        self.account_key = account_key
