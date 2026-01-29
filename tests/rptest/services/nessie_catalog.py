# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import os
from typing import Optional

import requests
from ducktape.utils.util import wait_until

from rptest.context import cloud_storage
from rptest.services.catalog_service import CatalogService, CatalogType


class NessieCatalog(CatalogService):
    """Nessie Catalog service

    The nessie catalog service maintain lifecycle of catalog process on the nodes.
    The service deploys nessie in a test mode with in-memory storage which is intended
    to be used for dev/test purposes.
    """

    PERSISTENT_ROOT = "/var/lib/nessie"
    INSTALL_PATH = "/opt/nessie"
    JAR = "quarkus-run.jar"
    JAR_PATH = f"/opt/nessie/deployments/{JAR}"
    NESSIE_PORT = 19120
    # Trino currently lacks SQL support for Nessie management:
    # https://projectnessie.org/iceberg/trino/.
    # Spark supports Nessie management via SQL, but uses the v1
    # API at time of writing: https://projectnessie.org/iceberg/spark/
    # Bump this API version to 'v2' when these catalog services better
    # support it.
    NESSIE_API_VERSION = "v1"

    # This should be kept up to date with version set in docker/ducktape-deps
    NESSIE_VERSION = "0.102.2"
    NESSIE_DEFAULT_WAREHOUSE = "main"

    LOG_FILE = os.path.join(PERSISTENT_ROOT, "nessie.log")
    logs = {
        "nessie_logs": {"path": LOG_FILE, "collect_default": True},
    }

    def __init__(
        self,
        ctx,
        cloud_storage_bucket: str,
        warehouse_name: str = CatalogService.DEFAULT_WAREHOUSE_NAME,
    ):
        super(NessieCatalog, self).__init__(
            ctx, cloud_storage_bucket, warehouse_name, num_nodes=1
        )

        self._ctx = ctx
        self._current_reference = "main"
        self.compute_warehouse_path()

        self._vendor_api_url = None
        self._catalog_url: Optional[str] = None

    @property
    def iceberg_rest_url(self) -> str:
        assert self._catalog_url, "URL not available because service is not started"
        return self._catalog_url

    @property
    def iceberg_rest_port(self) -> int:
        return self.NESSIE_PORT

    @property
    def vendor_api_url(self) -> str:
        assert self._vendor_api_url, "URL not available because service is not started"
        return self._vendor_api_url

    def catalog_type(self) -> CatalogType:
        return CatalogType.NESSIE

    def _java_home(self, node):
        return (
            node.account.ssh_output(
                "echo /usr/lib/jvm/java-21-openjdk-$(dpkg-architecture -q DEB_BUILD_ARCH)"
            )
            .decode("utf-8")
            .strip()
        )

    def _java_bin(self, node):
        java_home = self._java_home(node)
        return f"{java_home}/bin/java"

    def _make_env(self):
        env = dict()
        # Disable OpenTelemetry AWS and GCP resource providers that attempt to contact
        # cloud metadata services (AWS IMDS at 169.254.169.254, GCP metadata at
        # metadata.google.internal). When running in Docker containers not on these
        # clouds, these connections hang until timeout, causing slow startup.
        # In case there are other resource providers to disable in the future, we
        # can change it to
        # env["OTEL_JAVA_ENABLED_RESOURCE_PROVIDERS"] = "non.existing.class.name"
        # (at least one entry needs to be present for the setting to take effect).
        env["OTEL_JAVA_DISABLED_RESOURCE_PROVIDERS"] = ",".join(
            [
                # AWS resource providers
                "io.opentelemetry.contrib.aws.resource.Ec2ResourceProvider",
                "io.opentelemetry.contrib.aws.resource.EcsResourceProvider",
                "io.opentelemetry.contrib.aws.resource.EksResourceProvider",
                "io.opentelemetry.contrib.aws.resource.BeanstalkResourceProvider",
                "io.opentelemetry.contrib.aws.resource.LambdaResourceProvider",
                # GCP resource provider
                "io.opentelemetry.contrib.gcp.resource.GCPResourceProvider",
            ]
        )
        env["NESSIE_CATALOG_DEFAULT_WAREHOUSE"] = NessieCatalog.NESSIE_DEFAULT_WAREHOUSE
        env[
            f"NESSIE_CATALOG_WAREHOUSES_{NessieCatalog.NESSIE_DEFAULT_WAREHOUSE.upper()}_LOCATION"
        ] = self.cloud_storage_warehouse

        if isinstance(self.credentials, cloud_storage.S3Credentials):
            env["NESSIE_CATALOG_SERVICE_S3_DEFAULT_OPTIONS_REGION"] = (
                self.credentials.region
            )
            env["NESSIE_CATALOG_SERVICE_S3_DEFAULT_OPTIONS_ENDPOINT"] = (
                self.credentials.endpoint
            )
            env["NESSIE_CATALOG_VALIDATE_SECRETS"] = "true"
        return env

    def _make_java_properties(self):
        """
        These options don't work nicely with conversion to env variable format.
        Specify them as Java -D properties instead.
        https://projectnessie.org/nessie-latest/configuration/#s3-default-bucket-settings
        """
        d_flags = ""
        if isinstance(self.credentials, cloud_storage.S3Credentials):
            d_flags += "-Dnessie.catalog.service.s3.default-options.access-key=urn:nessie-secret:quarkus:my-secrets-default "
            d_flags += f"-Dmy-secrets-default.name={self.credentials.access_key} "
            d_flags += f"-Dmy-secrets-default.secret={self.credentials.secret_key} "
            d_flags += "-Dnessie.catalog.validate-secrets=true"
        elif isinstance(self.credentials, cloud_storage.AWSInstanceMetadataCredentials):
            d_flags += "-Dnessie.catalog.service.s3.default-options.auth-type=APPLICATION_GLOBAL"
        elif isinstance(self.credentials, cloud_storage.GCPInstanceMetadataCredentials):
            d_flags += "-Dnessie.catalog.service.gcs.default-options.auth-type=APPLICATION_DEFAULT"
        elif isinstance(self.credentials, cloud_storage.ABSSharedKeyCredentials):
            d_flags += "-Dnessie.catalog.service.adls.default-options.auth-type=STORAGE_SHARED_KEY "
            d_flags += "-Dnessie.catalog.service.adls.default-options.account=urn:nessie-secret:quarkus:my-secrets-default "
            d_flags += f"-Dmy-secrets-default.name={self.credentials.account_name} "
            d_flags += f"-Dmy-secrets-default.secret={self.credentials.account_key} "
            d_flags += f"-Dnessie.catalog.service.adls.default-options.endpoint=https://{self.credentials.endpoint}/{self.cloud_storage_bucket}"
        return d_flags

    def _java_cmd(self, node):
        java_home = self._java_home(node)
        java_bin = self._java_bin(node)
        envs = self._make_env()
        env = " ".join(f"{k}={v}" for k, v in envs.items())
        d_props = self._make_java_properties()
        return f"{env} JAVA_HOME={java_home} nohup {java_bin} {d_props} -jar {NessieCatalog.JAR_PATH} \
        1>> {NessieCatalog.LOG_FILE} 2>> {NessieCatalog.LOG_FILE} & echo $!"

    def _nessie_base_path(self, node):
        return f"http://{node.account.hostname}:{NessieCatalog.NESSIE_PORT}"

    def _http_request_path_from_node(self, node, endpoint):
        return f"{self._nessie_base_path(node)}/api/{endpoint}"

    def _nessie_iceberg_path(self, node):
        return f"{self._nessie_base_path(node)}/iceberg"

    def start_node(self, node, timeout_sec=60, **kwargs):
        node.account.ssh(
            "mkdir -p %s" % NessieCatalog.PERSISTENT_ROOT, allow_fail=False
        )

        cmd = self._java_cmd(node)
        self.logger.info(
            f"Starting nessie catalog service on {node.name} with command {cmd}"
        )

        node.account.ssh(cmd, allow_fail=False)

        # wait for the config endpoint to return 200
        def _nessie_ready():
            config_path = self._http_request_path_from_node(
                node, f"{NessieCatalog.NESSIE_API_VERSION}/config"
            )
            self.logger.debug(f"Querying nessie healthcheck on {config_path}")
            r = requests.get(config_path, timeout=10)

            self.logger.info(f"health check result status code: {r.status_code}")
            return r.status_code == 200

        wait_until(
            _nessie_ready,
            timeout_sec=timeout_sec,
            backoff_sec=0.4,
            err_msg="Error waiting for nessie catalog to start",
            retry_on_exc=True,
        )

        self._catalog_url = self._nessie_iceberg_path(node)
        self._vendor_api_url = self._http_request_path_from_node(
            node, NessieCatalog.NESSIE_API_VERSION
        )

    def wait_node(self, node, timeout_sec=None):
        ## unused as there is nothing to wait for here
        return False

    def stop_node(self, node, allow_fail=False, **_):
        node.account.kill_java_processes(NessieCatalog.JAR, allow_fail=allow_fail)

        def _stopped():
            out = node.account.ssh_output("jcmd").decode("utf-8")
            return NessieCatalog.JAR not in out

        wait_until(
            _stopped, timeout_sec=10, backoff_sec=1, err_msg="Error stopping Nessie"
        )

    def clean_node(self, node, **_):
        self.stop_node(node, allow_fail=True)
        node.account.remove(NessieCatalog.PERSISTENT_ROOT, allow_fail=True)
