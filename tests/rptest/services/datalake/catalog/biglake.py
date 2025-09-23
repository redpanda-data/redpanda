# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from typing import Any, Callable, Dict

from pyiceberg.catalog.rest.auth import AuthManager
from rptest.context.gcp import GCPContext
from rptest.services.catalog_service import CatalogService, CatalogType


class BiglakeMetastore(CatalogService):
    """
    https://cloud.google.com/bigquery/docs/blms-rest-catalog
    """

    def __init__(
        self,
        ctx: Any,
        *,
        cloud_storage_bucket: str,
        gcp_context: GCPContext,
    ):
        super().__init__(
            ctx,
            cloud_storage_bucket,
            # Custom warehouse names are not supported for GCP. Bucket name *is* the warehouse.
            warehouse_name="",
            num_nodes=0,
        )

        # self.warehouse_name = catalog.name
        self.compute_warehouse_path()

        self.gcp_context = gcp_context

    def catalog_type(self) -> CatalogType:
        return CatalogType.BIGLAKE

    @property
    def iceberg_rest_url(self) -> str:
        return "https://biglake.googleapis.com/iceberg/v1beta/restcatalog"

    @property
    def iceberg_rest_port(self) -> int:
        raise NotImplementedError("Biglake does not expose a port")

    def _configure_client(self, conf: Dict[str, Any]) -> None:
        conf["header.x-goog-user-project"] = self.gcp_context.project_id
        conf["header.X-Iceberg-Access-Delegation"] = "remote-signing"
        # Use gcsfs explicitly if you find issues with the default implementation (Arrow fs).
        # conf["py-io-impl"] = "pyiceberg.io.fsspec.FsspecFileIO"
        conf["auth"] = {
            "type": "custom",
            "impl": "rptest.services.datalake.catalog.biglake.BiglakeAuth",
            "custom": {"token_fetcher": lambda: self.gcp_context.fetch_iam_token()},
        }


class BiglakeAuth(AuthManager):
    def __init__(self, token_fetcher: Callable[[], str]):
        self.token_fetcher = token_fetcher

    def auth_header(self) -> str | None:
        """Return the Authorization header value, or None if not applicable."""
        return f"Bearer {self.token_fetcher()}"
