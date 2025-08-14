# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import uuid
from dataclasses import dataclass
from typing import Set

import databricks
import databricks.sdk
import databricks.sdk.errors
import databricks.sql
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import (
    CatalogInfo,
    CatalogType,
    ExternalLocationInfo,
)
from ducktape.services.service import Service
from ducktape.tests.test import TestContext

from rptest.context.databricks import DatabricksContext


class DatabricksWorkspace(Service):
    """
    A service wrapper for a hosted Databricks workspace.
    This service does not manage any nodes, but it provides a wrapper for
    the Databricks SDK to interact with the workspace and manage its resources.
    Including post-testing cleanup.
    """
    def __init__(self, context: TestContext):
        # Resource trackers.
        self._location_names: Set[str] = set()
        self._catalog_names: Set[str] = set()

        super().__init__(context, num_nodes=0)

        self._databricks_context = DatabricksContext.from_context(context)

        self._client = WorkspaceClient(
            config=self._databricks_context.databricks_config, )

        # Fail fast if credentials are invalid.
        self._verify_credentials()

    def stop(self, **kwargs):
        self.logger.debug("Stopping Databricks workspace")
        self._cleanup_catalogs()
        self._cleanup_dbx_locations()
        super().stop(**kwargs)

    def create_catalog(self, bucket: str) -> "DatabricksCatalogInfo":
        """
        Creates a brand new catalog in the databricks workspace.
        """

        self._location_names.add(bucket)

        try:
            location: ExternalLocationInfo = self._client.external_locations.create(
                name=bucket,
                # TODO: Add support for gcs, azure
                url=f"s3://{bucket}",
                credential_name=self._databricks_context.
                ext_loc_credential_name,
            )
            self.logger.debug(f"Created external location: {location}")
        except databricks.sdk.errors.DatabricksError as e:
            self.logger.error(f"Failed to create external location: {str(e)}")
            raise

        requested_catalog_name = f"panda-catalog-{uuid.uuid1()}"
        self._catalog_names.add(requested_catalog_name)

        catalog_info: CatalogInfo = self._client.catalogs.create(
            name=requested_catalog_name,
            storage_root=location.url,
        )
        self.logger.debug(f"Created catalog: {catalog_info}")

        assert catalog_info.catalog_type == CatalogType.MANAGED_CATALOG, \
            "We expect to only managed catalogs."
        assert catalog_info.name, "Catalog name must not be empty"

        try:
            sql_connection = databricks.sql.connect(
                server_hostname=self._databricks_context.server_hostname,
                http_path=self._databricks_context.sql_warehouse_path,
                catalog=catalog_info.name,
                credentials_provider=self._databricks_context.
                credentials_provider,
            )
            self.logger.debug("SQL connection established successfully.")
        except Exception as e:
            self.logger.error(f"Error establishing SQL connection: {e}")
            raise

        # This is a unity catalog peculiarity. It allows schemas (iceberg
        # namespaces) to be created but tables inside it are not allowed
        # to be created without this grant.
        #
        # To avoid this problem we create both, the schema and the grant
        # before redpanda broker is started.
        with sql_connection.cursor() as cursor:
            cursor.execute("CREATE SCHEMA `redpanda`;")
            self.logger.debug("Schema created successfully")

            principal_row = cursor.execute("SELECT current_user()").fetchone()
            assert principal_row, "Failed to get current user"
            principal = principal_row[0]

            # TODO: Identify Minimal Privileges (Least access principle)
            self.logger.debug(
                f"GRANT ALL PRIVILEGES ON CATALOG `{catalog_info.name}` TO `{principal}`"
            )
            sql = f"GRANT ALL PRIVILEGES ON CATALOG `{catalog_info.name}` TO `{principal}`"
            cursor.execute(sql)
            self.logger.debug("Granted PRIVILEGES successfully")

            self.logger.debug(f"Creating grants for: {principal=}")

            cursor.execute(
                f"GRANT EXTERNAL USE SCHEMA ON SCHEMA `redpanda` TO `{principal}`"
            )

            self.logger.debug("Grants created successfully")

        return DatabricksCatalogInfo(name=catalog_info.name)

    def _verify_credentials(self):
        try:
            user_info = self._client.current_user.me()
            self.logger.debug(f"Authenticated as: {user_info}")
        except Exception as e:
            self.logger.error(f"Error fetching user info: {str(e)}")

    def _cleanup_catalogs(self):
        for catalog_name in self._catalog_names:
            self.logger.debug(f"Cleaning up catalog {catalog_name}")

            # Verify if catalog was created successfully and only then delete.
            try:
                self._client.catalogs.get(catalog_name)
            except databricks.sdk.errors.platform.NotFound:
                self.logger.warning(
                    f"Catalog {catalog_name} not found for deletion")
                continue

            sql_connection = databricks.sql.connect(
                server_hostname=self._databricks_context.server_hostname,
                http_path=self._databricks_context.sql_warehouse_path,
                catalog=catalog_name,
                credentials_provider=self._databricks_context.
                credentials_provider,
            )

            with sql_connection.cursor() as cursor:
                schemas_rows = cursor.execute("SHOW SCHEMAS").fetchall()
                self.logger.debug(f"Schemas: {schemas_rows}")

                for schema_row in schemas_rows:
                    if schema_row[0] == "information_schema":
                        # Skip system owned schemas.
                        continue

                    self.logger.debug(f"Cleaning up schema {schema_row[0]}")

                    tables_rows = cursor.execute(
                        f"SHOW TABLES IN `{schema_row[0]}`").fetchall()

                    for schema_name, table_name, _ in tables_rows:
                        self.logger.debug(
                            f"Cleaning up table {table_name} in schema {schema_name}"
                        )
                        cursor.execute(
                            f"DROP TABLE `{schema_name}`.`{table_name}`")

                    cursor.execute(f"DROP SCHEMA `{schema_row[0]}` CASCADE")

            self._client.catalogs.delete(name=catalog_name)
            self.logger.debug(f"Deleted catalog {catalog_name}")

        self.logger.debug("Cleanup completed for catalogs.")

    def _cleanup_dbx_locations(self):
        for location_name in self._location_names:
            self.logger.debug(f"Cleaning up location {location_name}")

            try:
                self._client.external_locations.delete(location_name)
            except databricks.sdk.errors.platform.NotFound:
                self.logger.warning(
                    f"Location {location_name} not found for deletion")
                continue

            self.logger.debug(f"Deleted location {location_name}")

        self.logger.debug("Cleanup completed for locations.")


@dataclass
class DatabricksCatalogInfo:
    """
    A databricks catalog class encapsulating details pertaining to redpanda
    testing inside the ducktape framework.
    """
    name: str
