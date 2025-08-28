# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from abc import ABC
from dataclasses import dataclass
from logging import Logger
from typing import Any, Callable, Optional
from urllib.parse import urlparse

from databricks.sdk.core import Config, oauth_service_principal, pat_auth
from databricks.sdk.credentials_provider import CredentialsProvider
from ducktape.tests.test import TestContext

GLOBAL_DATABRICKS_WORKSPACE_URL = "databricks_workspace_url"
GLOBAL_DATABRICKS_SQL_WAREHOUSE_PATH = "databricks_sql_warehouse_path"
GLOBAL_DATABRICKS_EXT_LOC_CREDENTIAL_NAME = "databricks_ext_loc_credential_name"

GLOBAL_DATABRICKS_TOKEN = "databricks_token"
GLOBAL_DATABRICKS_CLIENT_ID = "databricks_client_id"
GLOBAL_DATABRICKS_CLIENT_SECRET = "databricks_client_secret"


class DatabricksContext:
    def __init__(
        self,
        *,
        workspace_url: str,
        credentials: "Credentials",
        sql_warehouse_path: str,
        ext_loc_credential_name: str,
        logger: Logger,
    ):
        self.workspace_url = workspace_url
        self.credentials = credentials
        self.sql_warehouse_path = sql_warehouse_path
        self.ext_loc_credential_name = ext_loc_credential_name
        self.logger = logger

    @staticmethod
    def available(test_context: TestContext) -> bool:
        return (
            GLOBAL_DATABRICKS_WORKSPACE_URL in test_context.globals
            and test_context.globals.get(GLOBAL_DATABRICKS_WORKSPACE_URL).strip() != ""
        )

    @staticmethod
    def from_context(test_context: TestContext) -> "DatabricksContext":
        # I.e. https://dbc-0f5177e3-6aa4.cloud.databricks.com/
        workspace_url = test_context.globals.get(GLOBAL_DATABRICKS_WORKSPACE_URL)

        credentials = Credentials.from_context(test_context)

        # I.e. /sql/1.0/warehouses/876tfg
        sql_warehouse_path = test_context.globals.get(
            GLOBAL_DATABRICKS_SQL_WAREHOUSE_PATH
        )

        ext_loc_credential_name = test_context.globals.get(
            GLOBAL_DATABRICKS_EXT_LOC_CREDENTIAL_NAME
        )

        assert workspace_url, "Missing databricks workspace url"
        assert sql_warehouse_path, "Missing databricks sql warehouse path"
        assert ext_loc_credential_name, (
            "Missing databricks external location credential name"
        )

        # Remove trailing slash so that no one has to worry about it.
        workspace_url = str(workspace_url).rstrip("/")

        return DatabricksContext(
            workspace_url=workspace_url,
            credentials=credentials,
            sql_warehouse_path=sql_warehouse_path,
            ext_loc_credential_name=ext_loc_credential_name,
            logger=test_context.logger,
        )

    @property
    def iceberg_rest_url(self) -> str:
        return f"{self.workspace_url}/api/2.1/unity-catalog/iceberg-rest"

    @property
    def server_hostname(self) -> str:
        parsed_url = urlparse(self.workspace_url)

        assert parsed_url.hostname, "Invalid workspace URL"

        return parsed_url.hostname

    @property
    def databricks_config(self) -> Config:
        """
        Returns the Databricks SDK configuration object.
        """
        config_kwargs: dict[str, Any] = {}

        # This is not a typo. The host is the workspace URL, not the server
        # hostname.
        config_kwargs["host"] = self.workspace_url

        if isinstance(self.credentials, PatCredentials):
            config_kwargs["token"] = self.credentials.token
        elif isinstance(self.credentials, OauthCredentials):
            config_kwargs["client_id"] = self.credentials.client_id
            config_kwargs["client_secret"] = self.credentials.client_secret
        else:
            raise ValueError(f"Unsupported credentials type: {type(self.credentials)}")

        return Config(**config_kwargs)

    @property
    def credentials_provider(self) -> Callable[[], Optional[CredentialsProvider]]:
        """
        Note: This method is a @property to avoid ambiguity whether the user
        needs to call the method or not when Databricks sdk expects a
        credential provider callable. With @property annotation we know it
        will always be invoked.
        Note: Useful for `databricks.sql.connect(...)`.
        """

        if isinstance(self.credentials, PatCredentials):
            return lambda: pat_auth(self.databricks_config)
        elif isinstance(self.credentials, OauthCredentials):
            return lambda: oauth_service_principal(self.databricks_config)
        else:
            raise ValueError(f"Unsupported credentials type: {type(self.credentials)}")


class Credentials(ABC):
    @staticmethod
    def from_context(test_context: TestContext) -> "Credentials":
        if _opt_str_not_empty(test_context.globals.get(GLOBAL_DATABRICKS_TOKEN)):
            return PatCredentials(
                token=test_context.globals.get(GLOBAL_DATABRICKS_TOKEN)
            )
        elif _opt_str_not_empty(test_context.globals.get(GLOBAL_DATABRICKS_CLIENT_ID)):
            # We assume that if client_id is set, client_secret is also set.
            client_id = test_context.globals.get(GLOBAL_DATABRICKS_CLIENT_ID)
            client_secret = test_context.globals.get(GLOBAL_DATABRICKS_CLIENT_SECRET)

            assert client_id, "Missing databricks client id"
            assert client_secret, "Missing databricks client secret"

            return OauthCredentials(
                client_id=test_context.globals.get(GLOBAL_DATABRICKS_CLIENT_ID),
                client_secret=test_context.globals.get(GLOBAL_DATABRICKS_CLIENT_SECRET),
            )
        else:
            raise ValueError(
                "Neither databricks token nor client id/secret is set. Please configure one."
            )


class PatCredentials(Credentials):
    """
    https://docs.databricks.com/aws/en/dev-tools/auth/pat
    """

    def __init__(self, token: str):
        self.token = token


@dataclass
class OauthCredentials(Credentials):
    """
    https://docs.databricks.com/aws/en/dev-tools/auth/oauth-m2m
    """

    client_id: str
    client_secret: str


def _opt_str_not_empty(value: Optional[str]) -> bool:
    """
    Helper function to check if a string is not None and not empty.
    """
    return value is not None and value.strip() != ""
