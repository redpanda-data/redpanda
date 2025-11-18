# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

"""
Schema Registry client for ducktape tests.

This module provides a simple wrapper around the auto-generated OpenAPI client
to make it easy to use in ducktape tests.

Example:
    from rptest.clients.schema_registry import SchemaRegistryClient
    from schema_registry_client.models import SchemaDef

    # In your test
    client = SchemaRegistryClient(self.redpanda)

    # Register a schema
    schema_id = client.api.post_subject_versions(
        subject="test-value",
        schema_def=SchemaDef(
            var_schema='{"type": "string"}',
            schema_type="AVRO"
        )
    )
    print(f"Registered schema with ID: {schema_id.id}")
"""

import random

import urllib3

from rptest.clients.schema_registry.generated.schema_registry_client import (
    ApiClient,
    Configuration,
    DefaultApi,
)

# Re-export all models for convenience
from rptest.clients.schema_registry.generated.schema_registry_client.models import (
    ErrorBody,
    GetCompatibility,
    GetSchemasIdsIdVersions200ResponseInner,
    IsCompatibile,
    Mode,
    PostSubjectVersions200Response,
    PutCompatibility,
    SchemaDef,
    SchemaDefReferencesInner,
    SecurityAcl,
    StoredSchema,
)
from rptest.services.redpanda import RedpandaService


class SchemaRegistryClient:
    """
    Wrapper around the auto-generated Schema Registry API client.

    This class simplifies the configuration and usage of the generated client
    for ducktape tests.

    Attributes:
        api: The DefaultApi instance with all Schema Registry methods
        configuration: The API client configuration
    """

    def __init__(self, redpanda_service: RedpandaService, hostname: str | None = None):
        """
        Initialize the Schema Registry client.

        Args:
            redpanda_service: RedpandaService instance from the test
        """
        # Get one of the nodes

        if hostname is not None:
            host = hostname
        else:
            node = random.choice(redpanda_service.nodes)
            host = node.account.hostname

        # TODO: configure TLS if redpanda is running with it enabled

        # Configure the client
        self.configuration = Configuration(host=f"http://{host}:8081")

        # Create API client
        self.api_client = ApiClient(self.configuration)

        # Set a global timeout for all requests. The generated Configuration doesn't allow passing
        # it, so pass it here and clear the pool to apply.
        self.api_client.rest_client.pool_manager.connection_pool_kw["timeout"] = (
            urllib3.Timeout(60)
        )
        self.api_client.rest_client.pool_manager.clear()

        # Create the API instance
        self.api = DefaultApi(self.api_client)

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - cleanup client."""
        if self.api_client:
            self.api_client.close()


__all__ = [
    "SchemaRegistryClient",
    # Models
    "ErrorBody",
    "GetCompatibility",
    "GetSchemasIdsIdVersions200ResponseInner",
    "IsCompatibile",
    "Mode",
    "PostSubjectVersions200Response",
    "PutCompatibility",
    "SchemaDef",
    "SchemaDefReferencesInner",
    "SecurityAcl",
    "StoredSchema",
]
