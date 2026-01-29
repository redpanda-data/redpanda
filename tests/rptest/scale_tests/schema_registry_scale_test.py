# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import json
import requests

from ducktape.mark import parametrize
from ducktape.tests.test import TestContext
from rptest.services.cluster import cluster
from rptest.services.redpanda import SchemaRegistryConfig
from rptest.tests.schema_registry_test import (
    SchemaRegistryEndpoints,
    create_topic_names,
    schema1_def,
)
from rptest.utils.mode_checks import skip_debug_mode

from typing import Any


class SchemaRegistryScaleTest(SchemaRegistryEndpoints):
    """
    Test class for schema registry scaling tests.
    """

    def __init__(self, context: TestContext, **kwargs: Any) -> None:
        super(SchemaRegistryScaleTest, self).__init__(context, **kwargs)

    @cluster(num_nodes=1)
    @skip_debug_mode
    @parametrize(iterations=4097)
    def test_post_subjects_subject_versions_and_delete_repeated(self, iterations: int):
        """
        Verify posting a schema and deleting it many times to trigger
        oversized allocation warnings in subject_entry::written_at and
        subject_entry::versions.
        """

        topic = create_topic_names(1)[0]
        subject = f"{topic}-key"
        schema_1_data = json.dumps({"schema": schema1_def})

        for _ in range(iterations):
            result_raw = self.sr_client.post_subjects_subject_versions(
                subject=subject, data=schema_1_data
            )
            self.logger.debug(result_raw)
            self.logger.debug(result_raw.json())
            assert result_raw.status_code == requests.codes.ok
            assert result_raw.json()["id"] == 1

            result_raw = self.sr_client.delete_subject_version(
                subject=subject, version="latest"
            )
            self.logger.debug(result_raw)
            assert result_raw.status_code == requests.codes.ok

    @cluster(num_nodes=1)
    @skip_debug_mode
    @parametrize(iterations=32769)
    def test_post_subjects_subject_versions_unique(self, iterations: int):
        """
        Verify repeatedly posting a unique version of a schema to trigger
        oversized allocation warnings in store::get_version_ids and
        store::get_versions.
        """

        topic = create_topic_names(1)[0]
        subject = f"{topic}-key"
        schema_1_dict = json.loads(schema1_def)

        for i in range(iterations):
            # Change the doc field to make the schema unique
            schema_1_dict["doc"] = str(i)
            schema_1_data = json.dumps({"schema": json.dumps(schema_1_dict)})
            result_raw = self.sr_client.post_subjects_subject_versions(
                subject=subject, data=schema_1_data
            )

            self.logger.debug(result_raw)
            self.logger.debug(result_raw.json())
            assert result_raw.status_code == requests.codes.ok
            assert result_raw.json()["id"] == i + 1

        result_raw = self.sr_client.get_subjects_subject_versions(subject=subject)
        self.logger.debug(result_raw.json())
        assert result_raw.status_code == requests.codes.ok
        assert len(result_raw.json()) == iterations

        self.logger.debug("Deleting the subject")
        result_raw = self.sr_client.delete_subject(subject=subject)
        self.logger.debug(result_raw.json())
        assert result_raw.status_code == requests.codes.ok

    @cluster(num_nodes=1)
    @skip_debug_mode
    @parametrize(iterations=4097)
    def test_set_subject_config_repeated(self, iterations: int):
        """
        Verify repeatedly setting subject config and then deleting it to trigger
        oversized allocation warnings in store::get_subject_config_written_at.
        """

        topic = create_topic_names(1)[0]
        subject = f"{topic}-key"

        compatibilities = ["NONE", "BACKWARD"]
        compatibility = compatibilities[0]

        for i in range(iterations):
            compatibility = compatibilities[i % len(compatibilities)]
            result_raw = self.sr_client.set_config_subject(
                subject=subject,
                data=json.dumps({"compatibility": compatibility}),
            )
            assert result_raw.status_code == requests.codes.ok
            assert result_raw.json()["compatibility"] == compatibility

        result_raw = self.sr_client.delete_config_subject(subject=subject)
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json()["compatibilityLevel"] == compatibility

    @cluster(num_nodes=1)
    @skip_debug_mode
    @parametrize(iterations=32769)
    def test_many_references(self, iterations: int):
        """
        Create many schemas referencing the same base schema to trigger
        oversized allocation warnings in sharded_store::referenced_by.
        """

        topic = create_topic_names(1)[0]
        base_subject = f"{topic}-key"
        base_version = 1
        schema_dict = json.loads(schema1_def)

        schema_data = json.dumps({"schema": json.dumps(schema_dict)})
        result_raw = self.sr_client.post_subjects_subject_versions(
            subject=base_subject, data=schema_data
        )

        assert result_raw.status_code == requests.codes.ok

        for i in range(iterations):
            # Change the doc field to make the schema unique
            schema_dict["doc"] = str(i)
            schema_data = json.dumps(
                {
                    "schema": json.dumps(schema_dict),
                    "references": [
                        {
                            "name": "testRef",
                            "subject": base_subject,
                            "version": base_version,
                        }
                    ],
                }
            )
            result_raw = self.sr_client.post_subjects_subject_versions(
                subject=base_subject, data=schema_data
            )

            assert result_raw.status_code == requests.codes.ok

        result_raw = self.sr_client.get_subjects_subject_versions_version_referenced_by(
            subject=base_subject, version=str(base_version)
        )
        assert result_raw.status_code == requests.codes.ok
        assert len(result_raw.json()) == iterations


class SchemaRegistryModeMutableScaleTest(SchemaRegistryEndpoints):
    """
    Test class for schema registry subject mode scaling tests.
    """

    def __init__(self, context: TestContext, **kwargs: Any) -> None:
        self.schema_registry_config = SchemaRegistryConfig()
        self.schema_registry_config.mode_mutability = True
        super(SchemaRegistryModeMutableScaleTest, self).__init__(
            context, schema_registry_config=self.schema_registry_config, **kwargs
        )

    @cluster(num_nodes=1)
    @skip_debug_mode
    @parametrize(iterations=4097)
    def test_set_subject_mode_repeated(self, iterations: int):
        """
        Verify repeatedly setting subject mode and then deleting it to trigger
        oversized allocation warnings in store::get_subject_mode_written_at.
        """

        topic = create_topic_names(1)[0]
        subject = f"{topic}-key"

        modes = ["READWRITE", "READONLY"]
        mode = modes[0]

        for i in range(iterations):
            mode = modes[i % len(modes)]
            result_raw = self.sr_client.set_mode_subject(
                subject=subject,
                data=json.dumps({"mode": mode}),
            )
            assert result_raw.status_code == requests.codes.ok
            assert result_raw.json()["mode"] == mode

        result_raw = self.sr_client.delete_mode_subject(subject=subject)
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json()["mode"] == mode
