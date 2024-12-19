# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
import tempfile

from rptest.clients.rpk import RpkTool
from rptest.services.cluster import cluster
from rptest.services.redpanda import SISettings, SchemaRegistryConfig
from rptest.services.redpanda_connect import RedpandaConnectService
from rptest.tests.datalake.datalake_services import DatalakeServices
from rptest.tests.datalake.datalake_verifier import DatalakeVerifier
from rptest.tests.datalake.query_engine_base import QueryEngineType
from rptest.tests.redpanda_test import RedpandaTest
from rptest.tests.datalake.utils import supported_storage_types
from rptest.util import expect_exception
from ducktape.mark import matrix

schema_avro = """
{
    "type": "record",
    "name": "VerifierRecord",
    "fields": [
        {
            "name": "verifier_string",
            "type": "string"
        },
        {
            "name": "ordinal",
            "type": "int"
        }
    ]
}
"""

legal_evo_schema_avro = """
{
    "type": "record",
    "name": "VerifierRecord",
    "fields": [
        {
            "name": "verifier_string",
            "type": "string"
        },
        {
            "name": "ordinal",
            "type": "long"
        },
        {
            "name": "another",
            "type": "float"
        }
    ]
}
"""

legal_evo_schema_avro_2 = """
{
    "type": "record",
    "name": "VerifierRecord",
    "fields": [
        {
            "name": "verifier_string",
            "type": "string"
        },
        {
            "name": "ordinal",
            "type": "long"
        }
    ]
}
"""

illegal_evo_schema_avro = """
{
    "type": "record",
    "name": "VerifierRecord",
    "fields": [
        {
            "name": "verifier_string",
            "type": "double"
        },
        {
            "name": "ordinal",
            "type": "long"
        }
    ]
}
"""


class SchemaEvolutionTest(RedpandaTest):
    def __init__(self, test_context):
        super(SchemaEvolutionTest, self).__init__(
            test_context=test_context,
            num_brokers=1,
            si_settings=SISettings(test_context,
                                   cloud_storage_enable_remote_read=False,
                                   cloud_storage_enable_remote_write=False),
            extra_rp_conf={
                "iceberg_enabled": True,
                "iceberg_catalog_commit_interval_ms": 5000,
            },
            schema_registry_config=SchemaRegistryConfig())

    def avro_stream_config(self, topic, subject, mapping: str):
        return {
            "input": {
                "generate": {
                    "mapping": mapping,
                    "interval": "",
                    "count": 1000,
                    "batch_size": 1
                }
            },
            "pipeline": {
                "processors": [{
                    "schema_registry_encode": {
                        "url": self.redpanda.schema_reg().split(",")[0],
                        "subject": subject,
                        "refresh_period": "10s"
                    }
                }]
            },
            "output": {
                "redpanda": {
                    "seed_brokers": self.redpanda.brokers_list(),
                    "topic": topic,
                }
            }
        }

    def setUp(self):
        pass

    def _create_schema(self, subject: str, schema: str, schema_type="avro"):
        rpk = RpkTool(self.redpanda)
        with tempfile.NamedTemporaryFile(suffix=f".{schema_type}") as tf:
            tf.write(bytes(schema, 'UTF-8'))
            tf.flush()
            rpk.create_schema(subject, tf.name)

    @cluster(num_nodes=4, log_allow_list=["Schema \d+ already exists"])
    @matrix(
        cloud_storage_type=supported_storage_types(),
        query_engine=[
            QueryEngineType.SPARK,
            QueryEngineType.TRINO,
        ],
    )
    def test_evolving_avro_schemas(self, cloud_storage_type, query_engine):
        topic_name = "ducky-topic"
        with DatalakeServices(self.test_context,
                              redpanda=self.redpanda,
                              filesystem_catalog_mode=True,
                              include_query_engines=[
                                  query_engine,
                              ]) as dl:

            dl.create_iceberg_enabled_topic(
                topic_name,
                partitions=5,
                replicas=1,
                iceberg_mode="value_schema_id_prefix")

            def get_qe():
                if query_engine == QueryEngineType.SPARK:
                    return dl.spark()
                else:
                    return dl.trino()

            self._create_schema("schema_avro", schema_avro)
            self._create_schema("legal_evo_schema_avro", legal_evo_schema_avro)
            self._create_schema("legal_evo_schema_avro_2",
                                legal_evo_schema_avro_2)
            self._create_schema("illegal_evo_schema_avro",
                                illegal_evo_schema_avro)

            connect = RedpandaConnectService(self.test_context, self.redpanda)
            connect.start()

            def run_verifier(schema: str, mapping: str):

                # create verifier
                verifier = DatalakeVerifier(self.redpanda, topic_name,
                                            get_qe())
                # create a stream
                connect.start_stream(name="ducky_stream",
                                     config=self.avro_stream_config(
                                         topic_name, schema, mapping=mapping))

                verifier.start()
                connect.stop_stream("ducky_stream", wait_to_finish=True)
                verifier.wait()

            run_verifier("schema_avro",
                         mapping=f"""
                    root.verifier_string = uuid_v4()
                    root.ordinal = counter()
                    """)

            # again w/ a new schema (extra field and promoted ordinal)
            run_verifier("legal_evo_schema_avro",
                         mapping=f"""
                    root.verifier_string = uuid_v4()
                    root.ordinal = counter()
                    root.another = 12.0
                    """)

            # remove the extra field and do it one more time
            run_verifier("legal_evo_schema_avro_2",
                         mapping=f"""
                    root.verifier_string = uuid_v4()
                    root.ordinal = counter()
                    """)

            # finally once w/ a non-compatible schema, verifier should time out and fail its assertion
            with expect_exception(
                    AssertionError,
                    lambda e: "Mismatch between maximum offsets" in str(e)):
                run_verifier("illegal_evo_schema_avro",
                             mapping=f"""
                    root.verifier_string = 23.0
                    root.ordinal = counter()
                    """)
