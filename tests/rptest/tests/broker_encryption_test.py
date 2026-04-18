# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import base64
import json
import random
import time
from typing import Any

import requests
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.keywrap import aes_key_unwrap
from ducktape.mark import ignore  # type: ignore[reportUnknownVariableType]
from ducktape.tests.test import TestContext

from rptest.clients.rpk import RpkTool
from rptest.services.cluster import cluster
from rptest.services.redpanda import (
    PandaproxyConfig,
    SchemaRegistryConfig,
)
from rptest.tests.redpanda_test import RedpandaTest

# Fixed 256-bit wrapping key used by the mock KMS provider.
# Must match the key in src/v/encryption/mock_kms_provider.cc.
MOCK_WRAPPING_KEY = bytes(range(0x00, 0x20))

# Avro schema for a record containing a PII field (SSN) tagged for encryption
# via encryption:kek_name and encryption:kms_key_id field annotations.
# The broker parses these annotations from the schema text to determine which
# fields to encrypt and which KEK to use.
AVRO_SCHEMA_WITH_PII = json.dumps(
    {
        "type": "record",
        "name": "UserRecord",
        "fields": [
            {"name": "user_id", "type": "string"},
            {"name": "name", "type": "string"},
            {
                "name": "ssn",
                "type": "string",
                "encryption:kek_name": "test-kek",
                "encryption:kms_key_id": "test-key-id",
            },
        ],
    }
)

# Schema registration payload. The encryption annotations are embedded in the
# schema text itself — the schema registry stores them as-is.
SCHEMA_WITH_ENCRYPT_RULE = json.dumps(
    {
        "schema": AVRO_SCHEMA_WITH_PII,
        "schemaType": "AVRO",
    }
)

# Plain Avro schema with no encryption rules for the passthrough test.
AVRO_SCHEMA_NO_RULES = json.dumps(
    {
        "type": "record",
        "name": "PlainRecord",
        "fields": [
            {"name": "user_id", "type": "string"},
            {"name": "payload", "type": "string"},
        ],
    }
)

SCHEMA_WITHOUT_ENCRYPT_RULE = json.dumps(
    {
        "schema": AVRO_SCHEMA_NO_RULES,
        "schemaType": "AVRO",
    }
)

# JSON Schema with an encrypted PII field for the decryption test.
JSON_SCHEMA_WITH_PII = json.dumps(
    {
        "type": "object",
        "properties": {
            "id": {"type": "integer"},
            "name": {"type": "string"},
            "secret": {
                "type": "string",
                "encryption:kek_name": "test-kek",
                "encryption:kms_key_id": "test-key-id",
            },
        },
        "required": ["id", "name", "secret"],
    }
)

JSON_SCHEMA_WITH_PII_PAYLOAD = json.dumps(
    {
        "schema": JSON_SCHEMA_WITH_PII,
        "schemaType": "JSON",
    }
)

SR_POST_HEADERS = {
    "Accept": "application/vnd.schemaregistry.v1+json",
    "Content-Type": "application/vnd.schemaregistry.v1+json",
}


class BrokerEncryptionTest(RedpandaTest):
    """End-to-end broker-side field-level encryption (BSFLE) test.

    These tests verify that the broker transparently encrypts PII-tagged
    fields on produce and that the resulting records carry the expected
    encryption metadata headers.
    """

    def __init__(self, test_context: TestContext):
        # Enable encryption at startup so the encryption_service initializes
        # with mock KMS. Topics without encryption annotations still pass
        # through unchanged (the schema resolver returns nullopt).
        extra_rp_conf = {
            "encryption_kms_type": "mock",
        }
        super().__init__(
            test_context,
            num_brokers=3,
            extra_rp_conf=extra_rp_conf,
            schema_registry_config=SchemaRegistryConfig(),
            pandaproxy_config=PandaproxyConfig(),
        )
        self.rpk = RpkTool(self.redpanda)

    def _sr_base_url(self) -> str:
        """Return the schema registry base URL for the first node."""
        hostname = self.redpanda.nodes[0].account.hostname
        return f"http://{hostname}:8081"

    def _register_schema(self, subject: str, schema_data: str) -> int:
        """Register a schema via the schema registry REST API.

        Returns the schema ID assigned by the registry.
        """
        url = f"{self._sr_base_url()}/subjects/{subject}/versions"
        resp = requests.post(url, data=schema_data, headers=SR_POST_HEADERS, timeout=60)
        assert resp.status_code == 200, (
            f"Schema registration failed: {resp.status_code} {resp.text}"
        )
        return resp.json()["id"]

    def _produce_pii_records(self, topic: str, count: int, schema_id: int) -> None:
        """Produce records matching the UserRecord schema (with PII)."""
        for i in range(count):
            value = json.dumps(
                {
                    "user_id": f"u{i}",
                    "name": f"User {i}",
                    "ssn": f"123-45-{i:04d}",
                }
            )
            self.rpk.produce(topic, key=f"user-{i}", msg=value, schema_id=schema_id)

    def _produce_plain_records(self, topic: str, count: int, schema_id: int) -> None:
        """Produce records matching the PlainRecord schema (no PII)."""
        for i in range(count):
            value = json.dumps(
                {
                    "user_id": f"u{i}",
                    "payload": f"plaintext-{i}",
                }
            )
            self.rpk.produce(topic, key=f"user-{i}", msg=value, schema_id=schema_id)

    def _consume_records(self, topic: str, count: int) -> list[dict[str, Any]]:
        """Consume records using rpk one at a time to get valid JSON.

        rpk consume outputs one JSON object per record when consuming
        a single record at a time (offset="N:N+1").
        """
        records: list[dict[str, Any]] = []
        for i in range(count):
            output = self.rpk.consume(topic, offset=f"{i}:{i + 1}")
            output = output.strip()
            if output:
                parsed: dict[str, Any] = json.loads(output)
                records.append(parsed)
        return records

    def _has_encryption_header(self, record: dict[str, Any]) -> bool:
        """Check whether a consumed record carries the rp.encryption header."""
        headers: list[dict[str, Any]] = record.get("headers", [])
        return any(h.get("key") == "rp.encryption" for h in headers)

    def _wait_for_schema_on_all_nodes(self, subject: str, timeout: int = 60) -> None:
        """Wait until the schema subject is available on every node's schema registry.

        The broker's encryption resolver reads from the local sharded_store
        which is populated by consuming the _schemas internal topic. This
        can take several seconds after schema registration.
        """
        deadline = time.time() + timeout
        while time.time() < deadline:
            all_ok = True
            for node in self.redpanda.nodes:
                hostname = node.account.hostname
                url = f"http://{hostname}:8081/subjects/{subject}/versions/latest"
                try:
                    resp = requests.get(url, timeout=5)
                    if resp.status_code != 200:
                        all_ok = False
                        break
                except Exception:
                    all_ok = False
                    break
            if all_ok:
                return
            time.sleep(1)
        raise TimeoutError(
            f"Schema subject '{subject}' not available on all nodes within {timeout}s"
        )

    @cluster(num_nodes=3)
    def test_encrypted_produce_consume(self):
        """Produce plaintext records to a topic with an ENCRYPT rule and
        verify that the SSN field is encrypted in the stored records.

        Steps:
          1. Enable encryption via cluster config
          2. Create a topic
          3. Register an Avro schema with a PII tag and ENCRYPT rule
          4. Produce 100 records with plaintext SSN values
          5. Consume all 100 records
          6. Verify each record carries the rp.encryption header
          7. Verify the SSN field value is NOT the original plaintext
        """
        topic = "bsfle-test-encrypted"
        self.rpk.create_topic(topic, partitions=1, replicas=3)

        schema_id = self._register_schema(
            subject=f"{topic}-value",
            schema_data=SCHEMA_WITH_ENCRYPT_RULE,
        )

        # Wait for the schema to be available on all nodes. The schema
        # registry replicates via the _schemas internal topic, and the
        # broker's encryption resolver reads from the local store. Poll
        # every node's schema registry until the subject is available.
        self._wait_for_schema_on_all_nodes(f"{topic}-value", timeout=60)

        record_count = 100
        self._produce_pii_records(topic, record_count, schema_id=schema_id)

        records = self._consume_records(topic, record_count)
        assert len(records) == record_count, (
            f"Expected {record_count} records, got {len(records)}"
        )

        for i, record in enumerate(records):
            assert self._has_encryption_header(record), (
                f"record {i}: missing rp.encryption header"
            )

            # The SSN field must not appear as the original plaintext
            # in the raw value bytes.
            original_ssn = f"123-45-{i:04d}"
            raw_value: str = record.get("value", "")
            assert original_ssn not in raw_value, (
                f"record {i}: SSN field was not encrypted, "
                f"found plaintext '{original_ssn}' in value"
            )

    @cluster(num_nodes=3)
    @ignore  # DEK rotation depends on timing behavior not yet testable
    def test_dek_rotation(self):
        """Produce records, wait for DEK expiry, produce again, and verify
        that the two batches use different DEK versions.

        Steps:
          1. Create a topic with a short DEK expiry (e.g. 5 seconds)
          2. Produce batch A (50 records)
          3. Sleep past the DEK expiry window
          4. Produce batch B (50 records)
          5. Consume all 100 records
          6. Extract dek_version from the rp.encryption header
          7. Verify batch A and batch B have different dek_version values
        """
        topic = "bsfle-test-dek-rotation"
        self.rpk.create_topic(topic, partitions=1, replicas=3)

        schema_id = self._register_schema(
            subject=f"{topic}-value",
            schema_data=SCHEMA_WITH_ENCRYPT_RULE,
        )

        batch_a_count = 50
        self._produce_pii_records(topic, batch_a_count, schema_id=schema_id)

        # Sleep past DEK expiry. The actual expiry is configured via
        # cluster config once the integration is wired up; for this
        # skeleton we assume a short TTL.
        dek_expiry_seconds = 5
        time.sleep(dek_expiry_seconds + 2)

        batch_b_count = 50
        self._produce_pii_records(topic, batch_b_count, schema_id=schema_id)

        total = batch_a_count + batch_b_count
        records = self._consume_records(topic, total)
        assert len(records) == total

        # TODO: parse the rp.encryption header to extract dek_version
        # and verify that batch A versions differ from batch B versions.

    @cluster(num_nodes=3)
    def test_no_encryption_passthrough(self):
        """Produce records to a topic whose schema has no ENCRYPT rule and
        verify that records pass through without modification.

        Steps:
          1. Create a topic (encryption_kms_type is NOT set)
          2. Register an Avro schema WITHOUT encryption rules
          3. Produce 50 records
          4. Consume all 50 records using schema registry decoding
          5. Verify NO rp.encryption header is present
          6. Verify all field values match the original plaintext
        """
        topic = "bsfle-test-passthrough"
        self.rpk.create_topic(topic, partitions=1, replicas=3)

        schema_id = self._register_schema(
            subject=f"{topic}-value",
            schema_data=SCHEMA_WITHOUT_ENCRYPT_RULE,
        )

        record_count = 50
        self._produce_plain_records(topic, record_count, schema_id=schema_id)

        # Consume with schema registry decoding so the value is returned
        # as readable JSON rather than raw Avro bytes.
        records: list[dict[str, Any]] = []
        for i in range(record_count):
            output = self.rpk.consume(
                topic, offset=f"{i}:{i + 1}", use_schema_registry="value"
            )
            output = output.strip()
            if output:
                parsed: dict[str, Any] = json.loads(output)
                records.append(parsed)

        assert len(records) == record_count, (
            f"Expected {record_count} records, got {len(records)}"
        )

        for i, record in enumerate(records):
            assert not self._has_encryption_header(record), (
                f"record {i}: unexpected rp.encryption header "
                f"on topic without encryption rules"
            )

            # The original field values should be present in the decoded
            # value.
            value_str: str = record.get("value", "")
            expected_payload = f"plaintext-{i}"
            assert expected_payload in value_str, (
                f"record {i}: expected plaintext '{expected_payload}' "
                f"in value but it was not found; got: {value_str}"
            )

    @staticmethod
    def _parse_encryption_metadata(raw_header_value: bytes) -> dict[str, Any]:
        """Parse the rp.encryption Protobuf header value.

        Manual proto3 wire format parser for EncryptionMetadata:
          message EncryptionMetadata { repeated DekEntry deks = 1; }
          message DekEntry {
            string kek_name = 1; string kms_type = 2;
            string kms_key_id = 3; bytes encrypted_dek = 4;
            string algorithm = 5; uint32 dek_version = 6;
          }
        """

        def read_varint(data: bytes, pos: int) -> tuple[int, int]:
            result = 0
            shift = 0
            while pos < len(data):
                b = data[pos]
                result |= (b & 0x7F) << shift
                pos += 1
                if (b & 0x80) == 0:
                    return result, pos
                shift += 7
            raise ValueError("truncated varint")

        def parse_dek_entry(data: bytes) -> dict[str, Any]:
            entry: dict[str, Any] = {
                "kek_name": "",
                "kms_type": "",
                "kms_key_id": "",
                "encrypted_dek": b"",
                "algorithm": "",
                "dek_version": 0,
            }
            pos = 0
            while pos < len(data):
                tag, pos = read_varint(data, pos)
                field_number = tag >> 3
                wire_type = tag & 0x07
                if wire_type == 2:  # length-delimited (string/bytes)
                    length, pos = read_varint(data, pos)
                    field_data = data[pos : pos + length]
                    pos += length
                    if field_number == 1:
                        entry["kek_name"] = field_data.decode("utf-8")
                    elif field_number == 2:
                        entry["kms_type"] = field_data.decode("utf-8")
                    elif field_number == 3:
                        entry["kms_key_id"] = field_data.decode("utf-8")
                    elif field_number == 4:
                        entry["encrypted_dek"] = bytes(field_data)
                    elif field_number == 5:
                        entry["algorithm"] = field_data.decode("utf-8")
                elif wire_type == 0:  # varint
                    value, pos = read_varint(data, pos)
                    if field_number == 6:
                        entry["dek_version"] = value
                else:
                    raise ValueError(f"unexpected wire type {wire_type}")
            return entry

        result: dict[str, Any] = {"deks": []}
        pos = 0
        while pos < len(raw_header_value):
            tag, pos = read_varint(raw_header_value, pos)
            field_number = tag >> 3
            wire_type = tag & 0x07
            if field_number == 1 and wire_type == 2:
                length, pos = read_varint(raw_header_value, pos)
                entry_data = raw_header_value[pos : pos + length]
                pos += length
                result["deks"].append(parse_dek_entry(entry_data))
            else:
                raise ValueError(f"unexpected field {field_number}/{wire_type}")
        return result

    @staticmethod
    def _unwrap_dek(encrypted_dek: bytes) -> bytes:
        """Unwrap (decrypt) a DEK using the mock KMS fixed wrapping key.

        Uses AES key unwrap (RFC 3394).
        """
        return aes_key_unwrap(MOCK_WRAPPING_KEY, encrypted_dek)

    @staticmethod
    def _decrypt_field(dek: bytes, ciphertext: bytes) -> str:
        """Decrypt a field value encrypted with AES-256-GCM.

        Ciphertext format: [12-byte IV | encrypted data | 16-byte auth tag]
        """
        iv = ciphertext[:12]
        # AES-GCM: the ciphertext includes the tag at the end
        encrypted_with_tag = ciphertext[12:]
        aesgcm = AESGCM(dek)
        plaintext = aesgcm.decrypt(iv, encrypted_with_tag, None)
        return plaintext.decode("utf-8")

    def _consume_raw_record(
        self, topic: str, offset: int
    ) -> tuple[bytes | None, bytes | None]:
        """Consume a single record using confluent_kafka for raw binary access.

        Returns (encryption_header_value, record_value) as raw bytes.
        confluent_kafka gives proper binary access to headers unlike rpk's
        JSON output which mangles binary data.
        """
        from confluent_kafka import Consumer, TopicPartition

        conf = {
            "bootstrap.servers": self.redpanda.brokers(),
            "group.id": f"bsfle-test-{random.randint(0, 999999)}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": "false",
        }
        consumer = Consumer(conf)
        tp = TopicPartition(topic, 0, offset)
        consumer.assign([tp])

        enc_header: bytes | None = None
        value: bytes | None = None

        msg = consumer.poll(timeout=10.0)
        if msg is not None and not msg.error():
            value = msg.value()
            hdrs = msg.headers() or []
            for key, val in hdrs:
                if key == "rp.encryption" and val and len(val) > 0:
                    enc_header = val
                    break

        consumer.close()
        return enc_header, value

    @cluster(num_nodes=3)
    def test_json_encrypt_decrypt_random_access(self):
        """Produce JSON records with an encrypted PII field, then consume
        them in random order and verify actual decryption works.

        This test exercises the full encryption/decryption pipeline:
          1. Create topic + register JSON Schema with encryption annotation
          2. Produce 200 records where the 'secret' field is deterministic
             (message-{offset})
          3. Consume records one at a time in random offset order
          4. For each record: parse rp.encryption header, unwrap the DEK
             using the mock KMS key, decrypt the 'secret' field with
             AES-256-GCM, and verify the plaintext matches the expected
             value for that offset
        """
        topic = "bsfle-json-decrypt"
        self.rpk.create_topic(topic, partitions=1, replicas=3)

        schema_id = self._register_schema(
            subject=f"{topic}-value",
            schema_data=JSON_SCHEMA_WITH_PII_PAYLOAD,
        )

        self._wait_for_schema_on_all_nodes(f"{topic}-value", timeout=60)

        # Produce 200 records. The 'secret' field value is deterministic
        # based on the record index so we can verify after decryption.
        record_count = 200
        for i in range(record_count):
            value = json.dumps(
                {
                    "id": i,
                    "name": f"user-{i}",
                    "secret": f"message-{i}",
                }
            )
            self.rpk.produce(topic, key=f"key-{i}", msg=value, schema_id=schema_id)

        # Consume in random order and decrypt each record.
        offsets = list(range(record_count))
        random.shuffle(offsets)

        # Cache unwrapped DEKs to avoid repeated unwrap (mirrors consumer
        # behavior).
        dek_cache: dict[bytes, bytes] = {}

        for offset in offsets:
            enc_header, raw_value = self._consume_raw_record(topic, offset)

            # 1. Parse the rp.encryption header (raw binary via confluent_kafka)
            assert enc_header is not None and len(enc_header) > 0, (
                f"offset {offset}: missing or empty rp.encryption header"
            )

            metadata = self._parse_encryption_metadata(enc_header)
            assert len(metadata["deks"]) > 0, (
                f"offset {offset}: no DEK entries in header"
            )

            dek_entry = metadata["deks"][0]
            encrypted_dek = dek_entry["encrypted_dek"]

            # 2. Unwrap the DEK (cache it)
            if encrypted_dek not in dek_cache:
                dek_cache[encrypted_dek] = self._unwrap_dek(encrypted_dek)
            plaintext_dek = dek_cache[encrypted_dek]

            # 3. Parse the record value. It has a 5-byte schema registry
            #    prefix (magic byte + 4-byte schema ID), then JSON.
            assert raw_value is not None, f"offset {offset}: no record value"
            json_bytes = raw_value[5:]  # skip schema prefix
            parsed_value: dict[str, Any] = json.loads(json_bytes)

            # 4. Verify plaintext is NOT present
            expected_plaintext = f"message-{offset}"
            encrypted_secret_b64: str = parsed_value.get("secret", "")
            assert encrypted_secret_b64 != expected_plaintext, (
                f"offset {offset}: secret field contains plaintext"
            )

            # 5. Decode the base64-encoded ciphertext and decrypt
            ciphertext = base64.b64decode(encrypted_secret_b64)
            decrypted = self._decrypt_field(plaintext_dek, ciphertext)
            assert decrypted == expected_plaintext, (
                f"offset {offset}: decrypted '{decrypted}' "
                f"!= expected '{expected_plaintext}'"
            )
