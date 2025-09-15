# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
import random
import threading
import time
import json
from time import sleep
from typing import List, Optional
from rptest.clients.rpk import RpkPartition, RpkTool
from rptest.services.redpanda import RedpandaService
from rptest.tests.datalake.query_engine_base import QueryEngineBase
from rptest.util import wait_until

from confluent_kafka import Consumer
from confluent_kafka import TopicPartition


class DatalakeVerifier:
    """
     Verifier that does the verification of the data in the redpanda Iceberg table.
     The verifier consumes offsets from specified topic and verifies it the data
     in the iceberg table matches.

     The verifier runs two threads:
     - one of them consumes messages from the specified topic and buffers them in memory.
       The semaphore is used to limit the number of messages buffered in memory.

     - second thread executes a per partition query that fetches the messages
       from the iceberg table

    If requested to go into offline mode the first thread is forced to stop
    early, with necessary info saved. No interactions with the cluster is
    performed after. Querying thread continues as normal. From offline mode
    there is no way back.
    """

    # TODO: add an ability to pass lambda to verify the message content
    def __init__(
        self,
        redpanda: RedpandaService,
        topic: str,
        query_engine: QueryEngineBase,
        compacted: bool = False,
        table_override: Optional[str] = None,
        max_buffered_msgs=5000,
    ):
        self.redpanda = redpanda
        self.topic = topic
        self.table = table_override or topic
        self.logger = redpanda.logger
        # Map from partition id to list of messages consumed
        # by from the partition
        self._consumed_messages = defaultdict(list)
        # Maximum offset consumed from the partition.
        # Consumed here refers to consumption by the app layer, meaning
        # there has to be a valid batch at the offset returned by the
        # kafka consume API.
        self._max_consumed_offsets = {}
        # Next position to be consumed from the partition. This may be
        # > max_consumed_offset + 1 if there are gaps from non consumable
        # batches like aborted data batches / control batches
        self._next_positions = defaultdict(lambda: -1)
        self._cg = f"verifier-group-{random.randint(0, 1000000)}"
        self._consumer: Consumer = self.create_consumer()
        self._query: QueryEngineBase = query_engine
        self._lock = threading.Lock()
        self._stop = threading.Event()
        # number of messages buffered in memory
        self._msg_semaphore = threading.Semaphore(max_buffered_msgs)
        self._num_msgs_pending_verification = 0
        # Signalled when enough messages are batched so query
        # thread can perform verification. Larger batches results
        # in fewer SQL queries and hence faster verification
        self._msgs_batched = threading.Condition()
        self._query_batch_size = 1000
        self._query_batch_wait_timeout_s = 3
        self._executor = ThreadPoolExecutor(max_workers=2)
        self._rpk = RpkTool(self.redpanda)
        # errors found during verification
        self._errors = []
        # map of last queried offset for each partition
        self._max_queried_offsets = {}
        self._last_checkpoint = {}
        self._received_first_iceberg_message = threading.Event()
        # offline mode: query only, as RP topic may be deleted or unmounted
        self._offline_mode_requested = threading.Event()
        self._consumer_stopped = threading.Event()
        self._offline_mode_established = False
        self._consumer_positions = None  # set iff in offline mode
        self._partition_hwms = None  # set iff in offline mode
        self._consumer_lock = threading.Lock()

        self._compacted = compacted
        # When consuming from a compacted topic, there may be records in the
        # Iceberg table that have since been compacted away in the log. We
        # maintain a set of compacted keys during message verification. If
        # the offset for a record read from the Iceberg table differs from
        # the offset for the record read from the log, the record's key is
        # added to the set. The key is removed from the set when a later
        # record with the same key is seen in the log. Finally, after
        # consuming, we assert that the size of this set is zero (otherwise,
        # it would imply an anomaly between the Iceberg table and the log).
        self._expected_compacted_keys = set()

    def create_consumer(self, config=None):
        if config is None:
            config = {
                "bootstrap.servers": self.redpanda.brokers(),
                "group.id": self._cg,
                "auto.offset.reset": "earliest",
            }
        # Inject SASL credentials if needed (same logic as oneshot_cloud)
        if 'security.protocol' in config and config['security.protocol'] in [
                'SASL_SSL', 'SASL_PLAINTEXT'
        ]:
            if 'sasl.username' not in config:
                config['sasl.username'] = 'admin'
            if 'sasl.password' not in config:
                config['sasl.password'] = 'admin'
        try:
            c = Consumer(config)
            c.subscribe([self.topic])
            self.logger.info(
                f"Consumer created and subscribed to topic {self.topic} with config: {config}"
            )
            return c
        except Exception as e:
            self.logger.error(f"Error creating consumer: {e}")
            raise

    def update_and_get_fetch_positions(self):
        with self._consumer_lock:
            if self._offline_mode_established:
                assert self._consumer_positions is not None
                return self._consumer_positions

            with self._lock:
                partitions = [
                    TopicPartition(topic=self.topic, partition=p)
                    for p in self._consumed_messages.keys()
                ]
                positions = self._consumer.position(partitions)
                for p in positions:
                    if p.error is not None:
                        self.logger.warning(
                            f"Error querying position for partition {p.partition}"
                        )
                    else:
                        self.logger.debug(
                            f"next position for {p.partition} is {p.offset}")
                        self._next_positions[p.partition] = p.offset
                return self._next_positions.copy()

    def partition_hwms(self) -> List[RpkPartition]:
        if self._offline_mode_established:
            assert self._partition_hwms is not None
            return self._partition_hwms
        return list(self._rpk.describe_topic(self.topic))

    # to be called no more than once
    def go_offline(self, timeout=60):
        assert not self._offline_mode_requested.is_set()
        self.logger.debug(f"offline mode requested")
        self._offline_mode_requested.set()
        assert self._consumer_stopped.wait(timeout)
        self.logger.debug(f"consistent state reached")
        self._consumer_positions = self.update_and_get_fetch_positions()
        self.logger.debug(f"remembered {self._consumer_positions=}")
        self._partition_hwms = self.partition_hwms()
        for p in self._partition_hwms:
            self.logger.debug(
                f"remembered partition {p.id=} hwm={p.high_watermark}, ")
        with self._consumer_lock:
            self._consumer.close()
            self._consumer = None
            self.logger.debug(f"offline mode established")
            self._offline_mode_established = True

    def _consumed_till_hwm(self, update: bool):
        self.logger.debug("checking _consumed_till_hwm")
        if update:
            # reduce _lock contention
            if not self._consumed_till_hwm(False):
                return False
            self.update_and_get_fetch_positions()
        for p in self.partition_hwms():
            if self._next_positions[p.id] < p.high_watermark:
                self.logger.debug(
                    f"partition {p.id} high watermark: {p.high_watermark} max offset: {self._next_positions[p.id]} has not been consumed fully"
                )
                return False
        return True

    def _consumer_thread(self):
        try:
            self.logger.info("Starting consumer thread")
            while not self._stop.is_set() and not (
                    self._offline_mode_requested.is_set()
                    and self._consumed_till_hwm(update=True)):
                self._msg_semaphore.acquire()
                if self._stop.is_set():
                    break
                msg = self._consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    self.logger.error(f"Consumer error: {msg.error()}")
                    continue

                with self._lock:
                    self._num_msgs_pending_verification += 1
                    self._consumed_messages[msg.partition()].append(msg)
                    if self._num_msgs_pending_verification >= self._query_batch_size:
                        with self._msgs_batched:
                            self._msgs_batched.notify()
                    self._max_consumed_offsets[msg.partition()] = max(
                        self._max_consumed_offsets.get(msg.partition(), -1),
                        msg.offset(),
                    )
                    self.logger.debug(
                        f"Max consumed offsets: {self._max_consumed_offsets}")
                    if len(self._errors) > 0:
                        return
        finally:
            self._consumer_stopped.set()

    def _get_query(self, partition, last_queried_offset, max_consumed_offset):
        return f"\
        SELECT redpanda.offset, redpanda.key FROM redpanda.{self._query.escape_identifier(self.table)} \
        WHERE redpanda.partition={partition} \
        AND redpanda.offset>{last_queried_offset} \
        AND redpanda.offset<={max_consumed_offset} \
        ORDER BY redpanda.offset"

    def _get_query_data(self):
        # The table schema in Databricks Unity Catalog:
        # 1. A struct column `redpanda` containing partition, offset, timestamp, headers, key
        # 2. A `value` column containing the message payload (hex encoded)
        # Note: For Databricks, we need the full catalog.namespace.table format
        if hasattr(self._query, '_catalog_name'):
            # For Databricks Unity Catalog, use full qualified name
            table_name = f"`{self._query._catalog_name}`.`redpanda`.{self._query.escape_identifier(self.table)}"
        else:
            # For other catalogs, use the default format
            table_name = f"redpanda.{self._query.escape_identifier(self.table)}"

        return f"""SELECT
                        redpanda.key,
                        value,
                        redpanda.headers
                    FROM {table_name}
                    ORDER BY redpanda.offset"""

    def _verify_next_message(self, partition, iceberg_offset, iceberg_key):
        if partition not in self._consumed_messages:
            self._errors.append(
                f"Partition {partition} returned from Iceberg query not found in consumed messages"
            )

        p_messages = self._consumed_messages[partition]

        if len(p_messages) == 0:
            return

        message = p_messages[0]
        consumer_offset = message.offset()
        if iceberg_offset > consumer_offset:
            self._errors.append(
                f"Offset from Iceberg table {iceberg_offset} is greater than next consumed offset {consumer_offset} for partition {partition}, most likely there is a gap in the table"
            )
            return

        if iceberg_offset <= self._max_queried_offsets.get(partition, -1):
            self._errors.append(
                f"Duplicate entry detected at offset {iceberg_offset} for partition {partition} "
            )
            return
        if not self._max_queried_offsets:
            self._received_first_iceberg_message.set()
        self._max_queried_offsets[partition] = iceberg_offset

        if consumer_offset != iceberg_offset:
            if self._compacted:
                self._expected_compacted_keys.add(iceberg_key)
                return
            else:
                self._errors.append(
                    f"Offset from iceberg table {iceberg_offset} for {partition} does not match the next consumed offset {consumer_offset}"
                )
                return
        else:
            if self._compacted:
                if iceberg_key in self._expected_compacted_keys:
                    self._expected_compacted_keys.remove(iceberg_key)

        self._consumed_messages[partition].pop(0)
        self._num_msgs_pending_verification -= 1
        self._msg_semaphore.release()

    def _query_thread(self):
        self.logger.info("Starting query thread")
        while not self._stop.is_set():
            try:
                with self._msgs_batched:
                    # Wait for enough data to be batched or a timeout.
                    self._msgs_batched.wait(
                        timeout=self._query_batch_wait_timeout_s)
                partitions = self.update_and_get_fetch_positions()

                for partition, next_consume_offset in partitions.items():
                    last_queried_offset = (
                        self._max_queried_offsets[partition]
                        if partition in self._max_queried_offsets else -1)

                    max_consumed = next_consume_offset - 1
                    # no new messages consumed, skip query
                    if max_consumed <= last_queried_offset:
                        continue

                    query = self._get_query(partition, last_queried_offset,
                                            max_consumed)
                    self.logger.debug(f"Executing query: {query}")

                    with self._query.run_query(query) as cursor:
                        with self._lock:
                            for row in cursor:
                                self._verify_next_message(partition, *row)
                                if len(self._errors) > 0:
                                    self.logger.error(
                                        f"violations detected: {self._errors}, stopping verifier"
                                    )
                                    return
                                self.logger.debug(
                                    f"verified message on {partition=} offset={row[0]}"
                                )

                    if len(self._max_queried_offsets) > 0:
                        self.logger.debug(
                            f"Max queried offsets: {self._max_queried_offsets}"
                        )

            except Exception as e:
                self.logger.error(f"Error querying iceberg table: {e}")
                sleep(2)

    def start(self, wait_first_iceberg_msg=False):
        self.logger.debug("Submitting consumer thread to executor")
        self._executor.submit(self._consumer_thread)
        self.logger.debug("Submitting query thread to executor")
        self._executor.submit(self._query_thread)
        if wait_first_iceberg_msg:
            self.logger.debug("Waiting for first iceberg message")
            self._received_first_iceberg_message.wait()
            self.logger.debug("Received first iceberg message")

    def _all_offsets_translated(self):
        partition_hwms = self.partition_hwms()
        with self._lock:
            if not self._consumed_till_hwm(update=False):
                return False
            for p in partition_hwms:
                if p.id not in self._max_queried_offsets:
                    self.logger.debug(
                        f"partition {p.id} not found in max offsets: {self._max_queried_offsets}"
                    )
                    return False
                # Ensure all the consumed messages are drained.
                return all(
                    len(messages) == 0
                    for messages in self._consumed_messages.values())

        return True

    def _made_progress(self):
        progress = False
        with self._lock:
            self.logger.debug(
                f"DatalakeVerifier._made_progress: Max queried offsets = {self._max_queried_offsets}"
            )
            self.logger.debug(
                f"DatalakeVerifier._made_progress: Last checkpoint = {self._last_checkpoint}"
            )

            for partition, offset in self._max_queried_offsets.items():
                if offset > self._last_checkpoint.get(partition, -1):
                    progress = True
                    self.logger.debug(
                        f"Partition {partition} has made progress: current offset {offset} > last checkpoint {self._last_checkpoint.get(partition, -1)}"
                    )
                    break
                else:
                    self.logger.debug(
                        f"Partition {partition} has not made progress: current offset {offset} <= last checkpoint {self._last_checkpoint.get(partition, -1)}"
                    )

            self._last_checkpoint = self._max_queried_offsets.copy()
            self.logger.debug(
                f"Updated last checkpoint: {self._last_checkpoint}")
        return progress

    def wait(self, progress_timeout_sec=30):
        try:
            while not self._all_offsets_translated():
                self.logger.debug(
                    f"Waiting for all offsets to be translated. Current state: _all_offsets_translated() is False"
                )
                wait_until(
                    lambda: self._made_progress(),
                    progress_timeout_sec,
                    backoff_sec=3,
                    err_msg=
                    f"Error waiting for the query to make progress for topic {self.topic}",
                )
                self.logger.debug(f"_made_progress() returned True")
                assert len(self._errors) == 0, (
                    f"Topic {self.topic} validation errors: {self._errors}")
            self.logger.debug(f"No errors around waiting")
        except Exception as e:
            self.logger.error(f"Error around waiting: {e}")
            raise
        finally:
            self.stop()

    def stop(self):
        self.logger.debug("stopping")
        try:
            self._stop.set()
            self._msg_semaphore.release()
            self._executor.shutdown(wait=False)
            assert len(self._errors) == 0, (
                f"Topic {self.topic} validation errors: {self._errors}")

            self.logger.debug(
                f"consumed offsets: {self._max_consumed_offsets}")
            self.logger.debug(f"queried offsets: {self._max_queried_offsets}")

            assert self._max_queried_offsets == self._max_consumed_offsets, (
                "Mismatch between maximum offsets in topic vs iceberg table")

            assert len(self._expected_compacted_keys) == 0, (
                f"Some keys which were compacted away were not seen later in the consumer's log"
            )
        finally:
            if self._consumer:
                self._consumer.close()

    @staticmethod
    def oneshot(
        redpanda: RedpandaService,
        topic: str,
        query_engine: QueryEngineBase,
        progress_timeout_sec=30,
    ):
        verifier = DatalakeVerifier(redpanda, topic, query_engine)
        verifier.start()
        verifier.wait(progress_timeout_sec=progress_timeout_sec)

    def verify_data(self, expected_records):
        """Verify data against expected records."""
        self.logger.info("Verifying data against expected records")
        query = self._get_query_data()

        # First, let's show a sample of what's in the table
        # Note: For Databricks, we need the full catalog.namespace.table format
        if hasattr(self._query, '_catalog_name'):
            # For Databricks Unity Catalog, use full qualified name
            table_name = f"`{self._query._catalog_name}`.`redpanda`.{self._query.escape_identifier(self.table)}"
        else:
            # For other catalogs, use the default format
            table_name = f"redpanda.{self._query.escape_identifier(self.table)}"

        sample_query = f"""SELECT
                            redpanda.key,
                            value,
                            redpanda.headers
                        FROM {table_name}
                        ORDER BY redpanda.offset
                        LIMIT 5"""

        try:
            with self._query.run_query(sample_query) as cursor:
                sample_rows = list(cursor)
                self.logger.info(
                    f"Sample of first {len(sample_rows)} rows in table:")
                for i, row in enumerate(sample_rows):
                    self.logger.debug(f"Row {i} has {len(row)} columns")
                    if len(row) < 3:
                        self.logger.warning(
                            f"  Row {i}: Malformed row with only {len(row)} columns"
                        )
                        continue
                    # Show raw hex values
                    self.logger.info(
                        f"  Row {i} RAW: key={row[0]}, value={row[1][:50]}..., headers={row[2]}"
                    )
                    # Show decoded values
                    key = self.safe_decode(row[0])
                    value = self.safe_decode(row[1])
                    headers = row[2]
                    value_preview = value[:50] + "..." if len(
                        value) > 50 else value
                    self.logger.info(
                        f"  Row {i} DECODED: key='{key}', value='{value_preview}', headers={headers}"
                    )
        except Exception as e:
            self.logger.warning(f"Could not fetch sample data: {e}")

        with self._query.run_query(query) as cursor:
            rows = list(cursor)
            self.logger.info(
                f"Query returned {len(rows)} rows from the Iceberg table.")

            # Log the structure of the first row to debug issues
            if rows:
                self.logger.debug(
                    f"First row structure: {len(rows[0])} columns")
                self.logger.debug(f"First row content: {rows[0]}")

            try:
                success, errors = self.verify_rows(rows, expected_records,
                                                   self.logger)
                return success, errors
            except IndexError as e:
                self.logger.error(f"IndexError in verify_rows: {e}")
                self.logger.error(
                    f"This usually means the query returned fewer columns than expected"
                )
                if rows:
                    self.logger.error(
                        f"First row had {len(rows[0])} columns: {rows[0]}")
                raise
            except Exception as e:
                self.logger.error(f"Unexpected error in verify_rows: {e}")
                raise

    def safe_decode(self, val):
        if val is None:
            return ""
        if isinstance(val, bytes):
            return val.decode("utf-8", errors="replace")
        if isinstance(val, memoryview):
            return val.tobytes().decode("utf-8", errors="replace")
        # Check if it's a hex string (from Databricks)
        if isinstance(val, str) and len(val) > 0 and all(
                c in '0123456789abcdefABCDEF'
                for c in val) and len(val) % 2 == 0:
            try:
                # Try to decode hex string to bytes then to utf-8
                decoded = bytes.fromhex(val).decode("utf-8", errors="replace")
                self.logger.debug(f"Decoded hex '{val}' to '{decoded}'")
                return decoded
            except Exception as e:
                self.logger.debug(f"Failed to decode hex '{val}': {e}")
                # If hex decode fails, return as is
                return val
        return str(val)

    @staticmethod
    def _json_dumps(val):
        if isinstance(val, (dict, list)):
            return json.dumps(val, sort_keys=True, indent=2)
        return str(val) if val is not None else ""

    def verify_rows(self, rows, expected_records, logger):
        logger.debug("Starting verify_rows")

        # Build lookup map of expected records for efficient comparison
        expected_lookup = {}
        for key, value, headers in expected_records:
            lookup_key = (key, self._json_dumps(value),
                          frozenset(headers.items()))
            expected_lookup[lookup_key] = (key, value, headers)
            logger.debug(
                f"Expected record: key='{key}', value='{self._json_dumps(value)}', headers={headers}"
            )

        found_keys = set()
        errors = []
        unmatched_rows = []

        for i, row in enumerate(rows):
            # Defensive check for row structure
            if len(row) < 3:
                logger.error(
                    f"Skipping malformed row #{i+1}: expected 3 columns, but got {len(row)}. Row: {row}"
                )
                errors.append(f"Malformed row found at index {i+1}")
                continue

            # row format from query: key, value, headers
            # Log raw values first
            logger.debug(
                f"Row {i} raw data: key={row[0]}, value={row[1]}, headers={row[2]}"
            )

            key_str = self.safe_decode(row[0])
            value_str = self.safe_decode(row[1])
            headers = row[2]

            header_dict = {}
            if headers is not None:
                # Handle numpy array or regular list
                try:
                    # Convert to list if it's a numpy array
                    header_list = list(headers) if hasattr(
                        headers, '__iter__') else []
                    for h in header_list:
                        if isinstance(h, dict):
                            header_key = self.safe_decode(h.get("key"))
                            header_value = self.safe_decode(h.get("value"))
                            header_dict[header_key] = header_value
                except Exception as e:
                    logger.debug(f"Error processing headers: {e}")

            logger.debug(
                f"Row {i} decoded: key='{key_str}', value='{value_str}', headers={header_dict}"
            )

            try:
                parsed_json = json.loads(value_str)
            except json.JSONDecodeError:
                parsed_json = None

            actual_val = self._json_dumps(
                parsed_json if parsed_json is not None else value_str)
            key_tuple = (key_str, actual_val, frozenset(header_dict.items()))

            logger.debug(f"Row {i} checking:\n"
                         f"  Key     : '{key_str}'\n"
                         f"  Value   : '{actual_val}'\n"
                         f"  Headers : {header_dict}")

            if key_tuple in expected_lookup:
                logger.info(f"✓ Match found for row {i} with key='{key_str}'")
                found_keys.add(key_tuple)
            else:
                logger.warning(
                    f"✗ No match for row {i}: key='{key_str}', value='{value_str}'"
                )
                unmatched_rows.append(
                    f"  - Row {i}: Key='{key_str}', Value='{value_str}', Headers={header_dict}"
                )

        # Identify and log missing records
        missing = set(expected_lookup.keys()) - found_keys
        for key in missing:
            key_str, val_str, headers = key
            original = expected_lookup[key]
            msg = (
                f"Missing record:\n"
                f"  Expected Key     : {original[0]}\n"
                f"  Expected Value   : {json.dumps(original[1], indent=2) if isinstance(original[1], dict) else original[1]}\n"
                f"  Expected Headers : {original[2]}")
            logger.error(msg)
            errors.append(msg)

        if unmatched_rows:
            logger.error(
                f"Found {len(unmatched_rows)} unexpected or mismatched rows in the table:"
            )
            for row_info in unmatched_rows:
                logger.error(row_info)
            errors.append(
                f"{len(unmatched_rows)} unexpected rows found in table but not in expected records."
            )

        if errors:
            # Provide summary
            logger.error(f"\n=== VERIFICATION SUMMARY ===")
            logger.error(f"Total expected records: {len(expected_records)}")
            logger.error(f"Total found records: {len(rows)}")
            logger.error(f"Matched records: {len(found_keys)}")
            logger.error(f"Missing records: {len(missing)}")
            logger.error(f"Unexpected records: {len(unmatched_rows)}")
            logger.error(f"=========================\n")
            return False, errors

        logger.info(
            "Verification successful. All expected records found and matched.")
        return True, []
