/**
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

/**
 * Policy Error define what does the coprocessor function
 * do when the apply function into Coprocessor class fail.
 * SkipOnFailure: the current record is lose but the function
 *                can run the next record
 * Deregister: the coprocessor function will deregister from
 *             the function batch, it won't apply to any next records
 */
import { Logger } from "winston";

export enum PolicyError {
  SkipOnFailure,
  Deregister,
}

export enum PolicyInjection {
  Earliest = 0,
  Stored,
  Latest = 2,
}

interface RecordHeader {
  headerKeyLength: number;
  headerKey: Buffer;
  headerValueLength: number;
  value: Buffer;
}

interface RecordBatchHeader {
  headerCrc: number;
  sizeBytes: number;
  baseOffset: bigint;
  recordBatchType: number;
  crc: number;
  attrs: number;
  lastOffsetDelta: number;
  firstTimestamp: bigint;
  maxTimestamp: bigint;
  producerId: bigint;
  producerEpoch: number;
  baseSequence: number;
  recordCount: number;
  term: bigint;
  isCompressed: number;
}

interface Record {
  length: number;
  attributes: number;
  timestampDelta: bigint;
  offsetDelta: number;
  keyLength: number;
  key: Buffer;
  valueLen: number;
  value: Buffer;
  headers: Array<RecordHeader>;
}

interface RecordBatch {
  records: Record[];
  header: RecordBatchHeader;
}

type Topic = string;

interface Coprocessor {
  inputTopics: [string, PolicyInjection][];
  policyError: PolicyError;
  globalId: bigint;
  apply: (
    record: RecordBatch,
    logger?: Logger
  ) => Promise<Map<Topic, RecordBatch>>;
}

export { RecordBatchHeader, RecordHeader, Record, RecordBatch, Coprocessor };
