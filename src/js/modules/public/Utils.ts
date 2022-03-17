/**
 * Copyright 2020 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

import {
  Record,
  RecordBatch,
  RecordBatchHeader,
  RecordHeader,
} from "./Coprocessor";

const createHeader = (
  header: Partial<RecordBatchHeader>
): RecordBatchHeader => {
  return {
    attrs: 0,
    baseOffset: BigInt(0),
    baseSequence: 0,
    crc: 0,
    firstTimestamp: BigInt(0),
    headerCrc: 0,
    lastOffsetDelta: 0,
    maxTimestamp: BigInt(0),
    producerEpoch: 0,
    producerId: BigInt(0),
    recordBatchType: 0,
    recordCount: 0,
    sizeBytes: 0,
    term: BigInt(0),
    isCompressed: 0,
    ...header,
  };
};

const createRecordHeader = (
  recordHeader: Partial<RecordHeader>
): RecordHeader => {
  return {
    headerKey: Buffer.from(""),
    headerKeyLength: 0,
    headerValueLength: 0,
    value: Buffer.from(""),
    ...recordHeader,
  };
};

const createRecord = (record: Partial<Record>): Record => {
  const headers = record?.headers || [];
  return {
    attributes: 0,
    key: Buffer.from(""),
    keyLength: 0,
    length: 0,
    offsetDelta: 0,
    timestampDelta: BigInt(0),
    value: Buffer.from(""),
    valueLen: 0,
    ...record,
    headers: headers.map(createRecordHeader),
  };
};

interface PartialRecordBatch {
  header?: Partial<RecordBatchHeader>;
  records?: Partial<Record>[];
}

interface RecordBatchFunctor extends RecordBatch {
  map(fn: (recordBatch) => RecordBatch): RecordBatch;
}

export const createRecordBatch = (
  record?: PartialRecordBatch
): RecordBatchFunctor => {
  const map =
    (recordBatch: RecordBatch) =>
    (fn: (recordBatch) => RecordBatch): RecordBatch => {
      return fn(recordBatch);
    };
  const records = record?.records || [];
  const resultBatch = {
    header: createHeader(record?.header || {}),
    records: records.map(createRecord),
  };
  return {
    ...resultBatch,
    map: map(resultBatch),
  };
};

export const createRecordBatchFunctor = (
  recordBatch: RecordBatch
): RecordBatchFunctor => {
  const map = (fn: (recordBatch) => RecordBatch): RecordBatch => {
    return fn(recordBatch);
  };
  return { ...recordBatch, map };
};

export const copyRecordBatch = (
  recordBatch: RecordBatch
): RecordBatchFunctor => {
  const deepCopiedRecords = recordBatch.records.map((record) => {
    const deepCopiedRecordHeaders = record.headers.map((header) => {
      const newKey = Buffer.alloc(header.headerKey.length);
      const newVal = Buffer.alloc(header.value.length);
      header.headerKey.copy(newKey);
      header.value.copy(newVal);
      return {
        ...header,
        headerKey: newKey,
        value: newVal,
      };
    });
    const newKey = Buffer.alloc(record.key.length);
    const newVal = Buffer.alloc(record.value.length);
    record.key.copy(newKey);
    record.value.copy(newVal);
    return {
      ...record,
      key: newKey,
      value: newVal,
      headers: deepCopiedRecordHeaders,
    };
  });
  const newBatch = {
    header: recordBatch.header,
    records: deepCopiedRecords,
  };
  return createRecordBatchFunctor(newBatch);
};

// receive int64 and return Uint64
const encodeZigzag = (field: bigint): bigint => {
  // Create Bigint with 64 bytes length and sign 63
  const digits = BigInt.asUintN(64, BigInt(63));
  // Create Bigint with 64 bytes length and sign 1
  const lsb = BigInt.asUintN(64, BigInt(1));
  return BigInt.asUintN(64, (field << lsb) ^ (field >> digits));
};

// given a number, it returns number bytes size on varint encode format
export const varintZigzagSize = (field: bigint): number => {
  let value = encodeZigzag(field);
  let size = 1;
  while (value >= 128) {
    value >>= BigInt(7);
    size += 1;
  }
  return size;
};

export const calculateRecordLength = (record: Record): number => {
  let size = 0;
  size += varintZigzagSize(BigInt(record.attributes));
  size += varintZigzagSize(BigInt(record.timestampDelta));
  size += varintZigzagSize(BigInt(record.offsetDelta));
  size += varintZigzagSize(BigInt(record.keyLength));
  size += record.key.length;
  size += varintZigzagSize(BigInt(record.valueLen));
  size += record.value.length;
  size += varintZigzagSize(BigInt(record.headers.length));
  size += record.headers.reduceRight<number>((totalHeaderSize, header) => {
    totalHeaderSize += varintZigzagSize(BigInt(header.headerKeyLength));
    totalHeaderSize += varintZigzagSize(BigInt(header.headerValueLength));
    totalHeaderSize += header.value.length;
    totalHeaderSize += header.headerKey.length;
    return totalHeaderSize;
  }, 0);
  return size;
};

export const calculateRecordBatchSize = (records: Record[]): number => {
  const headerBytesSize = 57;
  const arrayBytesSize = 4;
  return (
    headerBytesSize +
    records.reduce(
      (p, r) => p + r.length + varintZigzagSize(BigInt(r.length)),
      0
    ) +
    arrayBytesSize
  );
};
