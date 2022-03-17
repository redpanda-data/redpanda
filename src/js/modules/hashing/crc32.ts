/**
 * Copyright 2020 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

import { calculate } from "fast-crc32c/impls/js_crc32c";
import { RecordBatchHeader, Record, RecordBatch } from "../rpc/types";

//The byte indexes used by crcRecordbatchheaderinternal function
enum rbhiId {
  //each field in this number is the corresponding type size
  totalSize = 4 + 8 + 1 + 4 + 2 + 4 + 8 + 8 + 8 + 2 + 4 + 4,
  sizeBytes = 0,
  baseOffset = 4,
  recordBatchType = 12,
  crc = 13,
  attributes = 17,
  lastOffsetDelta = 19,
  firstTimestamp = 23,
  maxTimestamp = 31,
  producerId = 39,
  producerEpoch = 47,
  baseSequence = 49,
  recordCount = 53,
}

//The byte indexes used by crcRecordbatcHeader function
enum rbhId {
  //each field in this number is the corresponding type size
  totalSize = 2 + 4 + 8 + 8 + 8 + 2 + 4 + 4,
  attributes = 0,
  lastOffsetDelta = 2,
  firstTimestamp = 6,
  maxTimestamp = 14,
  producerId = 22,
  producerEpoch = 30,
  baseSequence = 32,
  recordCount = 36,
}

export function RpcHeaderCrc32(header: Buffer) {
  //Byte at pos 5 is where the crc starts
  const begin = 5;
  const init = 0;
  return calculate(header.subarray(begin), init);
}

//precondition buffer.length >= 8
function toLe64(val: bigint, buff: Buffer, begin: number) {
  buff.writeBigInt64LE(val, begin);
}

//precondition buffer.length >= 8
function toLeU64(val: bigint, buff: Buffer, begin: number) {
  buff.writeBigUInt64LE(val, begin);
}

//precondition buffer.length >= 4
function toLe32(val: number, buff: Buffer, begin: number) {
  buff.writeInt32LE(val, begin);
}

//precondition buffer.length >= 4
function toLeU32(val: number, buff: Buffer, begin: number) {
  buff.writeUInt32LE(val, begin);
}

//precondition buffer.length >= 2
function toLe16(val: number, buff: Buffer, begin: number) {
  buff.writeInt16LE(val, begin);
}

//precondition buffer.length >= 2
function toLeU16(val: number, buff: Buffer, begin: number) {
  buff.writeUInt16LE(val, begin);
}

//precondition buffer.length >= 1
function toLe8(val: number, buff: Buffer, begin: number) {
  buff.writeInt8(val, begin);
}

export function crcRecordBatchHeaderInternal(header: RecordBatchHeader) {
  let buff = Buffer.allocUnsafe(rbhiId.totalSize);
  toLe32(header.sizeBytes, buff, rbhiId.sizeBytes);
  toLe64(header.baseOffset, buff, rbhiId.baseOffset);
  toLe8(header.recordBatchType, buff, rbhiId.recordBatchType);
  toLe32(header.crc, buff, rbhiId.crc);
  toLeU16(header.attributes, buff, rbhiId.attributes);
  toLe32(header.lastOffsetDelta, buff, rbhiId.lastOffsetDelta);
  toLe64(header.firstTimestamp, buff, rbhiId.firstTimestamp);
  toLe64(header.maxTimestamp, buff, rbhiId.maxTimestamp);
  toLe64(header.producerId, buff, rbhiId.producerId);
  toLe16(header.producerEpoch, buff, rbhiId.producerEpoch);
  toLe32(header.baseSequence, buff, rbhiId.baseSequence);
  toLeU32(header.recordCount, buff, rbhiId.recordCount);
  return calculate(buff, 0);
}

function encodeZigZag64(n: bigint) {
  const digits = BigInt.asIntN(64, BigInt("63"));
  const lsb = BigInt.asUintN(64, BigInt("1"));
  return BigInt.asUintN(
    64,
    (BigInt.asUintN(64, n) << lsb) ^ BigInt.asUintN(64, n >> digits)
  );
}

function crcExtendVint(value: bigint, crc: number) {
  let encode = encodeZigZag64(value);
  const moreBytes = BigInt.asUintN(64, BigInt(128));
  const buffSz = 8 + 1; //bytes //last value of encoding
  let buff = Buffer.allocUnsafe(buffSz);
  let size = 0;
  let shift = BigInt.asUintN(64, BigInt(7));
  let proxyBuff = Buffer.allocUnsafe(8);
  while (encode >= moreBytes) {
    proxyBuff.writeBigUInt64LE(encode | moreBytes);
    proxyBuff.copy(buff, size, 0, 1);
    encode = encode >> shift;
    ++size;
  }
  proxyBuff.writeBigUInt64LE(encode);
  proxyBuff.copy(buff, size, 0, 1);
  crc = calculate(buff.subarray(0, size + 1), crc);
  return crc;
}

//precondition buff.length >= 8
function toBe64(val: bigint, buff: Buffer, start: number) {
  buff.writeBigInt64BE(val, start);
}

//precondition buff.length >= 8
function toBeU64(val: bigint, buff: Buffer, start: number) {
  buff.writeBigUInt64BE(val, start);
}

//precondition buff.length >= 4
function toBe32(val: number, buff: Buffer, start: number) {
  buff.writeInt32BE(val, start);
}

//precondition buff.length >= 4
function toBeU32(val: number, buff: Buffer, start: number) {
  buff.writeUInt32BE(val, start);
}

//precondition buff.length >= 2
function toBe16(val: number, buff: Buffer, start: number) {
  buff.writeInt16BE(val, start);
}

//precondition buff.length >= 2
function toBeU16(val: number, buff: Buffer, start: number) {
  buff.writeUInt16BE(val, start);
}

function crcRecordBatchHeader(crc: number, header: RecordBatchHeader) {
  let buff = Buffer.allocUnsafe(rbhId.totalSize);
  toBeU16(header.attributes, buff, rbhId.attributes);
  toBe32(header.lastOffsetDelta, buff, rbhId.lastOffsetDelta);
  toBe64(header.firstTimestamp, buff, rbhId.firstTimestamp);
  toBe64(header.maxTimestamp, buff, rbhId.maxTimestamp);
  toBe64(header.producerId, buff, rbhId.producerId);
  toBe16(header.producerEpoch, buff, rbhId.producerEpoch);
  toBe32(header.baseSequence, buff, rbhId.baseSequence);
  toBeU32(header.recordCount, buff, rbhId.recordCount);
  return calculate(buff, crc);
}

function crcRecord(crc: number, record: Record) {
  crc = crcExtendVint(BigInt(record.sizeBytes), crc);
  crc = crcExtendVint(BigInt(record.recordAttributes), crc);
  crc = crcExtendVint(BigInt(record.timestampDelta), crc);
  crc = crcExtendVint(BigInt(record.offsetDelta), crc);
  crc = crcExtendVint(BigInt(record.keySize), crc);
  crc = calculate(record.key, crc);
  crc = crcExtendVint(BigInt(record.valSize), crc);
  crc = calculate(record.value, crc);
  crc = crcExtendVint(BigInt(record.headers.length), crc);
  for (let h of record.headers) {
    crc = crcExtendVint(BigInt(h.keySize), crc);
    crc = calculate(h.key, crc);
    crc = crcExtendVint(BigInt(h.valSize), crc);
    crc = calculate(h.value, crc);
  }
  return crc;
}

export function crcRecordBatch(batch: RecordBatch) {
  let crc: number = 0;
  crc = crcRecordBatchHeader(crc, batch.header);
  for (let record of batch.records) {
    crc = crcRecord(crc, record);
  }
  return crc;
}
