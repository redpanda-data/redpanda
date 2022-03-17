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
  RpcHeader,
  SimplePod,
  RecordBatch,
  RecordBatchHeader,
  BatchHeader,
  Record,
  RecordHeader,
} from "./types";
import * as bytes from "buffer";
import {
  RpcHeaderCrc32,
  crcRecordBatchHeaderInternal,
  crcRecordBatch,
} from "../hashing/crc32";
import { RpcXxhash64 } from "../hashing/xxhash";
import { strict as assert } from "assert";

//The indices of the header
enum hId {
  version = 0,
  headerChecksum = 1,
  compression = 5,
  payloadSize = 6,
  meta = 10,
  correlationId = 14,
  payloadChecksum = 18,
}
//The indices of the pod
enum podId {
  readBytes = 0,
  x = 4,
  y = 6,
  z = 10,
}
//The indices of a RecordBatchHeader
enum rbhId {
  headerCrc = 0,
  sizeBytes = 4,
  baseOffset = 8,
  recordBatchType = 16,
  crc = 17,
  attrs = 21,
  lastOffsetDelta = 23,
  firstTimestamp = 27,
  maxTimestamp = 35,
  producerId = 43,
  producerEpoch = 51,
  baseSequence = 53,
  recordCount = 57,
  termId = 61,
  isCompressed = 69,
}
//Record RecordBatchHeader size
const rbhSz = 70;
//The indices of the record
enum rcId {
  sizeBytes = 0,
  attributes = 4,
  timestampDelta = 5,
  offsetDelta = 9,
  keySize = 13,
  keyBytesToFollow = 17,
  size = 21,
}

export class Deserializer {
  constructor() {}

  rpcHeader(bytes: Buffer) {
    //RpcHeader is 26 bytes
    //bytes[0...25]
    //get a subarray to header
    const head = bytes.subarray(0, 26);
    //parse the header
    const version = head.readUInt8(hId.version);
    const headerChecksum = head.readUInt32LE(hId.headerChecksum);
    const compression = head.readUInt8(hId.compression);
    const payloadSize = head.readUInt32LE(hId.payloadSize);
    const meta = head.readUInt32LE(hId.meta);
    const correlationId = head.readUInt32LE(hId.correlationId);
    const payloadChecksum: BigInt = head.readBigUInt64LE(hId.payloadChecksum);
    //calculate the crc of the header
    const crcVal = RpcHeaderCrc32(head);
    assert(crcVal == headerChecksum, "Error crc32 check failed");

    //return the header
    return new RpcHeader(
      version,
      headerChecksum,
      compression,
      payloadSize,
      meta,
      correlationId,
      payloadChecksum
    );
  }

  verifyPayload(payload: Buffer, payloadChecksum: bigint) {
    //index 26...bytes.length is the contents of iobuf
    //calculate the hash of payload
    const hashVal = RpcXxhash64(payload);
    //verify that checksum and xxhash are valid
    assert(hashVal == payloadChecksum, "Error xxhash check failed");
  }

  verifyRecordBatchHeaderInternal(header: RecordBatchHeader) {
    const crc = crcRecordBatchHeaderInternal(header);
    const expectedCrc = header.headerCrc;
    assert(
      crc == expectedCrc,
      "Error RecordBatchHeader internal checksum failed"
    );
  }

  verifyRecordBatch(batch: RecordBatch) {
    const bytes = 4;
    let crc = Buffer.allocUnsafe(bytes);
    let expectedCrc = Buffer.allocUnsafe(bytes);
    crc.writeUInt32LE(crcRecordBatch(batch));
    expectedCrc.writeInt32LE(batch.header.crc);
    assert(crc.equals(expectedCrc), "Error RecordBatch checksum failed");
    this.verifyRecordBatchHeaderInternal(batch.header);
  }

  simplePod(bytes: Buffer) {
    const readBytes = bytes.readInt32LE(podId.readBytes);
    let x = bytes.readInt16LE(podId.x);
    let y = bytes.readInt32LE(podId.y);
    let z = bytes.readBigInt64LE(podId.z);
    return new SimplePod(x, y, z);
  }

  recordBatchHeader(bytes: Buffer) {
    const headerCrc = bytes.readUInt32LE(rbhId.headerCrc);
    const sizeBytes = bytes.readInt32LE(rbhId.sizeBytes);
    const baseOffset = bytes.readBigInt64LE(rbhId.baseOffset);
    const recordBatchType = bytes.readInt8(rbhId.recordBatchType);
    const crc = bytes.readInt32LE(rbhId.crc);
    const attrs = bytes.readInt16LE(rbhId.attrs);
    const lastOffsetDelta = bytes.readInt32LE(rbhId.lastOffsetDelta);
    const firstTimestamp = bytes.readBigInt64LE(rbhId.firstTimestamp);
    const maxTimestamp = bytes.readBigInt64LE(rbhId.maxTimestamp);
    const producerId = bytes.readBigInt64LE(rbhId.producerId);
    const producerEpoch = bytes.readInt16LE(rbhId.producerEpoch);
    const baseSequence = bytes.readInt32LE(rbhId.baseSequence);
    const recordCount = bytes.readUInt32LE(rbhId.recordCount);
    const termId = bytes.readBigInt64LE(rbhId.termId);
    return new RecordBatchHeader(
      headerCrc,
      sizeBytes,
      baseOffset,
      recordBatchType,
      crc,
      attrs,
      lastOffsetDelta,
      firstTimestamp,
      maxTimestamp,
      producerId,
      producerEpoch,
      baseSequence,
      recordCount,
      termId
    );
  }

  batchHeader(bytes: Buffer) {
    const header: RecordBatchHeader = this.recordBatchHeader(bytes);
    const isCompressed: number = bytes.readInt8(rbhId.isCompressed);
    return new BatchHeader(header, isCompressed);
  }

  record(bytes: Buffer) {
    const sizeBytes = bytes.readInt32LE(rcId.sizeBytes);
    const attributes = bytes.readInt8(rcId.attributes);
    // @ts-ignore
    const timestampDelta = bytes.readInt64LE(rcId.timestampDelta);
    const offsetDelta = bytes.readInt32LE(rcId.offsetDelta);
    const keySize = bytes.readInt32LE(rcId.keySize);
    const keyBytesToFollow = bytes.readInt32LE(rcId.keyBytesToFollow);
    let begin = rcId.size;
    let end = begin + keyBytesToFollow;
    const key = Buffer.from(bytes.subarray(begin, end));
    const valSize = bytes.readInt32LE(end);
    const fourBytes = 4;
    begin = end + fourBytes;
    const valBytesToFollow = bytes.readInt32LE(begin);
    begin += fourBytes;
    end = begin + valBytesToFollow;
    const val = Buffer.from(bytes.subarray(begin, end));
    begin = end;
    const headerSize = bytes.readInt32LE(begin);
    let headers: Array<RecordHeader> = [];
    begin += fourBytes;
    for (let i: number = 0; i < headerSize; ++i) {
      const keySz = bytes.readInt32LE(begin);
      begin += fourBytes;
      const keyLen = bytes.readInt32LE(begin);
      begin += fourBytes;
      end = begin + keyLen;
      let hkey = Buffer.from(bytes.subarray(begin, end));
      const valSz = bytes.readInt32LE(end);
      begin = end + fourBytes;
      const valLen = bytes.readInt32LE(begin);
      begin += fourBytes;
      end = begin + valLen;
      let hvalue = Buffer.from(bytes.subarray(begin, end));
      begin = end;
      headers.push(new RecordHeader(keySz, hkey, valSz, hvalue));
    }
    return [
      new Record(
        sizeBytes,
        attributes,
        timestampDelta,
        offsetDelta,
        keySize,
        key,
        valSize,
        val,
        headers
      ),
      begin,
    ];
  }

  recordBatch(bytes: Buffer) {
    const batchHeader = this.batchHeader(bytes);
    let begin: any = rbhSz;
    let records = [];
    for (let i = 0; i < batchHeader.recordBatchHeader.recordCount; ++i) {
      let result = this.record(bytes.subarray(begin));
      records.push(result[0]);
      begin += result[1];
    }
    return [new RecordBatch(batchHeader.recordBatchHeader, records), begin];
  }

  recordBatchReader(bytes: Buffer) {
    const totalBatches = bytes.readInt32LE(0);
    let recBatches: Array<RecordBatch> = [];
    let begin: any = 4;
    for (let i = 0; i < totalBatches; ++i) {
      let result = this.recordBatch(bytes.subarray(begin));
      this.verifyRecordBatch(result[0]);
      recBatches.push(result[0]);
      begin += result[1];
    }
    return recBatches;
  }
}

export class Serializer {
  constructor() {}

  //preconditions:
  //1. header checksum has been calculated
  //2. payload checksum has been calculated
  rpcHeader(header: RpcHeader) {
    //RpcHeader is 26 bytes
    //The numbers are in bytes
    const totalBytes: number =
      1 + //version
      4 + //header_checksum
      1 + //compression
      4 + //payload size
      4 + //meta
      4 + //correlation_id
      8; //payload_checksum
    let buf: Buffer = Buffer.allocUnsafe(totalBytes);
    buf.writeUInt8(header.version, hId.version);
    buf.writeUInt32LE(header.headerChecksum, hId.headerChecksum);
    buf.writeUInt8(header.compression, hId.compression);
    buf.writeUInt32LE(header.payload, hId.payloadSize);
    buf.writeUInt32LE(header.meta, hId.meta);
    buf.writeUInt32LE(header.correlationId, hId.correlationId);
    buf.writeBigUInt64LE(header.payloadChecksum, hId.payloadChecksum);
    return buf;
  }

  simplePod(pod: SimplePod) {
    const payloadSize: number = 2 + 4 + 8;
    const totalBytes: number = 4 + payloadSize;
    let buf: Buffer = Buffer.allocUnsafe(totalBytes);
    buf.writeInt32LE(payloadSize, podId.readBytes);
    buf.writeInt16LE(pod.x, podId.x);
    buf.writeInt32LE(pod.y, podId.y);
    buf.writeBigInt64LE(pod.z, podId.z);
    return buf;
  }

  recordBatchHeader(header: RecordBatchHeader) {
    const totalBytes: number = 69;
    let buf: Buffer = Buffer.allocUnsafe(totalBytes);
    //to do write the bytes to fllow ;)
    buf.writeUInt32LE(header.headerCrc, rbhId.headerCrc);
    buf.writeInt32LE(header.sizeBytes, rbhId.sizeBytes);
    buf.writeBigInt64LE(header.baseOffset, rbhId.baseOffset);
    buf.writeInt8(header.recordBatchType, rbhId.recordBatchType);
    buf.writeInt32LE(header.crc, rbhId.crc);
    buf.writeInt16LE(header.attributes, rbhId.attrs);
    buf.writeInt32LE(header.lastOffsetDelta, rbhId.lastOffsetDelta);
    buf.writeBigInt64LE(header.firstTimestamp, rbhId.firstTimestamp);
    buf.writeBigInt64LE(header.maxTimestamp, rbhId.maxTimestamp);
    buf.writeBigInt64LE(header.producerId, rbhId.producerId);
    buf.writeInt16LE(header.producerEpoch, rbhId.producerEpoch);
    buf.writeInt32LE(header.baseSequence, rbhId.baseSequence);
    buf.writeUInt32LE(header.recordCount, rbhId.recordCount);
    buf.writeBigInt64LE(header.termId, rbhId.termId);
    return buf;
  }

  batchHeader(header: RecordBatchHeader) {
    let buf: Buffer = this.recordBatchHeader(header);
    const bytes: number = 1;
    let buf2: Buffer = Buffer.allocUnsafe(bytes);
    //write bytes to follow
    let isCompressed = 0;
    buf2.writeInt8(isCompressed);
    return Buffer.concat([buf, buf2]);
  }

  private getBuffFromVal32(val: number) {
    let buf: Buffer = Buffer.allocUnsafe(4);
    buf.writeInt32LE(val);
    return buf;
  }

  private getBuffFromVal64(a: number, b: number) {
    let buf: Buffer = Buffer.allocUnsafe(8);
    buf.writeInt32LE(a);
    buf.writeInt32LE(b, 4);
    return buf;
  }

  record(header: Record) {
    let buf1: Buffer = Buffer.allocUnsafe(rcId.size);
    buf1.writeInt32LE(header.sizeBytes, rcId.sizeBytes);
    buf1.writeInt8(header.recordAttributes, rcId.attributes);
    // @ts-ignore
    buf1.writeInt64LE(header.timestampDelta, rcId.timestampDelta);
    buf1.writeInt32LE(header.offsetDelta, rcId.offsetDelta);
    buf1.writeInt32LE(header.keySize, rcId.keySize);
    buf1.writeInt32LE(header.key.length, rcId.keyBytesToFollow);
    let buf2 = Buffer.from(header.key);
    let buf3 = this.getBuffFromVal64(header.valSize, header.value.length);
    let buf4 = Buffer.from(header.value);
    let buf5 = this.getBuffFromVal32(header.headers.length);
    let headersBuf = Buffer.concat([buf1, buf2, buf3, buf4, buf5]);
    for (let h of header.headers) {
      let tmp1 = this.getBuffFromVal64(h.keySize, h.key.length);
      let tmp2 = Buffer.from(h.key);
      let tmp3 = this.getBuffFromVal64(h.valSize, h.value.length);
      let tmp4 = Buffer.from(h.value);
      headersBuf = Buffer.concat([headersBuf, tmp1, tmp2, tmp3, tmp4]);
    }
    return headersBuf;
  }

  recordBatch(batch: RecordBatch) {
    let result: Buffer = this.batchHeader(batch.header);
    for (let record of batch.records) {
      let serializedRecordBuf: Buffer = this.record(record);
      result = Buffer.concat([result, serializedRecordBuf]);
    }
    return result;
  }

  recordBatchReader(reader: Array<RecordBatch>) {
    let result = this.getBuffFromVal32(reader.length);
    for (let batch of reader) {
      let tmp = this.recordBatch(batch);
      result = Buffer.concat([result, tmp]);
    }
    return result;
  }

  //preconditions
  //1. materialized.length > 0
  materializedRecordBatchReader(materialized: Array<Array<RecordBatch>>) {
    const totalReaders = materialized.length;
    let buff = this.getBuffFromVal32(totalReaders);
    for (let batch of materialized) {
      let result = this.recordBatchReader(batch);
      buff = Buffer.concat([buff, result]);
    }
    return buff;
  }
}
