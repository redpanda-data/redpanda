/**
 * Copyright 2020 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

import { IOBuf } from "../../utilities/IOBuf";
import {
  Record,
  RecordBatch,
  RecordBatchHeader,
} from "../../domain/generatedRpc/generatedClasses";
import { calculate } from "fast-crc32c";

// receive int64 and return Uint64
const encodeZigzag = (field: bigint): bigint => {
  // Create Bigint with 64 bytes length and sign 63
  const digits = BigInt.asUintN(64, BigInt(63));
  // Create Bigint with 64 bytes length and sign 1
  const lsb = BigInt.asUintN(64, BigInt(1));
  return BigInt.asUintN(64, (field << lsb) ^ (field >> digits));
};

// receive Uint64 and return int64
const decodeZigzag = (field: bigint): bigint => {
  const lsb = BigInt.asIntN(64, BigInt(1));
  return (
    BigInt.asIntN(64, field >> lsb) ^ BigInt.asIntN(64, ~(field & lsb) + lsb)
  );
};

/** Serialization **/

/**
 * return a new offset after apply serialization process
 */
type WriteFn<T> = (field: T, buffer: IOBuf, object?) => number;
type ToBytes<T> = (value: T, buffer: IOBuf) => number;

const writeInt8LE: WriteFn<number> = (field, buffer) => {
  return buffer.appendInt8(field);
};

const writeInt16LE: WriteFn<number> = (field, buffer) => {
  return buffer.appendInt16LE(field);
};

const writeInt32LE: WriteFn<number> = (field, buffer) => {
  return buffer.appendInt32LE(field);
};

const writeInt64LE: WriteFn<bigint> = (field, buffer) => {
  return buffer.appendBigInt64LE(field);
};

const writeUInt8LE: WriteFn<number> = (field, buffer) => {
  return buffer.appendUInt8(field);
};

const writeUInt16LE: WriteFn<number> = (field, buffer) => {
  return buffer.appendUInt16LE(field);
};

const writeUInt32LE: WriteFn<number> = (field, buffer) => {
  return buffer.appendUInt32LE(field);
};

const writeUInt64LE: WriteFn<bigint> = (field, buffer) => {
  return buffer.appendBigUInt64LE(field);
};

const writeVarint: WriteFn<bigint> = (field, buffer) => {
  let value = encodeZigzag(field);
  let writtenBytes = 0;
  if (value < 0x80) {
    return buffer.appendUInt8(Number(value));
  }
  writtenBytes += buffer.appendUInt8(
    Number((value & BigInt(255)) | BigInt(0x80))
  );
  value >>= BigInt(7);
  if (value < 0x80) {
    writtenBytes += buffer.appendUInt8(Number(value));
  }
  do {
    writtenBytes += buffer.appendUInt8(
      Number((value & BigInt(255)) | BigInt(0x80))
    );
    value >>= BigInt(7);
  } while (value >= 0x80);
  writtenBytes += buffer.appendUInt8(Number(value));
  return writtenBytes;
};

const writeVarintBuffer = (field: bigint, buffer: Buffer) => {
  let value = encodeZigzag(field);
  let writtenBytes = 0;
  if (value < 0x80) {
    buffer.writeUInt8(Number(value));
    return 1;
  }
  buffer.writeUInt8(Number((value & BigInt(255)) | BigInt(0x80)));
  writtenBytes = 1;
  value >>= BigInt(7);
  if (value < 0x80) {
    buffer.writeUInt8(Number(value), writtenBytes);
    writtenBytes += 1;
    return writtenBytes;
  }
  do {
    buffer.writeUInt8(
      Number((value & BigInt(255)) | BigInt(0x80)),
      writtenBytes
    );
    writtenBytes += 1;
    value >>= BigInt(7);
  } while (value >= 0x80);
  buffer.writeUInt8(Number(value), writtenBytes);
  writtenBytes += 1;
  return writtenBytes;
};

const writeBoolean: WriteFn<boolean> = (field, buffer) => {
  return buffer.appendInt8(field ? 1 : 0);
};

const writeString: WriteFn<string> = (field, buffer) => {
  buffer.appendInt32LE(Buffer.byteLength(field));
  const stringSize = buffer.appendString(field);
  return 4 + stringSize;
};

const writeBuffer: WriteFn<Buffer> = (field, buffer, offset) => {
  buffer.appendInt32LE(Buffer.byteLength(field));
  const bufferBuffer = buffer.appendBuffer(field);
  return 4 + bufferBuffer;
};

/**
 * @param appendSize, define if writeArray puts an int32 (array size) in buffer
 */
const writeArray = (appendSize?: boolean) => <T>(
  fields: T[],
  buffer: IOBuf,
  fn: WriteFn<T>
): number => {
  let writtenBytes = 0;
  if (appendSize) {
    buffer.appendInt32LE(fields.length);
    writtenBytes += 4;
  }
  for (const item of fields) {
    writtenBytes += fn(item, buffer);
  }
  return writtenBytes;
};

const writeObject = <T>(
  buffer: IOBuf,
  type: { toBytes: ToBytes<T> },
  object: T
): number => {
  return type.toBytes(object, buffer);
};

/** Deserializer **/

type FromBytes<T> = { fromBytes: (Buffer, number) => [T, number] };
type ReadFunction<T> = (
  buffer: Buffer,
  offset: number,
  obj?: FromBytes<T>,
  type?: T
) => [T, number];

const readInt8LE: ReadFunction<number> = (buffer: Buffer, offset) => {
  const value = buffer.readInt8(offset);
  offset += 1;
  return [value, offset];
};

const readInt16LE: ReadFunction<number> = (buffer: Buffer, offset) => {
  const value = buffer.readInt16LE(offset);
  offset += 2;
  return [value, offset];
};

const readInt32LE: ReadFunction<number> = (buffer: Buffer, offset) => {
  const value = buffer.readInt32LE(offset);
  offset += 4;
  return [value, offset];
};

const readInt64LE: ReadFunction<bigint> = (buffer: Buffer, offset) => {
  const value = buffer.readBigInt64LE(offset);
  offset += 8;
  return [BigInt(value), offset];
};

const readUInt8LE: ReadFunction<number> = (buffer: Buffer, offset) => {
  const value = buffer.readUInt8(offset);
  offset += 1;
  return [value, offset];
};

const readUInt16LE: ReadFunction<number> = (buffer: Buffer, offset) => {
  const value = buffer.readUInt16LE(offset);
  offset += 2;
  return [value, offset];
};

const readUInt32LE: ReadFunction<number> = (buffer: Buffer, offset) => {
  const value = buffer.readUInt32LE(offset);
  offset += 4;
  return [value, offset];
};

const readUInt64LE: ReadFunction<bigint> = (buffer: Buffer, offset) => {
  const value = buffer.readBigUInt64LE(offset);
  offset += 8;
  return [BigInt(value), offset];
};

const readVarint: ReadFunction<bigint> = (buffer: Buffer, offset: number) => {
  let result = BigInt.asUintN(64, BigInt(0));
  let shift = 0;
  let initialOffset = offset;
  for (
    let src = buffer.readInt8(initialOffset);
    shift <= 63;
    src = buffer.readInt8(initialOffset)
  ) {
    initialOffset += 1;
    // check if byte read is (1000 0000) has the first bit in 1
    if (src & 128) {
      result |= BigInt.asUintN(64, BigInt(src & 127)) << BigInt(shift);
    } else {
      result |= BigInt.asUintN(7, BigInt(src & 127)) << BigInt(shift);
      break;
    }
    shift += 7;
  }
  return [decodeZigzag(result), initialOffset];
};

const readBoolean: ReadFunction<boolean> = (buffer: Buffer, offset: number) => {
  const value = buffer.readInt8(offset);
  offset += 1;
  return [Boolean(value), offset];
};

const readString: ReadFunction<string> = (buffer: Buffer, offset) => {
  const size = buffer.readInt32LE(offset);
  offset += 4;
  const value = buffer.toString(undefined, offset, offset + size);
  offset += size;
  return [value, offset];
};

const readBuffer: ReadFunction<Buffer> = (buffer: Buffer, offset: number) => {
  const size = buffer.readInt32LE(offset);
  offset += 4;
  const value = buffer.slice(offset, offset + size);
  offset += size;
  return [value, offset];
};

/**
 * @param readSize, define if readArray reads an int32 (array size) from buffer,
 * otherwise, the array size is passed
 */
const readArray = (readSize?: number) => <T>(
  buffer: Buffer,
  offset: number,
  fn: ReadFunction<T>,
  obj?: FromBytes<T>
): [T[], number] => {
  const array: T[] = [];
  let arraySize = readSize
  if (arraySize === undefined) {
    arraySize = buffer.readInt32LE(offset);
    offset += 4;
  }
  for (let i = 0; i < arraySize; i++) {
    const [value, newOffset] = fn(buffer, offset, obj);
    offset = newOffset;
    array.push(value);
  }
  return [array, offset];
};

const readObject = <T>(
  buffer: Buffer,
  offset: number,
  obj: FromBytes<T>
): [T, number] => {
  return obj.fromBytes(buffer, offset);
};

const extendRecords = (records: Record[], seed = 0): number => {
  const auxBuffer = Buffer.alloc(8);
  return records.reduce((prev, record) => {
    let size;
    size = writeVarintBuffer(BigInt(record.length), auxBuffer);
    const lengthCrc = calculate(auxBuffer.slice(0, size), prev);
    size = writeVarintBuffer(BigInt(record.attributes), auxBuffer);
    const attrCrc = calculate(auxBuffer.slice(0, size), lengthCrc);
    size = writeVarintBuffer(record.timestampDelta, auxBuffer);
    const timesStampCrc = calculate(auxBuffer.slice(0, size), attrCrc);
    size = writeVarintBuffer(BigInt(record.offsetDelta), auxBuffer);
    const offsetDeltaCrc = calculate(auxBuffer.slice(0, size), timesStampCrc);
    size = writeVarintBuffer(BigInt(record.keyLength), auxBuffer);
    const keyLengthCrc = calculate(auxBuffer.slice(0, size), offsetDeltaCrc);
    const keyCrc = calculate(record.key, keyLengthCrc);
    size = writeVarintBuffer(BigInt(record.valueLen), auxBuffer);
    const valueLengthCrc = calculate(auxBuffer.slice(0, size), keyCrc);
    const valueCrc = calculate(record.value, valueLengthCrc);
    size = writeVarintBuffer(BigInt(record.headers.length), auxBuffer);
    return calculate(auxBuffer.slice(0, size), valueCrc);
  }, seed);
};

const recordBatchEncode = (value: RecordBatch, buffer: IOBuf): number => {
  let wroteBytes = 0;
  // write header some attribute on BE format
  const bufferHeaderCrc = Buffer.allocUnsafe(40);
  bufferHeaderCrc.writeInt16BE(value.header.attrs, 0);
  bufferHeaderCrc.writeInt32BE(value.header.lastOffsetDelta, 2);
  bufferHeaderCrc.writeBigInt64BE(value.header.firstTimestamp, 6);
  bufferHeaderCrc.writeBigInt64BE(value.header.maxTimestamp, 14);
  bufferHeaderCrc.writeBigInt64BE(value.header.producerId, 22);
  bufferHeaderCrc.writeInt16BE(value.header.producerEpoch, 30);
  bufferHeaderCrc.writeInt32BE(value.header.baseSequence, 32);
  bufferHeaderCrc.writeInt32BE(value.header.recordCount, 36);

  // reserve record batch header, term, isCompressed.
  const reserver = buffer.getReserve(70);
  // create auxiliary iobuf

  wroteBytes += writeObject(reserver, RecordBatchHeader, value.header);
  wroteBytes += writeArray(false)(value.records, buffer, (item, auxBuffer) =>
    writeObject(auxBuffer, Record, item)
  );
  // Get buffer instance from iobuf
  const headerBuffer = reserver.getIterable().slice(70);
  // Calculate header crc, it depends on records and, some header attributes
  const crc = extendRecords(value.records, calculate(bufferHeaderCrc));
  // 17 is the byte position for crc attribute on Record Batch Header
  headerBuffer.writeUInt32LE(crc, 17);
  /**
   * for headerCrc we need to calculate:
   * sizeBytes (4)
   * baseOffset (12)
   * recordBatchType (20)
   * crc (21)
   * attrs (25)
   * lastOffsetDelta (27)
   * firstTimestamp (31)
   * maxTimestamp (39)
   * producerId (47)
   * producerEpoch (55)
   * baseSequence (57)
   * recordCount (61)
   *
   * 4 -> 61 is the range of the bytes on Buffer that we need for
   * calculate headerCrc
   */
  const headerCrc = calculate(headerBuffer.slice(4, 61));
  headerBuffer.writeUInt32LE(headerCrc, 0);
  // reset reserve write offset, I can use index 0 because there only
  // one fragment in this reserve with 70 bytes, this in order to update
  // complete record batch header with its crc hash code.
  reserver.getFragmentByIndex(0).used = 0
  reserver.appendBuffer(headerBuffer);

  return wroteBytes;
};

export default {
  writeInt8LE,
  writeInt16LE,
  writeInt32LE,
  writeInt64LE,
  writeUInt8LE,
  writeUInt16LE,
  writeUInt32LE,
  writeUInt64LE,
  writeString,
  writeBoolean,
  writeArray,
  writeObject,
  writeVarint,
  writeBuffer,
  writeVarintBuffer,
  readInt8LE,
  readInt16LE,
  readInt32LE,
  readInt64LE,
  readUInt8LE,
  readUInt16LE,
  readUInt32LE,
  readUInt64LE,
  readString,
  readBoolean,
  readObject,
  readVarint,
  readArray,
  readBuffer,
  recordBatchEncode,
  extendRecords,
};
