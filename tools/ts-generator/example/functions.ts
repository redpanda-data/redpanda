/**
 * Copyright 2020 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

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
type WriteFn<T> = (field: T, buffer: Buffer, offset: number, object?) => number;
type ToBytes<T> = (value: T, buffer: Buffer, offset: number) => number;

const writeInt8LE: WriteFn<number> = (field, buffer, offset) => {
  buffer.writeInt8(field, offset);
  // 1 bytes is the length of a number in the buffer
  offset += 1;
  return offset;
};

const writeInt16LE: WriteFn<number> = (field, buffer, offset) => {
  buffer.writeInt16LE(field, offset);
  // 2 bytes is the length of a number in the buffer
  offset += 2;
  return offset;
};

const writeInt32LE: WriteFn<number> = (field, buffer, offset) => {
  buffer.writeInt32LE(field, offset);
  // 4 bytes is the length of a number in the buffer
  offset += 4;
  return offset;
};

const writeInt64LE: WriteFn<bigint> = (field, buffer, offset) => {
  buffer.writeBigInt64LE(field, offset);
  // 8 bytes is the length of a number in the buffer
  offset += 8;
  return offset;
};

const writeUInt8LE: WriteFn<number> = (field, buffer, offset) => {
  buffer.writeUInt8(field, offset);
  // 1 bytes is the length of a number in the buffer
  offset += 1;
  return offset;
};

const writeUInt16LE: WriteFn<number> = (field, buffer, offset) => {
  buffer.writeUInt16LE(field, offset);
  // 2 bytes is the length of a number in the buffer
  offset += 2;
  return offset;
};

const writeUInt32LE: WriteFn<number> = (field, buffer, offset) => {
  buffer.writeUInt32LE(field, offset);
  // 4 bytes is the length of a number in the buffer
  offset += 4;
  return offset;
};

const writeUInt64LE: WriteFn<bigint> = (field, buffer, offset) => {
  buffer.writeBigUInt64LE(field, offset);
  // 8 bytes is the length of a number in the buffer
  offset += 8;
  return offset;
};

const writeVarint: WriteFn<bigint> = (field, buffer, offset) => {
  let value = encodeZigzag(field);
  if (value < 0x80) {
    buffer.writeUInt8(Number(value), offset);
    offset += 1;
    return offset;
  }
  buffer.writeUInt8(Number((value & BigInt(255)) | BigInt(0x80)), offset);
  offset++;
  value >>= BigInt(7);
  if (value < 0x80) {
    buffer.writeUInt8(Number(value), offset);
    offset++;
    return offset;
  }
  do {
    buffer.writeUInt8(Number((value & BigInt(255)) | BigInt(0x80)), offset);
    offset++;
    value >>= BigInt(7);
  } while (value >= 0x80);
  buffer.writeUInt8(Number(value), offset);
  offset++;
  return offset;
};

const writeBoolean: WriteFn<boolean> = (field, buffer, offset) => {
  buffer.writeInt8(field ? 1 : 0, offset);
  offset += 1;
  return offset;
};

const writeString: WriteFn<string> = (field, buffer, offset) => {
  buffer.writeInt32LE(Buffer.byteLength(field), offset);
  offset += 4;
  buffer.write(field, offset);
  offset += Buffer.byteLength(field);
  return offset;
};

const writeBuffer: WriteFn<Buffer> = (field, buffer, offset) => {
  buffer.writeInt32LE(Buffer.byteLength(field), offset);
  offset += 4;
  field.copy(buffer, offset);
  offset += Buffer.byteLength(field);
  return offset;
};

const writeArray = <T>(
  fields: T[],
  buffer: Buffer,
  offset: number,
  fn: WriteFn<T>
): number => {
  buffer.writeInt32LE(fields.length, offset);
  offset += 4;
  for (const item of fields) {
    offset = fn(item, buffer, offset);
  }
  return offset;
};

const writeObject = <T>(
  buffer: Buffer,
  offset: number,
  type: { toBytes: ToBytes<T> },
  object: T
): number => {
  return type.toBytes(object, buffer, offset);
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

const readArray = <T>(
  buffer: Buffer,
  offset: number,
  fn: ReadFunction<T>,
  obj?: FromBytes<T>
): [T[], number] => {
  const array: T[] = [];
  const size = buffer.readInt32LE(offset);
  offset += 4;
  for (let i = 0; i < size; i++) {
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
};
