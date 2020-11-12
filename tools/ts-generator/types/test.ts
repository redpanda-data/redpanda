/**
 * Copyright 2020 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

import BF from "./functions";
import * as asserts from "assert";

describe("Buffer functions test", function () {
  const buffer = Buffer.alloc(20);
  beforeEach(() => buffer.fill(0));
  it("should writeInt8LE and readInt8LE", function () {
    const int8 = 3;
    BF.writeInt8LE(int8, buffer, 0);
    const [result, offset] = BF.readInt8LE(buffer, 0);
    asserts.strictEqual(result, int8);
    asserts.strictEqual(offset, 1);
  });

  it("should writeInt16LE and readInt16LE", function () {
    const int16 = 35;
    BF.writeInt16LE(int16, buffer, 0);
    const [result, offset] = BF.readInt16LE(buffer, 0);
    asserts.strictEqual(result, int16);
    asserts.strictEqual(offset, 2);
  });

  it("should writeInt32LE and readInt32LE", function () {
    const int32 = 234;
    BF.writeInt32LE(int32, buffer, 0);
    const [result, offset] = BF.readInt32LE(buffer, 0);
    asserts.strictEqual(result, int32);
    asserts.strictEqual(offset, 4);
  });

  it("should writeInt64LE and readInt64LE", function () {
    const int64 = BigInt(234);
    BF.writeInt64LE(int64, buffer, 0);
    const [result, offset] = BF.readInt64LE(buffer, 0);
    asserts.strictEqual(result, int64);
    asserts.strictEqual(offset, 8);
  });

  it("should writeUInt8LE and readUInt8LE", function () {
    const int8 = 254;
    BF.writeUInt8LE(int8, buffer, 0);
    const [result, offset] = BF.readUInt8LE(buffer, 0);
    asserts.strictEqual(result, int8);
    asserts.strictEqual(offset, 1);
  });

  it("should writeUInt16LE and readUInt16LE", function () {
    const int16 = 65535;
    BF.writeUInt16LE(int16, buffer, 0);
    const [result, offset] = BF.readUInt16LE(buffer, 0);
    asserts.strictEqual(result, int16);
    asserts.strictEqual(offset, 2);
  });

  it("should writeUInt32LE and readUInt32LE", function () {
    const int32 = 4294967295;
    BF.writeUInt32LE(int32, buffer, 0);
    const [result, offset] = BF.readUInt32LE(buffer, 0);
    asserts.strictEqual(result, int32);
    asserts.strictEqual(offset, 4);
  });

  it("should writeUInt64LE and readUInt64LE", function () {
    // @ts-ignore
    const int64 = BigInt(2 ** 63 - 1);
    BF.writeUInt64LE(int64, buffer, 0);
    const [result, offset] = BF.readUInt64LE(buffer, 0);
    asserts.strictEqual(result, int64);
    asserts.strictEqual(offset, 8);
  });

  it("should writeBoolean and readBoolean", function () {
    const bol = true;
    BF.writeBoolean(bol, buffer, 0);
    const [result, offset] = BF.readBoolean(buffer, 0);
    asserts.strictEqual(result, bol);
    asserts.strictEqual(offset, 1);
  });

  it("should writeVarint and readVarint", function () {
    const varint = BigInt(123456789);
    BF.writeVarint(varint, buffer, 0);
    const [result, offset] = BF.readVarint(buffer, 0);
    asserts.strictEqual(result, varint);
    asserts.strictEqual(offset, 4);
  });

  it("should writeBuffer and readBuffer", function () {
    const bufferValue = Buffer.from("string");
    BF.writeBuffer(bufferValue, buffer, 0);
    const [result, offset] = BF.readBuffer(buffer, 0);
    asserts.strictEqual(result.toString(), bufferValue.toString());
    asserts.strictEqual(offset, 10);
  });

  it("should writeArray and readArray", function () {
    const array = ["a", "b", "c"];
    BF.writeArray(array, buffer, 0, (item, auxBuffer, auxOffset) =>
      BF.writeString(item, auxBuffer, auxOffset)
    );
    const [result, offset] = BF.readArray(buffer, 0, (auxBuffer, auxOffset) =>
      BF.readString(auxBuffer, auxOffset)
    );
    asserts.deepStrictEqual(result, array);
    asserts.strictEqual(offset, 19);
  });
});
