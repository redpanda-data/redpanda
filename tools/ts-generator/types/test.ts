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
import { IOBuf } from "../../../src/js/modules/utilities/IOBuf";

describe("Buffer functions test", function () {
  let ioBuf = new IOBuf();
  beforeEach(() => (ioBuf = new IOBuf()));
  [
    {
      message: "should writeInt8LE and readInt8LE",
      value: 3,
      bufferSize: 1,
      writeFn: "writeInt8LE",
      readFn: "readInt8LE",
    },
    {
      message: "should writeInt16LE and readInt16LE",
      value: 35,
      bufferSize: 2,
      writeFn: "writeInt16LE",
      readFn: "readInt16LE",
    },
    {
      message: "should writeInt32LE and readInt32LE",
      value: 234,
      bufferSize: 4,
      writeFn: "writeInt32LE",
      readFn: "readInt32LE",
    },
    {
      message: "should writeInt64LE and readInt64LE",
      value: BigInt(234),
      bufferSize: 8,
      writeFn: "writeInt64LE",
      readFn: "readInt64LE",
    },
    {
      message: "should writeUInt8LE and readUInt8LE",
      value: 254,
      bufferSize: 1,
      writeFn: "writeUInt8LE",
      readFn: "readUInt8LE",
    },
    {
      message: "should writeUInt16LE and readUInt16LE",
      value: 65535,
      bufferSize: 2,
      writeFn: "writeUInt16LE",
      readFn: "readUInt16LE",
    },
    {
      message: "should writeUInt32LE and readUInt32LE",
      value: 4294967295,
      bufferSize: 4,
      writeFn: "writeUInt32LE",
      readFn: "readUInt32LE",
    },
    {
      message: "should writeUInt64LE and readUInt64LE",
      value: BigInt(2 ** 63 - 1),
      bufferSize: 8,
      writeFn: "writeUInt64LE",
      readFn: "readUInt64LE",
    },
    {
      message: "should writeBoolean and readBoolean",
      value: true,
      bufferSize: 1,
      writeFn: "writeBoolean",
      readFn: "readBoolean",
    },
    {
      message: "should writeVarint and readVarint",
      value: BigInt(123456789),
      bufferSize: 4,
      writeFn: "writeVarint",
      readFn: "readVarint",
    },
    {
      message: "should writeBuffer and readBuffer",
      value: Buffer.from("string"),
      bufferSize: 10,
      writeFn: "writeBuffer",
      readFn: "readBuffer",
    },
  ].forEach(({ message, value, bufferSize, writeFn, readFn }) => {
    it(message, function () {
      BF[writeFn](value, ioBuf);
      const buffer = ioBuf.getIterable().slice(bufferSize);
      const [result, offset] = BF[readFn](buffer, 0);
      asserts.deepStrictEqual(result, value);
      asserts.strictEqual(offset, bufferSize);
    });
  });

  it("should writeArray and readArray", function () {
    const array = ["a", "b", "c"];
    const size = BF.writeArray(true)(
      array,
      ioBuf,
      (item, auxBuffer, auxOffset) => BF.writeString(item, auxBuffer, auxOffset)
    );
    const buffer = ioBuf.getIterable().slice(size);
    const [result, offset] = BF.readArray()(buffer, 0, (auxBuffer, auxOffset) =>
      BF.readString(auxBuffer, auxOffset)
    );
    asserts.deepStrictEqual(result, array);
    asserts.strictEqual(offset, 19);
  });
});
