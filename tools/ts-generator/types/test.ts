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

  it("should write varint in buffer", function () {
    const buffer = Buffer.alloc(8);
    const size = BF.writeVarintBuffer(BigInt(56000), buffer);
    const [number, offset] = BF.readVarint(buffer, 0);
    asserts.deepStrictEqual(number, BigInt(56000));
    asserts.strictEqual(offset, 3);
    asserts.strictEqual(size, 3);
  });

  it("should extend records", function () {
    const crc = BF.extendRecords([
      {
        attributes: 0,
        key: Buffer.from([]),
        keyLength: 0,
        length: 10,
        offsetDelta: 0,
        timestampDelta: BigInt(0),
        value: Buffer.from([54, 65, 73, 74]),
        valueLen: 4,
        headers: [],
      },
    ]);
    const expectedCrc = 2892331115;
    asserts.strictEqual(crc, expectedCrc);
  });

  it("should extend records with seed", function () {
    const seed = 321123434;
    const crc = BF.extendRecords(
      [
        {
          attributes: 0,
          key: Buffer.from([]),
          keyLength: 0,
          length: 10,
          offsetDelta: 0,
          timestampDelta: BigInt(0),
          value: Buffer.from([54, 65, 73, 74]),
          valueLen: 4,
          headers: [],
        },
      ],
      seed
    );
    const unexpectedCrc = 2892331115;
    const expectedCrc = 3089006589;
    asserts.strictEqual(crc, expectedCrc);
    asserts.notStrictEqual(crc, unexpectedCrc);
  });

  it(
    "should record batch encode its attributes and " +
      "generate crc and crcHeader",
    function () {
      const recordBatch = {
        header: {
          attrs: 0,
          baseOffset: BigInt(3),
          baseSequence: 0,
          crc: 0,
          firstTimestamp: BigInt(1604520312708),
          headerCrc: 0,
          lastOffsetDelta: 0,
          maxTimestamp: BigInt(-1),
          producerEpoch: -1,
          producerId: BigInt(-1),
          recordBatchType: 1,
          recordCount: 1,
          sizeBytes: 72,
          term: BigInt(3),
          isCompressed: 0,
        },
        records: [
          {
            attributes: 0,
            key: Buffer.from([]),
            keyLength: 0,
            length: 10,
            offsetDelta: 0,
            timestampDelta: BigInt(0),
            value: Buffer.from([54, 65, 73, 74]),
            valueLen: 4,
            headers: [],
          },
        ],
      };
      const iobuf = new IOBuf();
      BF.recordBatchEncode(recordBatch, iobuf);
      const iterable = iobuf.getIterable();
      asserts.strictEqual(iterable.readInt32LE(), 1686771807);
      asserts.strictEqual(iterable.readInt32LE(), recordBatch.header.sizeBytes);
      asserts.strictEqual(
        iterable.readBigInt64LE(),
        recordBatch.header.baseOffset
      );
      asserts.strictEqual(
        iterable.readInt8(),
        recordBatch.header.recordBatchType
      );
      asserts.strictEqual(iterable.readInt32LE(), 1343530892);
    }
  );
});
