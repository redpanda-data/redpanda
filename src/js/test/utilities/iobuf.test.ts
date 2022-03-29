/**
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

import {
  FragmentIterator,
  fragmentSizes,
  IOBuf,
} from "../../modules/utilities/IOBuf";
import assert = require("assert");

describe("iobuf", () => {
  const readWrite = [
    {
      writeFnName: "appendInt8",
      readFnName: "readInt8",
      value: 2,
    },
    {
      writeFnName: "appendUInt8",
      readFnName: "readUInt8",
      value: 12,
    },
    {
      writeFnName: "appendInt16LE",
      readFnName: "readInt16LE",
      value: 2 ** 16 / 2 - 1,
    },
    {
      writeFnName: "appendUInt16LE",
      readFnName: "readUInt16LE",
      value: 2 ** 16 - 1,
    },
    {
      writeFnName: "appendInt32LE",
      readFnName: "readInt32LE",
      value: 2 ** 32 / 2 - 1,
    },
    {
      writeFnName: "appendUInt32LE",
      readFnName: "readUInt32LE",
      value: 2 ** 32 - 1,
    },
    {
      writeFnName: "appendBigInt64LE",
      readFnName: "readBigInt64LE",
      value: BigInt(2 ** 63 / 2 - 1),
    },
    {
      writeFnName: "appendBigUInt64LE",
      readFnName: "readBigUInt64LE",
      value: BigInt(2 ** 63 - 1),
    },
  ];

  const fillIOBuf = (iobuf: IOBuf, space: number) => {
    let count = 0;
    while (count <= space) {
      iobuf.appendInt8(1);
      count++;
    }
  };

  const readUntil = (iobuf: FragmentIterator, space: number) => {
    let count = 0;
    while (count <= space) {
      iobuf.readInt8();
      count++;
    }
  };

  it("should create IOBuf without fragments", () => {
    const iobuf = new IOBuf();
    assert.strictEqual(iobuf.length(), 0);
  });

  it("should add first fragment when writing in IOBuf", () => {
    const iobuf = new IOBuf();
    assert.strictEqual(iobuf.length(), 0);
    iobuf.appendInt8(1);
    assert.strictEqual(iobuf.length(), 1);
  });

  it("should add a fragment when the current fragment is fill", () => {
    const iobuf = new IOBuf();
    assert.strictEqual(iobuf.length(), 0);
    iobuf.appendInt8(0);
    assert.strictEqual(iobuf.length(), 1);
    assert.strictEqual(iobuf.getFragmentByIndex(0).getSize(), 512);
    fillIOBuf(iobuf, 511);
    assert.strictEqual(iobuf.length(), 2);
    assert.strictEqual(iobuf.getFragmentByIndex(1).getSize(), 768);
  });

  it("should add a new fragment with expected sizes", () => {
    const iobuf = new IOBuf();
    let index = 0;
    fragmentSizes.forEach((value) => {
      fillIOBuf(iobuf, value - 2);
      assert.strictEqual(iobuf.getFragmentByIndex(index).getSize(), value);
      assert.strictEqual(iobuf.length(), index + 1);
      index++;
    });
  });

  describe("iobuf with enough space for write and read operation", () => {
    readWrite.forEach(function (t) {
      it(`should ${t.writeFnName} and ${t.writeFnName} into iobuf`, () => {
        const iobuf = new IOBuf();
        iobuf[t.writeFnName](t.value);
        const fragmentIterable = iobuf.getIterable();
        const read = fragmentIterable[t.readFnName]();
        assert.strictEqual(read, t.value);
      });
    });

    it("should write and read string", () => {
      const iobuf = new IOBuf();
      const string = "testImportant";
      const size = Buffer.byteLength(string);
      iobuf.appendString(string);
      const fragmentIterable = iobuf.getIterable();
      assert.strictEqual(fragmentIterable.toString(size), string);
    });

    it("should write and read Buffer", () => {
      const iobuf = new IOBuf();
      const buffer = Buffer.from("123-test-123");
      iobuf.appendBuffer(buffer);
      const fragmentIterable = iobuf.getIterable();
      const bufferResult = fragmentIterable.slice(buffer.length);
      assert(bufferResult.equals(buffer));
    });
  });

  describe("iobuf without enough space for write and read operation", () => {
    readWrite.forEach(function (t) {
      it(`should ${t.writeFnName} and ${t.writeFnName} into iobuf `, () => {
        const iobuf = new IOBuf();
        fillIOBuf(iobuf, 510);
        iobuf[t.writeFnName](t.value);
        const fragmentIterable = iobuf.getIterable();
        readUntil(fragmentIterable, 510);
        const read = fragmentIterable[t.readFnName](510);
        assert.strictEqual(read, t.value);
      });
    });

    it("should write and read string", () => {
      const iobuf = new IOBuf();
      const string = "testImportant";
      fillIOBuf(iobuf, 510);
      const size = Buffer.byteLength(string);
      iobuf.appendString(string);
      const fragmentIterable = iobuf.getIterable();
      readUntil(fragmentIterable, 510);
      assert.strictEqual(fragmentIterable.toString(size), string);
    });

    it("should write and read Buffer", () => {
      const iobuf = new IOBuf();
      const buffer = Buffer.from("123-test-123");
      fillIOBuf(iobuf, 510);
      iobuf.appendBuffer(buffer);
      const fragmentIterable = iobuf.getIterable();
      readUntil(fragmentIterable, 510);
      const bufferResult = fragmentIterable.slice(buffer.length);
      assert(bufferResult.equals(buffer));
    });
  });

  it(`should keep an offset after write into iobuf`, () => {
    const iobuf = new IOBuf();
    iobuf.appendInt8(8);
    iobuf.appendInt32LE(32);
    iobuf.appendBigInt64LE(BigInt(64));
    const fragmentIterable = iobuf.getIterable();
    assert.strictEqual(fragmentIterable.readInt8(), 8);
    assert.strictEqual(fragmentIterable.readInt32LE(), 32);
    assert.strictEqual(fragmentIterable.readBigInt64LE(), BigInt(64));
  });

  it("should allow to slice an iobuf", () => {
    const iobuf = new IOBuf();
    const string = "123-test-123";
    iobuf.appendString(string);
    const fragmentIterable = iobuf.getIterable();
    const result = fragmentIterable.slice(Buffer.byteLength(string));
    assert.strictEqual(string, result.toString());
    assert.strictEqual(result.byteLength, Buffer.byteLength(string));
  });

  it("should allow slice an iobuf even if the slice take 2 fragment", () => {
    const iobuf = new IOBuf();
    const string = "123-test-123";
    fillIOBuf(iobuf, 510);
    iobuf.appendString(string);
    const fragmentIterable = iobuf.getIterable();
    readUntil(fragmentIterable, 510);
    const result = fragmentIterable.slice(Buffer.byteLength(string));
    assert.strictEqual(string, result.toString());
  });

  it("should iterate IOBuf with forEach", () => {
    const iob = new IOBuf();
    // write int until having 3 fragments in iobuf
    fillIOBuf(iob, 2400);
    const bufferInBuffer = [];
    iob.forEach((fragment) => bufferInBuffer.push(fragment));
    assert.strictEqual(iob.length(), bufferInBuffer.length);
    bufferInBuffer.forEach((fragment, index) => {
      assert.deepStrictEqual(fragment, iob.getFragmentByIndex(index));
    });
  });

  it("should create reserve in IOBuf", () => {
    const iobuf = new IOBuf();
    const reserve = iobuf.getReserve(8);
    iobuf.appendInt32LE(400);
    reserve.appendInt32LE(200);
    reserve.appendInt32LE(200);
    const fragmentIterable = iobuf.getIterable();
    assert.strictEqual(fragmentIterable.readInt32LE(), 200);
    assert.strictEqual(fragmentIterable.readInt32LE(), 200);
    assert.strictEqual(fragmentIterable.readInt32LE(), 400);
  });

  it("should totalChainSize return a writing bytes in IOBuf", function () {
    const iobuf = new IOBuf();
    assert.strictEqual(iobuf.totalChainSize(), 0);
    iobuf.appendInt32LE(1);
    assert.strictEqual(iobuf.totalChainSize(), 4);
    iobuf.appendBigInt64LE(BigInt(1));
    assert.strictEqual(iobuf.totalChainSize(), 12);
    iobuf.appendBigInt64LE(BigInt(1));
    assert.strictEqual(iobuf.totalChainSize(), 20);
    const string = "test".repeat(150); // <- 600 string bytes
    const stringSize = Buffer.byteLength(string);
    assert.strictEqual(stringSize, 600);
    iobuf.appendString(string);
    assert.strictEqual(iobuf.totalChainSize(), 620);
  });
});
