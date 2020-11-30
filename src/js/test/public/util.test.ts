import {
  calculateRecordBatchSize,
  calculateRecordLength,
} from "../../modules/public";
import { Record } from "../../modules/public/Coprocessor";
import { varintZigzagSize } from "../../modules/public/Utils";
import * as assert from "assert";

describe("Public utils functions", function () {
  it("should calculate number byte size on varint encode", function () {
    const size = varintZigzagSize(BigInt(50)); // zigzag: 100
    assert.strictEqual(size, 1);
    const size2 = varintZigzagSize(BigInt(200)); // zigzag: 400
    assert.strictEqual(size2, 2);
    const size3 = varintZigzagSize(BigInt(30000)); // zigzag: 60000
    assert.strictEqual(size3, 3);
    const size4 = varintZigzagSize(BigInt(10000000)); // zigzag: 20000000
    assert.strictEqual(size4, 4);
  });

  it("should calculateRecordLength for record without header", function () {
    const value = Buffer.from("Test");
    /**
     * value (4 bytes)
     * header (1 byte)
     * key (0 byte)
     * offsetDelta (1 byte)
     * timestampDelta (1 byte)
     * keyLength (1 byte)
     * attributes (1 byte)
     * valueLen (1 byte)
     * Total = 10 bytes
     */
    const record: Record = {
      value: value,
      headers: [],
      key: Buffer.from(""),
      offsetDelta: 0,
      timestampDelta: BigInt(0),
      keyLength: 0,
      attributes: 0,
      valueLen: value.length,
      length: 0,
    };
    const size = calculateRecordLength(record);
    assert.strictEqual(size, 10);
  });

  it("should calculateRecordLength for record with header", function () {
    const value = Buffer.from("Test");
    /**
     * value (4 bytes)
     * header (1 byte)
     * key (0 byte)
     * offsetDelta (1 byte)
     * timestampDelta (1 byte)
     * keyLength (1 byte)
     * attributes (1 byte)
     * valueLen (1 byte)
     * Total = 10 bytes
     */
    const record: Record = {
      value: value,
      headers: [
        {
          headerKey: Buffer.from("key"),
          headerKeyLength: 3,
          headerValueLength: 5,
          value: Buffer.from("value"),
        },
        {
          headerKey: Buffer.from("key"),
          headerKeyLength: 3,
          headerValueLength: 5,
          value: Buffer.from("value"),
        },
      ],
      key: Buffer.from(""),
      offsetDelta: 0,
      timestampDelta: BigInt(0),
      keyLength: 0,
      attributes: 0,
      valueLen: value.length,
      length: 0,
    };
    const size = calculateRecordLength(record);
    assert.strictEqual(size, 30);
  });

  it("should calculate record batch size", function () {
    const value = Buffer.from("Test");
    const record: Record = {
      value: value,
      headers: [],
      key: Buffer.from(""),
      offsetDelta: 0,
      timestampDelta: BigInt(0),
      keyLength: 0,
      attributes: 0,
      length: 10,
      valueLen: value.length,
    };
    const size = calculateRecordBatchSize([record]);
    // 61 bytes from record batch header and 11 bytes from record
    assert.strictEqual(size, 72);
  });
});
