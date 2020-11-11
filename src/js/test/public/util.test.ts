import {
  calculateRecordBatchSize,
  calculateRecordLength
} from "../../modules/public";
import {
  Record
} from "../../modules/public/Coprocessor";
import { varintZigzagSize } from "../../modules/public/Utils";
import * as assert from "assert";

describe("Public utils functions", function() {
  it("should calculate number byte size on varint encode", function() {
    const size = varintZigzagSize(BigInt(50)); // zigzag: 100
    assert.strictEqual(size, 1);
    const size2 = varintZigzagSize(BigInt(200)); // zigzag: 400
    assert.strictEqual(size2, 2);
    const size3 = varintZigzagSize(BigInt(30000)); // zigzag: 60000
    assert.strictEqual(size3, 3);
    const size4 = varintZigzagSize(BigInt(10000000)); // zigzag: 20000000
    assert.strictEqual(size4, 4);
  });

  it("should calculateRecordLength", function() {
    const value = Buffer.from("Test");
    const record: Record = {
      value: value,                 // 4  |
      headers: [],                  // 1  |
      key: Buffer.from(""),         // 0  |
      offsetDelta: 0,               // 1  |
      timestampDelta: BigInt(0),    // 1  |
      keyLength: 0,                 // 1  |
      attributes: 0,                // 1  |
      length: 0,                    // 1  |
      valueLen: value.length        // 1 => 11
    };
    console.log(record);
    const size = calculateRecordLength(record);
    assert.strictEqual(size, 11);
  });

  it("should calculate record batch size", function() {
    const value = Buffer.from("Test");
    const record: Record = {
      value: value,
      headers: [],
      key: Buffer.from(""),
      offsetDelta: 0,
      timestampDelta: BigInt(0),
      keyLength: 0,
      attributes: 0,
      length: 11,
      valueLen: value.length
    };
    const size = calculateRecordBatchSize([record]);
    // 61 bytes from record batch header and 11 bytes from record
    assert.strictEqual(size, 72)
  });
});
