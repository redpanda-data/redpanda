/**
 * Copyright 2020 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

import { Class1, Class2, Class3 } from "./generated";
import * as assert from "assert";
import { IOBuf } from "../../../src/js/modules/utilities/IOBuf";

const classSigned: Class2 = {
  numberSigned8: 8,
  numberSigned16: 24,
  numberSigned32: 123,
};

const classUSigned: Class3 = {
  numberUSigned8: 255,
  numberUSigned16: 65535,
  numberUSigned32: 4294967295,
  optional: undefined,
};

const class1: Class1 = {
  bigint: BigInt(1234567890),
  arrayValue: ["value 1", "value 2"],
  booleanValue: true,
  bufferValue: Buffer.from("Buffer Value"),
  stringValue: "value 3",
  varintValue: BigInt(123789),
  classSigned: classSigned,
  classUSigned: classUSigned,
  mixCustomType: [undefined, "string", undefined, undefined, "other string"],
};

// Create buffer where the binary data is going to be save
const io = new IOBuf();
// Write into the buffer
const size = Class1.toBytes(class1, io);
// Read data from the buffer
const bufferResult = io.getIterable().slice(size);
const [result, resultSize] = Class1.fromBytes(bufferResult);

// check result size
assert.strictEqual(size, resultSize);
// Check data
assert.strictEqual(class1.stringValue, result.stringValue);
assert.strictEqual(class1.booleanValue, result.booleanValue);
assert.strictEqual(class1.varintValue, result.varintValue);
// check class 2, with signed values
assert.strictEqual(
  class1.classSigned.numberSigned32,
  result.classSigned.numberSigned32
);
assert.strictEqual(
  class1.classSigned.numberSigned8,
  result.classSigned.numberSigned8
);
// check class 3, with usigned values
assert.strictEqual(
  class1.classUSigned.numberUSigned8,
  result.classUSigned.numberUSigned8
);

assert.strictEqual(
  class1.classUSigned.numberUSigned16,
  result.classUSigned.numberUSigned16
);

assert.deepStrictEqual(class1.mixCustomType, result.mixCustomType);

console.log("All generated code was encoded and decoded as expected");
