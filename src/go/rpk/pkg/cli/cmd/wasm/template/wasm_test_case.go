// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package template

const wasmTestJS = `const transform = require("../src/wasm");
const { createRecordBatch } = require("@vectorizedio/wasm-api");
const assert = require("assert");

const record = createRecordBatch();

describe("transform", () => {
  it("should apply function", function() {
    const result = transform.default.apply(record);
    assert.strictEqual(result.size, 1);
    assert(result.get("topic"));
    assert(!result.get("unExpectedTopic"));
    result.get("topic").records.forEach(record => {
      assert.strictEqual(record.value, Buffer.from("Changed"))
    })
  });
});`

func GetWasmTestJs() string {
	return wasmTestJS
}
