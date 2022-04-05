// Copyright 2020 Redpanda Data, Inc.
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

const record = createRecordBatch({records: [{value: Buffer.from("test")}]});

describe("transform", () => {
  it("should apply function", function() {
    // apply function returns a Promise<Map<string, RecordBatch>>
    return transform.default.apply(record)
      .then((resultApply) => {
        assert.strictEqual(resultApply.size, 1);
        assert(resultApply.get("result"));
        assert(!resultApply.get("unExpectedTopic"));
        resultApply.get("result").records.forEach(record => {
          assert.deepStrictEqual(record.value, Buffer.from("TEST"))
        })
      })
  });
});`

func WasmTestJs() string {
	return wasmTestJS
}
