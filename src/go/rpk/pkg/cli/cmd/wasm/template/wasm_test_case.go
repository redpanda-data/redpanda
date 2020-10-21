package template

const wasmTestJS = `const transform = require("../src/wasm");
const { createRecordBatch } = require("../vectorized");
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
