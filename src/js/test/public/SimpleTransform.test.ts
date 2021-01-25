import { createRecordBatch, SimpleTransform } from "../../modules/public";
import {
  PolicyError,
  Coprocessor,
  RecordBatch,
} from "../../modules/public/Coprocessor";
import assert = require("assert");

describe("SimpleTransform", function () {
  const transform: Coprocessor["apply"] = (record) => {
    const map: Map<string, RecordBatch> = new Map();
    map.set("result", {
      header: record.header,
      records: record.records.map((r) => ({
        ...r,
        value: Buffer.from("newRecord"),
      })),
    });
    return map;
  };

  it("should create simpleTransform and set all values", function () {
    const simpleTransform = new SimpleTransform();
    const topics = ["topicA"];
    simpleTransform.subscribe(topics);
    simpleTransform.errorHandler(PolicyError.Deregister);
    simpleTransform.processRecord(transform);

    assert.strictEqual(simpleTransform.apply, transform);
    assert.strictEqual(simpleTransform.apply, transform);
    assert.strictEqual(simpleTransform.policyError, PolicyError.Deregister);

    simpleTransform.errorHandler(PolicyError.SkipOnFailure);
    assert.strictEqual(simpleTransform.policyError, PolicyError.SkipOnFailure);
  });

  it("should fail if the apply function isn't set", function () {
    const simpleTransform = new SimpleTransform();
    try {
      simpleTransform.apply(createRecordBatch());
    } catch (e) {
      assert.strictEqual(e.message, "processRecord isn't implemented yet");
    }
  });

  it("should fail set a unexpected policyError", function () {
    const simpleTransform = new SimpleTransform();
    try {
      // eslint-disable-next-line @typescript-eslint/ban-ts-comment
      // @ts-ignore
      simpleTransform.errorHandler("No fails never");
    } catch (e) {
      assert.strictEqual(
        e.message,
        "unexpected policy No fails never, " +
          "valid: 0,1,SkipOnFailure,Deregister"
      );
    }
  });
});
