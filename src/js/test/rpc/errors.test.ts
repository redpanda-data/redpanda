import error from "../../modules/rpc/errors";
import assert = require("assert");
describe("handle error", () => {
  it("should validated topic name", function () {
    const invalid = error.validateKafkaTopicName("invalid.");
    const invalid2 = error.validateKafkaTopicName("invalid<$");
    const invalid3 = error.validateKafkaTopicName("ValidTopic");
    assert.strictEqual(invalid, false);
    assert.strictEqual(invalid2, false);
    assert.strictEqual(invalid3, true);
  });
});
