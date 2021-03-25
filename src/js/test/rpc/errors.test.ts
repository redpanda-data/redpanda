import error from "../../modules/rpc/errors";
import assert = require("assert");
describe("handle error", () => {
  it("should validated topic name", function () {
    const veryLongTopic =
      "abcdefghij_abcdefghij_abcdefghij_abcdefghij_abcdefghij_abcdefghij_abcdefghij_abcdefghij_abcdefghij_abcdefghij_abcdefghij_abcdefghij_abcdefghij_abcdefghij_abcdefghij_abcdefghij_abcdefghij_abcdefghij_abcdefghij_abcdefghij_abcdefghij_abcdefghij_abcdefghij_abcdefghij_abcdefghij_abcdefghij_";
    assert.ok(!error.validateKafkaTopicName("invalid<$"));
    assert.ok(error.validateKafkaTopicName("Valid.Topic"));
    assert.ok(error.validateKafkaTopicName("ValidTopic"));
    assert.ok(error.validateKafkaTopicName("Valid-Topic"));
    assert.ok(error.validateKafkaTopicName("Valid_Topic"));
    assert.ok(!error.validateKafkaTopicName("."));
    assert.ok(!error.validateKafkaTopicName(".."));
    assert.ok(!error.validateKafkaTopicName(veryLongTopic));
  });
});
