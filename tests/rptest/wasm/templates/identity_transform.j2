const {
  SimpleTransform,
  PolicyError,
} = require("@vectorizedio/wasm-api");
const transform = new SimpleTransform();
transform.subscribe([{{input_topics}}]);
transform.errorHandler(PolicyError.SkipOnFailure);
transform.processRecord((recordBatch) => {
  const result = new Map();
  {% for output_topic in output_topics %}
	result.set("{{output_topic}}", recordBatch);
	{% endfor %}
  return Promise.resolve(result);
});
exports["default"] = transform;
