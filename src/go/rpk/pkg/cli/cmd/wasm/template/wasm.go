package template

const wasmJs = `const { SimpleTransform, PolicyError } = require("../vectorized");
const transform = new SimpleTransform();
/* Topics that fire the transform function */
transform.subscribe([
  /*\"input topics\"*/
]);
/* The strategy the transform engine will use when handling errors */
transform.errorHandler(PolicyError.SkipOnFailure);
/* Transform function */
transform.processRecord((recordBatch) => {
  /* Custom logic */
  const result = new Map();
  const transformedRecord = recordBatch.map(({ header, records }) => ({
    header,
    records: records.map((record) => ({
      ...record,
      value: Buffer.from("Change Record"),
    })),
  }));
  result.set("topic", transformedRecord);
  return result;
});
exports["default"] = transform;
`

func GetWasmJs() string {
	return wasmJs
}
