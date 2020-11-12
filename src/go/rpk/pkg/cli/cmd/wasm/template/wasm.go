// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

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
