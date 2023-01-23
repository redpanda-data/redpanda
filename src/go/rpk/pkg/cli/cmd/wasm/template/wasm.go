// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package template

const wasmJs = `const {
  SimpleTransform,
  PolicyError,
  PolicyInjection
} = require("@redpanda-data/wasm-api");
const transform = new SimpleTransform();
/* Topics that fire the transform function */
transform.subscribe([["test-topic", PolicyInjection.Stored]]);
/* The strategy the transform engine will use when handling errors */
transform.errorHandler(PolicyError.SkipOnFailure);
/* Auxiliar transform function for records */
const uppercase = (record) => {
  const newRecord = {
    ...record,
    value: record.value.map((char) => {
      if (char >= 97 && char <= 122) {
        return char - 32;
      } else {
        return char;
      }
    }),
  };
  return newRecord;
}
/* Transform function */
transform.processRecord((recordBatch) => {
  const result = new Map();
  const transformedRecord = recordBatch.map(({ header, records }) => {
    return {
      header,
      records: records.map(uppercase),
    };
  });
  result.set("result", transformedRecord);
  // processRecord function returns a Promise
  return Promise.resolve(result);
});
exports["default"] = transform;
`

func WasmJs() string {
	return wasmJs
}
