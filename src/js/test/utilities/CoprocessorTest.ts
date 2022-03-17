/**
 * Copyright 2020 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

import {
  Coprocessor,
  RecordBatch,
  PolicyError,
  PolicyInjection,
} from "../../modules/public/Coprocessor";

function defaultInput(): [string, PolicyInjection][] {
  return [["topicA", PolicyInjection.Stored]];
}

class CoprocessorTest implements Coprocessor {
  globalId = BigInt(1);
  inputTopics = defaultInput();
  policyError = PolicyError.Deregister;

  apply(record: RecordBatch): Promise<Map<string, RecordBatch>> {
    return Promise.resolve(new Map([["test", record]]));
  }
}

export default CoprocessorTest;
