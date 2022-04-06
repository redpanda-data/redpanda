/**
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

import {
  Coprocessor,
  PolicyError,
  PolicyInjection,
} from "../modules/public/Coprocessor";
import { Handle } from "../modules/domain/Handle";
import { createRecordBatch } from "../modules/public/";

export const createMockCoprocessor = (
  globalId: Coprocessor["globalId"] = BigInt(1),
  inputTopics: Coprocessor["inputTopics"] = [
    ["topicA", PolicyInjection.Stored],
  ],
  policyError: Coprocessor["policyError"] = PolicyError.SkipOnFailure,
  apply: Coprocessor["apply"] = () =>
    Promise.resolve(new Map([["result", createRecordBatch()]]))
): Coprocessor => ({
  globalId,
  inputTopics,
  policyError,
  apply,
});

export const createHandle = (coprocessor?: Partial<Coprocessor>): Handle => ({
  coprocessor: createMockCoprocessor(
    coprocessor?.globalId,
    coprocessor?.inputTopics,
    coprocessor?.policyError,
    coprocessor?.apply
  ),
  checksum: "check",
});
