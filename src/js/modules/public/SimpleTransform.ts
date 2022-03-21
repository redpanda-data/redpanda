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
  RecordBatch,
  Coprocessor,
  PolicyError,
  PolicyInjection,
} from "./Coprocessor";

export class SimpleTransform implements Coprocessor {
  subscribe(inputTopics: Coprocessor["inputTopics"]): void {
    this.inputTopics = inputTopics;
  }

  errorHandler(policyError: PolicyError): void {
    switch (policyError) {
      case PolicyError.Deregister:
        this.policyError = policyError;
        break;
      case PolicyError.SkipOnFailure:
        this.policyError = policyError;
        break;
      default:
        throw Error(
          `unexpected policy ${policyError}, valid: ${Object.keys(PolicyError)}`
        );
    }
  }

  processRecord(applyFunction: Coprocessor["apply"]): void {
    this.apply = applyFunction;
  }

  globalId: bigint;
  inputTopics: [string, PolicyInjection][];
  policyError: PolicyError;

  apply = (record: RecordBatch): Promise<Map<string, RecordBatch>> => {
    throw Error("processRecord isn't implemented yet");
  };
}
