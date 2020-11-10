import { Coprocessor, PolicyError } from "../modules/public/Coprocessor";
import { Handle } from "../modules/domain/Handle";
import { createRecordBatch } from "../modules/public/";

export const createMockCoprocessor = (
  globalId: Coprocessor["globalId"] = BigInt(1),
  inputTopics: Coprocessor["inputTopics"] = ["topicA"],
  policyError: Coprocessor["policyError"] = PolicyError.SkipOnFailure,
  apply: Coprocessor["apply"] = () => new Map([["result", createRecordBatch()]])
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
  filename: "file",
});
