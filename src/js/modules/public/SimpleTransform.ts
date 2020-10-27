import { RecordBatch, Coprocessor, PolicyError } from "./Coprocessor";

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
  inputTopics: string[];
  policyError: PolicyError;

  apply(record: RecordBatch): RecordBatch {
    throw Error("processRecord isn't implemented yet");
  }
}
