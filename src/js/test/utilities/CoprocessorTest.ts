import {
  Coprocessor,
  RecordBatch,
  PolicyError,
} from "../../modules/public/Coprocessor";

class CoprocessorTest implements Coprocessor {
  globalId = BigInt(1);
  inputTopics = ["topicA"];
  policyError = PolicyError.Deregister;

  apply(record: RecordBatch): Map<string, RecordBatch> {
    return new Map([["test", record]]);
  }
}

export default CoprocessorTest;
