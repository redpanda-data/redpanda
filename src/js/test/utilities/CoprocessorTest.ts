import {
  Coprocessor,
  RecordBatch,
  PolicyError,
} from "../../modules/public/Coprocessor";

class CoprocessorTest implements Coprocessor {
  globalId = 1;
  inputTopics = ["topicA"];
  policyError = PolicyError.Deregister;

  apply(record: RecordBatch): RecordBatch {
    return record;
  }
}

export default CoprocessorTest;
