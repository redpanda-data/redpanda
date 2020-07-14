import {Coprocessor, CoprocessorRecordBatch, PolicyError} from "../../modules/public/Coprocessor";

class CoprocessorTest implements Coprocessor {
  globalId = 1;
  inputTopics = ["topicA"];
  policyError =  PolicyError.Deregister;

  apply(record: CoprocessorRecordBatch): CoprocessorRecordBatch {
    return record;
  }
}

export default CoprocessorTest