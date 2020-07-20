import { CoprocessorRecordBatch } from "../public/Coprocessor";

interface NamespacedTopicPartition {
  topic: string;
}

interface RecordBatchReader {
  records: CoprocessorRecordBatch[];
}

export class CoprocessorRequest {
  constructor(
    private ntp: NamespacedTopicPartition,
    private recordBatchReader: RecordBatchReader,
    private id: string
  ) {}

  getId = () => this.id;
  getTopic = () => this.ntp.topic;
  getRecords = () => this.recordBatchReader.records;
}
