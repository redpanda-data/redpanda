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

  getId = (): string => this.id;
  getTopic = (): string => this.ntp.topic;
  getRecords = (): CoprocessorRecordBatch[] => this.recordBatchReader.records;
}
