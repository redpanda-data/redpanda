import { RecordBatch } from "../public/Coprocessor";

interface NamespacedTopicPartition {
  topic: string;
}

interface RecordBatchReader {
  records: RecordBatch[];
}

export class Request {
  constructor(
    private ntp: NamespacedTopicPartition,
    private recordBatchReader: RecordBatchReader,
    private id: string
  ) {}

  getId = (): string => this.id;
  getTopic = (): string => this.ntp.topic;
  getRecords = (): RecordBatch[] => this.recordBatchReader.records;
}
