/**
 * Policy Error define what does the coprocessor function
 * do when the apply function into Coprocessor class fail.
 * SkipOnFailure: the current record is lose but the function
 *                can run the next record
 * Deregister: the coprocessor function will deregister from
 *             the function batch, it won't apply to any next records
 */
export enum PolicyError {
  SkipOnFailure,
  Deregister,
}

interface RecordHeader {
  headerKeyLength: bigint;
  headerKey: string;
  headerValueLength: bigint;
  value: Buffer;
}

interface RecordBatchHeader {
  headerCrc: number;
  sizeBytes: number;
  baseOffset: bigint;
  recordBatchType: number;
  crc: number;
  attrs: number;
  lastOffsetDelta: number;
  firstTimestamp: bigint;
  maxTimestamp: bigint;
  producerId: bigint;
  producerEpoch: number;
  baseSequence: number;
  recordCount: number;
}

interface Record {
  length: bigint;
  attributes: number;
  timestampDelta: bigint;
  offsetDelta: bigint;
  keyLength: bigint;
  key: Buffer;
  valueLen: bigint;
  value: Buffer;
  headers: Array<RecordHeader>;
}

interface RecordBatch {
  records: Record[];
  header: RecordBatchHeader;
}

interface Coprocessor {
  inputTopics: string[];
  policyError: PolicyError;
  globalId: number;
  apply(record: RecordBatch): RecordBatch;
}

export { RecordBatchHeader, RecordHeader, Record, RecordBatch, Coprocessor };
