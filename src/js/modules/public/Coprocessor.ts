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
  keySize: number;
  key: Buffer;
  valSize: number;
  value: Buffer;
}

interface RecordsHeader {
  attributes: number;
  lastOffsetDelta: number;
  firstTimestamp: bigint;
  maxTimestamp: bigint;
  producerId: bigint;
  producerEpoch: number;
  baseSequence: number;
  recordCount: number;
}

interface Record {
  sizeBytes: number;
  recordAttributes: number;
  timestampDelta: number;
  offsetDelta: number;
  keySize: number;
  key: Buffer;
  valSize: number;
  value: Buffer;
  headers: Array<RecordHeader>;
  size(): number;
}

interface RecordBatch {
  records: Record[];
  header: RecordsHeader;
}

interface Coprocessor {
  inputTopics: string[];
  policyError: PolicyError;
  globalId: number;
  apply(record: RecordBatch): RecordBatch;
}

export { RecordsHeader, RecordHeader, Record, RecordBatch, Coprocessor };
