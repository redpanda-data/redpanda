import {
  Record,
  RecordBatch,
  RecordBatchHeader,
  RecordHeader,
} from "./Coprocessor";

const createHeader = (
  header: Partial<RecordBatchHeader>
): RecordBatchHeader => {
  return {
    attrs: 0,
    baseOffset: BigInt(0),
    baseSequence: 0,
    crc: 0,
    firstTimestamp: BigInt(0),
    headerCrc: 0,
    lastOffsetDelta: 0,
    maxTimestamp: BigInt(0),
    producerEpoch: 0,
    producerId: BigInt(0),
    recordBatchType: 0,
    recordCount: 0,
    sizeBytes: 0,
    term: BigInt(0),
    isCompressed: 0,
    ...header,
  };
};

const createRecordHeader = (
  recordHeader: Partial<RecordHeader>
): RecordHeader => {
  return {
    headerKey: "",
    headerKeyLength: BigInt(0),
    headerValueLength: BigInt(0),
    value: Buffer.from(""),
    ...recordHeader,
  };
};

const createRecord = (record: Partial<Record>): Record => {
  const headers = record?.headers || [];
  return {
    attributes: 0,
    key: Buffer.from(""),
    keyLength: 0,
    length: 0,
    offsetDelta: 0,
    timestampDelta: BigInt(0),
    value: Buffer.from(""),
    valueLen: 0,
    ...record,
    headers: headers.map(createRecordHeader),
  };
};

interface PartialRecordBatch {
  header?: Partial<RecordBatchHeader>;
  records?: Partial<Record>[];
}

interface RecordBatchFunctor extends RecordBatch {
  map(fn: (record) => RecordBatch): RecordBatch;
}

export const createRecordBatch = (
  record?: PartialRecordBatch
): RecordBatchFunctor => {
  const map = (record: RecordBatch) => (
    fn: (record) => RecordBatch
  ): RecordBatch => {
    return fn(record);
  };
  const records = record?.records || [];
  const resultRecord = {
    header: createHeader(record?.header || {}),
    records: records.map(createRecord),
  };
  return {
    ...resultRecord,
    map: map(resultRecord),
  };
};

export const createRecordBatchFunctor = (
  record: RecordBatch
): RecordBatchFunctor => {
  const map = (fn: (record) => RecordBatch): RecordBatch => {
    return fn(record);
  };
  return { ...record, map };
};
