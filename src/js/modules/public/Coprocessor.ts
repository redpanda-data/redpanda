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
    Deregister
}

interface CoprocessorRecordHeader {
    keySize: number;
    key: Buffer;
    valSize: number;
    value: Buffer;
}

interface CoprocessorRecordsHeader {
    attributes: number;
    lastOffsetDelta: number;
    firstTimestamp: bigint;
    maxTimestamp: bigint;
    producerId: bigint;
    producerEpoch: number;
    baseSequence: number;
    recordCount: number;
}

interface CoprocessorRecord {
    sizeBytes: number;
    recordAttributes: number;
    timestampDelta: number;
    offsetDelta: number;
    keySize: number;
    key: Buffer;
    valSize: number;
    value: Buffer;
    headers: Array<CoprocessorRecordHeader>;
    size(): number
}

interface CoprocessorRecordBatch {
    records: CoprocessorRecord[]
    header: CoprocessorRecordsHeader
}

interface Coprocessor {
    inputTopics: string[]
    policyError: PolicyError
    globalId: number
    apply(record: CoprocessorRecordBatch): CoprocessorRecordBatch
}

export {
    CoprocessorRecordsHeader,
    CoprocessorRecordHeader,
    CoprocessorRecord,
    CoprocessorRecordBatch,
    Coprocessor
}
