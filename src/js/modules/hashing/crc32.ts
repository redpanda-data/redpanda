import { calculate } from 'fast-crc32c'
import { RecordBatchHeader } from '../rpc/types'

//The byte indexes used by crcRecordbatchheaderinternal function
enum rbhiId {
    //each field in this number is the corresponding type size 
    totalSize = 4 + 8 + 1 + 4 + 2 + 4 + 8 + 8 + 8 + 2 + 4 + 4,
    sizeBytes = 0,
    baseOffset = 4,
    recordBatchType = 12,
    crc = 13,
    attributes = 17,
    lastOffsetDelta = 19,
    firstTimestamp = 23,
    maxTimestamp = 31,
    producerId = 39,
    producerEpoch = 47,
    baseSequence = 49,
    recordCount = 53
}

export function RpcHeaderCrc32(header: Buffer) {
    //payload_size 4 bytes
    //meta 4 bytes
    //correlation_id 4 bytes
    //payload_checksum 8 bytes
    let ranges: number[][] = [[5, 6], [6, 10], [10, 14], [14, 18], [18, 26]];
    let init: number = 0;
    return ranges.reduce((value, id) =>
        calculate(header.subarray(id[0], id[1]), value), init);
}

//precondition buffer.length >= 8
function toLe64(val: bigint, buff: Buffer, begin: number) {
    buff.writeBigInt64LE(val, begin);
}

//precondition buffer.length >= 8
function toLeU64(val: bigint, buff: Buffer, begin: number) {
    buff.writeBigUInt64LE(val, begin);
}

//precondition buffer.length >= 4
function toLe32(val: number, buff: Buffer, begin: number) {
    buff.writeInt32LE(val, begin);
}

//precondition buffer.length >= 4
function toLeU32(val: number, buff: Buffer, begin: number) {
    buff.writeUInt32LE(val, begin);
}

//precondition buffer.length >= 2
function toLe16(val: number, buff: Buffer, begin: number) {
    buff.writeInt16LE(val, begin);
}

//precondition buffer.length >= 2
function toLeU16(val: number, buff: Buffer, begin: number) {
    buff.writeUInt16LE(val, begin);
}

//precondition buffer.length >= 1
function toLe8(val: number, buff: Buffer, begin: number) {
    buff.writeInt8(val, begin);
}

export function crcRecordBatchHeaderInternal(header: RecordBatchHeader) {
    let buff = Buffer.allocUnsafe(rbhiId.totalSize);
    toLe32(header.sizeBytes, buff, rbhiId.sizeBytes);
    toLe64(header.baseOffset, buff, rbhiId.baseOffset);
    toLe8(header.recordBatchType, buff, rbhiId.recordBatchType);
    toLe32(header.crc, buff, rbhiId.crc);
    toLeU16(header.attributes, buff, rbhiId.attributes);
    toLe32(header.lastOffsetDelta, buff, rbhiId.lastOffsetDelta);
    toLe64(header.firstTimestamp, buff, rbhiId.firstTimestamp);
    toLe64(header.maxTimestamp, buff, rbhiId.maxTimestamp);
    toLe64(header.producerId, buff, rbhiId.producerId);
    toLe16(header.producerEpoch, buff, rbhiId.producerEpoch);
    toLe32(header.baseSequence, buff, rbhiId.baseSequence);
    toLeU32(header.recordCount, buff, rbhiId.recordCount);
    return calculate(buff, 0);
}
