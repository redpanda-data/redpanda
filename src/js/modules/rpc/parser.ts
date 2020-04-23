import {
    RpcHeader, SimplePod, RecordBatch, RecordBatchHeader,
    BatchHeader
} from './types';
import * as bytes from 'buffer';
import {
    RpcHeaderCrc32, crcRecordBatchHeaderInternal,
    crcRecordBatch
} from '../hashing/crc32';
import { RpcXxhash64 } from '../hashing/xxhash';
import { strict as assert } from 'assert';

//The indices of the header
const hId = [0, 1, 5, 6, 10, 14, 18];
//The indices of the pod
const podId = [0, 4, 6, 10];
//The indices of a RecordBatchHeader
enum rbhId {
    headerCrc = 0,
    sizeBytes = 4,
    baseOffset = 8,
    recordBatchType = 16,
    crc = 17,
    attrs = 21,
    lastOffsetDelta = 23,
    firstTimestamp = 27,
    maxTimestamp = 35,
    producerId = 43,
    producerEpoch = 51,
    baseSequence = 53,
    recordCount = 57,
    termId = 61,
    isCompressed = 69
}
//Record RecordBatchHeader size
const rbhSz = 70;

export class Deserializer {
    constructor() { }

    rpcHeader(bytes: Buffer) {
        //RpcHeader is 26 bytes
        //bytes[0...25] 
        //get a subarray to header
        const head = bytes.subarray(0, 26);
        //parse the header
        const version: number = head.readUInt8(hId[0]);
        const headerChecksum: number = head.readUInt32LE(hId[1]);
        const compression: number = head.readUInt8(hId[2]);
        const payloadSize: number = head.readUInt32LE(hId[3]);
        const meta: number = head.readUInt32LE(hId[4]);
        const correlationId: number = head.readUInt32LE(hId[5]);
        const payloadChecksum: any = head.readBigUInt64LE(hId[6]);
        //calculate the crc of the header
        const crcVal = RpcHeaderCrc32(head);
        assert(crcVal == headerChecksum, "Error crc32 check failed");

        //return the header
        return new RpcHeader(version,
            headerChecksum,
            compression,
            payloadSize,
            meta,
            correlationId,
            payloadChecksum);
    }

    verifyPayload(payload: Buffer, payloadChecksum: bigint) {
        //index 26...bytes.length is the contents of iobuf
        //calculate the hash of payload
        const hashVal = RpcXxhash64(payload);
        //verify that checksum and xxhash are valid
        assert(hashVal == payloadChecksum, "Error xxhash check failed");
    }

    verifyRecordBatchHeaderInternal(header: RecordBatchHeader) {
        const crc = crcRecordBatchHeaderInternal(header);
        const expectedCrc = header.headerCrc;
        assert(crc == expectedCrc, "Error RecordBatchHeader internal checksum failed");
    }

    verifyRecordBatch(batch: RecordBatch) {
        const bytes = 4;
        let crc = Buffer.allocUnsafe(bytes);
        let expectedCrc = Buffer.allocUnsafe(bytes);
        crc.writeUInt32LE(crcRecordBatch(batch));
        expectedCrc.writeInt32LE(batch.header.crc);
        assert(crc.equals(expectedCrc), "Error RecordBatch checksum failed");
        this.verifyRecordBatchHeaderInternal(batch.header);
    }

    simplePod(bytes: Buffer) {
        const readBytes: number = bytes.readInt32LE(podId[0]);
        let x: number = bytes.readInt16LE(podId[1]);
        let y: number = bytes.readInt32LE(podId[2]);
        let z: bigint = bytes.readBigInt64LE(podId[3]);
        return new SimplePod(x, y, z);
    }

    recordBatchHeader(bytes: Buffer) {
        const headerCrc = bytes.readUInt32LE(rbhId.headerCrc);
        const sizeBytes = bytes.readInt32LE(rbhId.sizeBytes);
        const baseOffset = bytes.readBigInt64LE(rbhId.baseOffset);
        const recordBatchType = bytes.readInt8(rbhId.recordBatchType);
        const crc = bytes.readInt32LE(rbhId.crc);
        const attrs = bytes.readInt16LE(rbhId.attrs);
        const lastOffsetDelta = bytes.readInt32LE(rbhId.lastOffsetDelta);
        const firstTimestamp = bytes.readBigInt64LE(rbhId.firstTimestamp);
        const maxTimestamp = bytes.readBigInt64LE(rbhId.maxTimestamp);
        const producerId = bytes.readBigInt64LE(rbhId.producerId);
        const producerEpoch = bytes.readInt16LE(rbhId.producerEpoch);
        const baseSequence = bytes.readInt32LE(rbhId.baseSequence);
        const recordCount = bytes.readUInt32LE(rbhId.recordCount);
        const termId = bytes.readBigInt64LE(rbhId.termId);
        return new RecordBatchHeader(headerCrc, sizeBytes, baseOffset, recordBatchType,
            crc, attrs, lastOffsetDelta, firstTimestamp,
            maxTimestamp, producerId, producerEpoch, baseSequence,
            recordCount, termId);
    }

    batchHeader(bytes: Buffer) {
        const header: RecordBatchHeader = this.recordBatchHeader(bytes);
        const isCompressed: number = bytes.readInt8(rbhId.isCompressed);
        return new BatchHeader(header, isCompressed);
    }
}

export class Serializer {
    constructor() { }

    //preconditions:
    //1. header checksum has been calculated
    //2. payload checksum has been calculated
    rpcHeader(header: RpcHeader) {
        //RpcHeader is 26 bytes
        //The numbers are in bytes 
        const totalBytes: number = 1 +  //version
            4 +  //header_checksum
            1 +  //compression
            4 +  //payload size
            4 +  //meta 
            4 +  //correlation_id
            8;   //payload_checksum
        let buf: Buffer = Buffer.allocUnsafe(totalBytes);
        buf.writeUInt8(header.version, hId[0]);
        buf.writeUInt32LE(header.headerChecksum, hId[1]);
        buf.writeUInt8(header.compression, hId[2]);
        buf.writeUInt32LE(header.payload, hId[3]);
        buf.writeUInt32LE(header.meta, hId[4]);
        buf.writeUInt32LE(header.correlationId, hId[5]);
        buf.writeBigUInt64LE(header.payloadChecksum, hId[6]);
        return buf;
    }

    simplePod(pod: SimplePod) {
        const payloadSize: number = 2 + 4 + 8;
        const totalBytes: number = 4 + payloadSize;
        let buf: Buffer = Buffer.allocUnsafe(totalBytes);
        buf.writeInt32LE(payloadSize, podId[0]);
        buf.writeInt16LE(pod.x, podId[1]);
        buf.writeInt32LE(pod.y, podId[2]);
        buf.writeBigInt64LE(pod.z, podId[3]);
        return buf;
    }

    recordBatchHeader(header: RecordBatchHeader) {
        const totalBytes: number = 69;
        let buf: Buffer = Buffer.allocUnsafe(totalBytes);
        //to do write the bytes to fllow ;)
        buf.writeUInt32LE(header.headerCrc, rbhId.headerCrc);
        buf.writeInt32LE(header.sizeBytes, rbhId.sizeBytes);
        buf.writeBigInt64LE(header.baseOffset, rbhId.baseOffset);
        buf.writeInt8(header.recordBatchType, rbhId.recordBatchType);
        buf.writeInt32LE(header.crc, rbhId.crc);
        buf.writeInt16LE(header.attributes, rbhId.attrs);
        buf.writeInt32LE(header.lastOffsetDelta, rbhId.lastOffsetDelta);
        buf.writeBigInt64LE(header.firstTimestamp, rbhId.firstTimestamp);
        buf.writeBigInt64LE(header.maxTimestamp, rbhId.maxTimestamp);
        buf.writeBigInt64LE(header.producerId, rbhId.producerId);
        buf.writeInt16LE(header.producerEpoch, rbhId.producerEpoch);
        buf.writeInt32LE(header.baseSequence, rbhId.baseSequence);
        buf.writeUInt32LE(header.recordCount, rbhId.recordCount);
        buf.writeBigInt64LE(header.termId, rbhId.termId);
        return buf;
    }

    batchHeader(header: RecordBatchHeader) {
        let buf: Buffer = this.recordBatchHeader(header);
        const bytes: number = 1;
        let buf2: Buffer = Buffer.allocUnsafe(bytes);
        //write bytes to follow
        let isCompressed = 0;
        buf2.writeInt8(isCompressed);
        return Buffer.concat([buf, buf2]);
    }

}
