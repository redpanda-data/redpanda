import * as bytes from 'buffer'

export class SimplePod {
    constructor(x: number, y: number, z: any) {
        this.x = x;
        this.y = y;
        this.z = z;
    }

    print() {
        console.log('x: ' + this.x + '\n' +
            'y: ' + this.y + '\n' +
            'z: ' + this.z + '\n');
    }

    equals(pod: SimplePod) {
        return (this.x == pod.x &&
            this.y == pod.y &&
            this.z == pod.z);
    }

    x: number;
    y: number;
    z: bigint;
}

export class RpcHeader {
    constructor(version: number,
        headerChecksum: number,
        compression: number,
        payload: number,
        meta: number,
        correlationId: number,
        payloadChecksum: number) {
        this.version = version;
        this.headerChecksum = headerChecksum;
        this.compression = compression;
        this.payload = payload;
        this.meta = meta;
        this.correlationId = correlationId;
        this.payloadChecksum = payloadChecksum;
    }

    print() {
        console.log('Header:' + '\n' +
            'version:' + this.version + '\n' +
            'headerChecksum:' + this.headerChecksum + '\n' +
            'compression:' + this.compression + '\n' +
            'payload:' + this.payload + '\n' +
            'meta:' + this.meta + '\n' +
            'correlationId:' + this.correlationId + '\n' +
            'payloadChecksum:' + this.payloadChecksum);
    }

    equals(header: RpcHeader) {
        return (this.version == header.version &&
            this.headerChecksum == header.headerChecksum &&
            this.compression == header.compression &&
            this.payload == header.payload &&
            this.meta == header.meta &&
            this.correlationId == header.correlationId &&
            this.payloadChecksum == header.payloadChecksum);
    }

    version: number;
    headerChecksum: number;
    compression: number;
    payload: number;
    meta: number;
    correlationId: number;
    payloadChecksum: any;
}

export class Netbuf {
    constructor(header: RpcHeader, buffer: Buffer) {
        this.header = header;
        this.buffer = buffer;
    }

    header: RpcHeader;
    buffer: Buffer;
}

export class RecordBatchHeader {
    constructor(headerCrc: number,
        sizeBytes: number,
        baseOffset: bigint,
        recordBatchType: number,
        crc: number,
        attributes: number,
        lastOffsetDelta: number,
        firstTimestamp: bigint,
        maxTimestamp: bigint,
        producerId: bigint,
        producerEpoch: number,
        baseSequence: number,
        recordCount: number,
        termId: bigint) {
        this.headerCrc = headerCrc;
        this.sizeBytes = sizeBytes;
        this.baseOffset = baseOffset;
        this.recordBatchType = recordBatchType;
        this.crc = crc;
        this.attributes = attributes;
        this.lastOffsetDelta = lastOffsetDelta;
        this.firstTimestamp = firstTimestamp;
        this.maxTimestamp = maxTimestamp;
        this.producerId = producerId;
        this.producerEpoch = producerEpoch;
        this.baseSequence = baseSequence;
        this.recordCount = recordCount;
        this.termId = termId;
    }
    equals(header: RecordBatchHeader) {
        return this.sizeBytes == header.sizeBytes &&
            this.baseOffset == header.baseOffset &&
            this.recordBatchType == header.recordBatchType &&
            this.crc == header.crc &&
            this.attributes == header.attributes &&
            this.lastOffsetDelta == header.lastOffsetDelta &&
            this.firstTimestamp == header.firstTimestamp &&
            this.maxTimestamp == header.maxTimestamp &&
            this.producerEpoch == header.producerEpoch &&
            this.baseSequence == header.baseSequence &&
            this.recordCount == header.recordCount &&
            this.termId == header.termId;
    }

    size() {
        return 4 //headerCrc  
            + 4 // sizeBytes 
            + 8 //baseOffset 
            + 1 //recordBatchtype
            + 4 //crc
            + 2 //attributes 
            + 4 //lastoffsetdelta
            + 8 // firstTimestamp
            + 8 // maxtimestamp 
            + 8 //producerid
            + 2 // producerepoch 
            + 4 //basesequence
            + 4 //record count 
            + 8; //term id    
    }

    headerCrc: number;
    sizeBytes: number;
    baseOffset: bigint;
    recordBatchType: number;
    crc: number;
    attributes: number;
    lastOffsetDelta: number;
    firstTimestamp: bigint;
    maxTimestamp: bigint;
    producerId: bigint;
    producerEpoch: number;
    baseSequence: number;
    recordCount: number;
    termId: bigint;
}

export class BatchHeader {
    constructor(recordBatchHeader: RecordBatchHeader,
        isCompressed: number) {
        this.recordBatchHeader = recordBatchHeader;
        this.isCompressed = isCompressed;
    }

    size() {
        return this.recordBatchHeader.size()
            + 1; //isCompressed
    }

    recordBatchHeader: RecordBatchHeader;
    isCompressed: number;
}
