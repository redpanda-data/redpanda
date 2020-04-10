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
