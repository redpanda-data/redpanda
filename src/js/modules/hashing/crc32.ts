import { calculate } from 'fast-crc32c'

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
