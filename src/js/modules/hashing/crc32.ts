import { calculate } from 'fast-crc32c'

export function RpcHeaderCrc32(header: Buffer) {
    //payload_size 4 bytes
    //meta 4 bytes
    //correlation_id 4 bytes
    //payload_checksum 8 bytes
    let ranges: number[][] = [[5, 6], [6, 10], [10, 14], [14, 18], [18, 26]];
    let init: number = 0;
    return ranges.reduce((value, id) => calculate(header.subarray(id[0], id[1])), init);
}
