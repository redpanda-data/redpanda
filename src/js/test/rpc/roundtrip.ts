import { strict as assert } from 'assert';
import { Serializer, Deserializer } from '../../modules/rpc/parser';
import { RpcHeader, SimplePod } from '../../modules/rpc/types';
import { RpcHeaderCrc32 } from '../../modules/hashing/crc32';
import { RpcXxhash64 } from '../../modules/hashing/xxhash';
import 'mocha'

function makeHeader() {
    //some random values
    //It's crc32 is = 1774429187
    //If you change the values of this header
    //Please change the const value in the crc32 test case below
    return new RpcHeader(1, 0, 0, 1, 2, 3, 0);
}

function makeSimplePod() {
    let buf: Buffer = Buffer.allocUnsafe(8);
    buf.writeBigInt64LE(BigInt(3), 0);
    return new SimplePod(1, 2, buf.readBigInt64LE(0));
}

function makeSmallBuff() {
    let buf: Buffer = Buffer.allocUnsafe(4);
    buf.writeInt32LE(4);
    return buf;
}

describe('RPC',
    function() {
        describe('roundtrip simple_pod',
            function() {
                it('Assert should fail if serializer output differs',
                    function() {
                        const header = makeHeader();
                        const simplePod = makeSimplePod();
                        const serializer = new Serializer();
                        //serialization starts here
                        let serializedPod = serializer.simplePod(simplePod);
                        //get payload hash
                        const hash = RpcXxhash64(serializedPod);
                        //assign hash to payload
                        header.payloadChecksum = hash;
                        //serialize header
                        const serializedHeader = serializer.rpcHeader(header);
                        //get the crc and assign the checksum
                        let crc32 = RpcHeaderCrc32(serializedHeader);
                        header.headerChecksum = crc32;
                        //write the serialized value
                        serializedHeader.writeUInt32LE(crc32, 1);
                        //start deserialization
                        let deserializer = new Deserializer();
                        let resultHeader = deserializer.rpcHeader(serializedHeader);
                        //verify payload
                        deserializer.verifyPayload(serializedPod, resultHeader.payloadChecksum);
                        //deserialize payload
                        let resultPod = deserializer.simplePod(serializedPod);
                        //assert the data is the same 
                        assert(header.equals(resultHeader));
                        assert(simplePod.equals(resultPod));
                    });
            });
        describe('try to deserialize Header with less bytes',
            function() {
                it('should throw and catch',
                    function() {
                        let buf = makeSmallBuff();
                        let deserializer = new Deserializer();
                        try {
                            deserializer.rpcHeader(buf);
                            assert(false);
                        }
                        catch (err) {
                            assert(true);
                        }
                    });
            });
        describe('try to deserialize SimplePod with less bytes',
            function() {
                it('should throw and catch',
                    function() {
                        let buf = makeSmallBuff();
                        let deserializer = new Deserializer();
                        try {
                            deserializer.simplePod(buf);
                            assert(false);
                        }
                        catch (err) {
                            assert(true);
                        }
                    });
            });
        describe('check RpcHeaderCrc32 hasn\'t changed',
            function() {
                it('crc32 of makeHeader should be equal to constant value',
                    function() {
                        const header = makeHeader();
                        const simplePod = makeSimplePod();
                        const serializer = new Serializer();
                        let serializedPod = serializer.simplePod(simplePod);
                        const hash = RpcXxhash64(serializedPod);
                        header.payloadChecksum = hash;
                        const serializedHeader = serializer.rpcHeader(header);
                        const resultCrc32 = RpcHeaderCrc32(serializedHeader);
                        const expectedCrc32 = 1774429187;
                        assert(expectedCrc32, resultCrc32);
                    });
            });
    });
