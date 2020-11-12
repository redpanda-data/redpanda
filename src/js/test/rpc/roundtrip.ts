/**
 * Copyright 2020 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

import { strict as assert } from "assert";
import { Serializer, Deserializer } from "../../modules/rpc/parser";
import {
  RpcHeader,
  SimplePod,
  RecordBatchHeader,
} from "../../modules/rpc/types";
import { RecordHeader, Record, RecordBatch } from "../../modules/rpc/types";
import {
  RpcHeaderCrc32,
  crcRecordBatch,
  crcRecordBatchHeaderInternal,
} from "../../modules/hashing/crc32";
import { RpcXxhash64 } from "../../modules/hashing/xxhash";
import "mocha";
import * as re from "rewire";

let mod = re("../../modules/hashing/crc32.js");
let zigzag = mod.__get__("encodeZigZag64");

function makeHeader() {
  //some random values
  //It's crc32 is = 1774429187
  //If you change the values of this header
  //Please change the const value in the crc32 test case below
  return new RpcHeader(1, 0, 0, 1, 2, 3, BigInt(0));
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

//to-do make these functions random and move them
//to a utility file
function makeBuffer() {
  return Buffer.allocUnsafe(50).fill("k");
}

function makeHeaders() {
  let ret: Array<RecordHeader> = [];
  let key = makeBuffer();
  let keySz = key.length;
  let v = makeBuffer();
  let vSz = v.length;
  ret.push(new RecordHeader(keySz, key, vSz, v));
  return ret;
}

function makeRecord() {
  const key = makeBuffer();
  const keySz = key.length;
  const v = makeBuffer();
  const vSz = v.length;
  const hdrs = makeHeaders();
  let size =
    1 + // attrs size
    4 + //timestampe delta
    4 + //offset delta
    4 + //size of key length
    key.length + //size of key
    4 + //size of value
    v.length +
    4; //headers size
  for (let h of hdrs) {
    size +=
      4 + // key
      h.key.length + //key length
      4 + // value size
      h.value.length; // value length
  }
  return new Record(size, 0, 1, 1, keySz, key, vSz, v, hdrs);
}

function makeRecordBatchHeader() {
  //to-do this should be randomized
  const numOfRecords = 1;
  return new RecordBatchHeader(
    0,
    0,
    BigInt(0),
    0,
    0,
    0,
    numOfRecords - 1,
    BigInt(0),
    BigInt(1),
    BigInt(0),
    0,
    0,
    numOfRecords,
    BigInt(0)
  );
}

function makeRecordBatch() {
  const timestamp = 0;
  let header = makeRecordBatchHeader();
  let size = 70; //record batch header size
  let records: Array<Record> = [];
  records.push(makeRecord());
  for (let rcd of records) {
    size += rcd.size() + 4;
  }
  header.sizeBytes = size;
  let batch = new RecordBatch(header, records);
  batch.header.crc = crcRecordBatch(batch);
  batch.header.headerCrc = crcRecordBatchHeaderInternal(batch.header);
  return batch;
}

describe("RPC", () => {
  describe("roundtrip simple_pod", () => {
    it("Assert should fail if serializer output differs", () => {
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
  describe("try to deserialize Header with less bytes", () => {
    it("should throw and catch", () => {
      let buf = makeSmallBuff();
      let deserializer = new Deserializer();
      try {
        deserializer.rpcHeader(buf);
        assert(false);
      } catch (err) {
        assert(true);
      }
    });
  });
  describe("try to deserialize SimplePod with less bytes", () => {
    it("should throw and catch", () => {
      let buf = makeSmallBuff();
      let deserializer = new Deserializer();
      try {
        deserializer.simplePod(buf);
        assert(false);
      } catch (err) {
        assert(true);
      }
    });
  });
  describe("check RpcHeaderCrc32 hasn't changed", () => {
    it("crc32 of makeHeader should be equal to constant value", () => {
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
  describe("roundtrip RecordBatch", () => {
    it("Assert should fail if serializer output differs", () => {
      const batch = makeRecordBatch();
      const serializer = new Serializer();
      //serialization starts here
      let serializedBatch = serializer.recordBatch(batch);
      //start deserialization
      let deserializer = new Deserializer();
      let resultBatch = deserializer.recordBatch(serializedBatch);
      //assert the data is the same
      assert(batch.equals(resultBatch[0]));
    });
  });
  describe("test zigzag enconding with min/max", () => {
    it("Assert should fail if enconding fails", () => {
      const max: BigInt = BigInt.asIntN(64, BigInt("9223372036854775807"));
      const min: BigInt = BigInt.asIntN(64, BigInt("-9223372036854775808"));
      const maxResult = BigInt("18446744073709551614");
      const minResult = BigInt("18446744073709551615");

      assert(maxResult == zigzag(max));
      assert(minResult == zigzag(min));
    });
  });
});
