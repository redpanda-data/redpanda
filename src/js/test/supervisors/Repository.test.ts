/**
 * Copyright 2020 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

import * as assert from "assert";
import Repository from "../../modules/supervisors/Repository";

import { createHandle, createMockCoprocessor } from "../testUtilities";
import { ProcessBatchRequestItem } from "../../modules/domain/generatedRpc/generatedClasses";
import { ProcessBatchServer } from "../../modules/rpc/server";
import { createRecordBatch } from "../../modules/public";
import { PolicyInjection, RecordBatch } from "../../modules/public/Coprocessor";
import sinon = require("sinon");
import LogService from "../../modules/utilities/Logging";
import { createSandbox, SinonSandbox } from "sinon";
import { calculateRecordBatchSize } from "../../modules/public/Utils";

let sinonInstance: SinonSandbox;

const createSinonInstances = (sinonInstance: SinonSandbox) => {
  const info = sinonInstance.stub();
  sinonInstance.stub(LogService, "createLogger").returns({
    // eslint-disable-next-line @typescript-eslint/ban-ts-comment
    // @ts-ignore
    warn: sinonInstance.stub(),
    info,
  });
  return { info };
};

describe("Repository", function () {
  beforeEach(() => {
    sinonInstance = createSandbox();
  });

  afterEach(async () => {
    sinonInstance.restore();
  });

  it("should initialize with an empty map", function () {
    createSinonInstances(sinonInstance);
    const repository = new Repository();
    assert(repository.size() === 0);
  });

  it("should add a handle to the repository", function () {
    createSinonInstances(sinonInstance);
    const repository = new Repository();
    repository.add(createHandle());
    assert(repository.size() === 1);
  });

  it(
    "should replace a handle if a new one with the same globalId " +
      "is added.",
    function () {
      createSinonInstances(sinonInstance);
      const topicA = "topicA";
      const topicB = "topicB";
      const coprocessorA = createMockCoprocessor(BigInt(1), [
        [topicA, PolicyInjection.Stored],
      ]);
      const coprocessorB = createMockCoprocessor(BigInt(1), [
        [topicB, PolicyInjection.Stored],
      ]);
      const repository = new Repository();
      repository.add(createHandle(coprocessorA));
      assert(repository.findByCoprocessor(coprocessorA));
      const lookup_a = repository.findByCoprocessor(coprocessorA);
      assert(lookup_a);
      assert(
        lookup_a.coprocessor.inputTopics.find(([topic, _]) => {
          return topic == topicA;
        })
      );
      repository.add(createHandle(coprocessorB));
      assert(repository.findByCoprocessor(coprocessorA));
      const lookup_b = repository.findByCoprocessor(coprocessorB);
      assert(lookup_b);
      assert(
        lookup_b.coprocessor.inputTopics.find(([topic, _]) => {
          return topic == topicB;
        })
      );
    }
  );

  it("should find a handle by another Handle", function () {
    createSinonInstances(sinonInstance);
    const repository = new Repository();
    const handleA = createHandle();
    const handleB = createHandle({
      globalId: BigInt(2),
      inputTopics: [["topicB", PolicyInjection.Stored]],
    });
    repository.add(handleA);
    assert(repository.findByGlobalId(handleA));
    assert(!repository.findByGlobalId(handleB));
  });

  it("should find a handle by another Coprocessor", function () {
    createSinonInstances(sinonInstance);
    const repository = new Repository();
    const handleA = createHandle();
    const handleB = createHandle({
      globalId: BigInt(2),
      inputTopics: [["topicB", PolicyInjection.Stored]],
    });
    repository.add(handleA);
    assert(repository.findByCoprocessor(handleA.coprocessor));
    assert(!repository.findByCoprocessor(handleB.coprocessor));
  });

  it("should apply function and calculate record batch size", function (done) {
    createSinonInstances(sinonInstance);
    const repository = new Repository();
    const handleA = createHandle();
    repository.add(handleA);
    const processBatchRequest: ProcessBatchRequestItem = {
      ntp: {
        topic: "topic",
        namespace: "namespace",
        partition: 1,
      },
      coprocessorIds: [handleA.coprocessor.globalId],
      recordBatch: [
        {
          header: {
            term: BigInt(1),
            sizeBytes: 1,
            crc: 1,
            headerCrc: 1,
            firstTimestamp: BigInt(1),
            recordCount: 1,
            isCompressed: 0,
            baseOffset: BigInt(1),
            attrs: 0,
            baseSequence: 0,
            lastOffsetDelta: 0,
            maxTimestamp: BigInt(0),
            producerEpoch: 1,
            producerId: BigInt(1),
            recordBatchType: 1,
          },
          records: [
            {
              length: 0,
              headers: [],
              valueLen: 2,
              attributes: 0,
              keyLength: 0,
              key: Buffer.from(""),
              timestampDelta: BigInt(1),
              offsetDelta: 0,
              value: Buffer.from("test"),
            },
          ],
        },
      ],
    };
    handleA.coprocessor.apply = (record: RecordBatch) =>
      Promise.resolve(
        new Map([
          [
            "result",
            createRecordBatch({
              records: record.records.map((r) => ({
                ...r,
                value: Buffer.from("TEST"),
                valueLen: 4,
              })),
            }),
          ],
        ])
      );
    const stubFire = sinonInstance.stub(
      ProcessBatchServer.prototype,
      "fireException"
    );
    const stubHandleError = sinonInstance.stub(
      ProcessBatchServer.prototype,
      "handleErrorByPolicy"
    );
    const applyResult = repository.applyCoprocessor(
      [handleA.coprocessor.globalId],
      processBatchRequest,
      stubHandleError,
      stubFire
    );
    Promise.all(applyResult).then((result) => {
      const recordBatchResult = result[0][0].resultRecordBatch;
      const record = recordBatchResult[0].records[0];
      assert.strictEqual(recordBatchResult[0].header.sizeBytes, 72);
      assert.strictEqual(record.length, 10);
      assert.deepStrictEqual(record.value, Buffer.from("TEST"));
      done();
    });
  });

  it("should pass recordBatch and logger to coprocessor apply fn", () => {
    const { info } = createSinonInstances(sinonInstance);
    const repository = new Repository();
    const handleA = createHandle();
    const reallyLongInput =
      "aaaabbbbccccddddeeeeffffgggghhhhiiiijjjjkkkkllllmmmmnnnnooooppppqqqqrrrrssssttttuuuuvvvvwwwwxxxxyyyyzzzzaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaabbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    const reallyLongInputBuffer = Buffer.from(reallyLongInput);
    repository.add(handleA);
    const processBatchRequest: ProcessBatchRequestItem = {
      ntp: {
        topic: "topic",
        namespace: "namespace",
        partition: 1,
      },
      coprocessorIds: [handleA.coprocessor.globalId],
      recordBatch: [
        {
          header: {
            term: BigInt(1),
            sizeBytes: 1,
            crc: 1,
            headerCrc: 1,
            firstTimestamp: BigInt(1),
            recordCount: 1,
            isCompressed: 0,
            baseOffset: BigInt(1),
            attrs: 0,
            baseSequence: 0,
            lastOffsetDelta: 0,
            maxTimestamp: BigInt(0),
            producerEpoch: 1,
            producerId: BigInt(1),
            recordBatchType: 1,
          },
          records: [
            {
              length: 0,
              headers: [],
              attributes: 0,
              keyLength: 0,
              key: Buffer.from(""),
              timestampDelta: BigInt(1),
              offsetDelta: 0,
              value: reallyLongInputBuffer,
              valueLen: reallyLongInputBuffer.byteLength,
            },
          ],
        },
      ],
    };
    handleA.coprocessor.apply = (record: RecordBatch, logger) => {
      logger.info("test");
      return Promise.resolve(
        new Map([
          [
            "result",
            createRecordBatch({
              records: record.records.map((r) => ({
                ...r,
                value: Buffer.from("TEST"),
              })),
            }),
          ],
        ])
      );
    };
    const stubFire = sinonInstance.stub(
      ProcessBatchServer.prototype,
      "fireException"
    );
    const stubHandleError = sinonInstance.stub(
      ProcessBatchServer.prototype,
      "handleErrorByPolicy"
    );
    const applyResult = repository.applyCoprocessor(
      [handleA.coprocessor.globalId],
      processBatchRequest,
      stubHandleError,
      stubFire
    );
    return Promise.all(applyResult).then((result) => {
      const recordBatchResult = result[0][0].resultRecordBatch;
      const record = recordBatchResult[0].records[0];
      assert.strictEqual(recordBatchResult[0].header.sizeBytes, 72);
      assert.strictEqual(record.length, 10);
      assert.strictEqual(
        recordBatchResult[0].header.sizeBytes,
        calculateRecordBatchSize(recordBatchResult[0].records)
      );
      assert.strictEqual(record.valueLen, 4);
      assert.deepStrictEqual(record.value, Buffer.from("TEST"));
      assert.strictEqual(info.called, true);
    });
  });
});
