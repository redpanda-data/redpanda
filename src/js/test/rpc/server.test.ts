/**
 * Copyright 2020 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

import { ProcessBatchServer } from "../../modules/rpc/server";
import Repository from "../../modules/supervisors/Repository";
import { reset, stub } from "sinon";
import { SupervisorClient } from "../../modules/rpc/serverAndClients/processBatch";
import { createRecordBatch } from "../../modules/public";
import { Script_ManagerServer as ManagementServer } from "../../modules/rpc/serverAndClients/server";
import FileManager from "../../modules/supervisors/FileManager";
import { ProcessBatchRequest } from "../../modules/domain/generatedRpc/generatedClasses";
import { createHandle } from "../testUtilities";
import { PolicyError, RecordBatch } from "../../modules/public/Coprocessor";
import assert = require("assert");

const INotifyWait = require("inotifywait");
const sinon = require("sinon");

let server: ProcessBatchServer;
let client: SupervisorClient;
const spyFireExceptionServer = sinon.stub(
  ProcessBatchServer.prototype,
  "fireException"
);
const spyGetHandles = sinon.stub(
  Repository.prototype,
  "getHandlesByCoprocessorIds"
);
const spyFindByCoprocessor = sinon.stub(
  Repository.prototype,
  "findByCoprocessor"
);
const spyMoveHandle = sinon.stub(FileManager.prototype, "moveCoprocessorFile");
const spyDeregister = sinon.spy(FileManager.prototype, "deregisterCoprocessor");
let manageServer: ManagementServer;

const createProcessBatchRequest = (
  ids: bigint[] = [],
  topic = "topic"
): ProcessBatchRequest => {
  return {
    requests: [
      {
        recordBatch: [createRecordBatch()],
        coprocessorIds: ids,
        ntp: { partition: 1, namespace: "", topic },
      },
    ],
  };
};

describe("Server", function () {
  describe("Given a Request", function () {
    stub(FileManager.prototype, "readActiveCoprocessor");
    stub(FileManager.prototype, "updateRepositoryOnNewFile");
    stub(INotifyWait.prototype);

    beforeEach(() => {
      reset();
      spyFireExceptionServer.reset();
      spyGetHandles.reset();
      spyMoveHandle.reset();
      spyFindByCoprocessor.reset();
      spyDeregister.resetHistory();
      manageServer = new ManagementServer();
      manageServer.disable_copros = () => Promise.resolve({ inputs: [0] });
      manageServer.listen(43118);
      server = new ProcessBatchServer("a", "i", "s");
      server.listen(4300);
      client = new SupervisorClient(4300);
    });

    afterEach(async () => {
      client.close();
      await server.closeConnection();
      await manageServer.closeConnection();
    });

    it(
      "should fail when the given recordProcessBatch doesn't have " +
        "coprocessor ids",
      function (done) {
        spyFireExceptionServer.returns(Promise.resolve({ result: [] }));
        client.process_batch(createProcessBatchRequest()).then(() => {
          assert(
            spyFireExceptionServer.calledWith(
              "Bad request: request without coprocessor ids"
            )
          );
          done();
        });
      }
    );

    it("should fail if there isn't coprocessor that processBatch contain", function (done) {
      spyGetHandles.returns([]);
      spyFireExceptionServer.returns(
        Promise.resolve([
          {
            coprocessorId: "",
            ntp: { namespace: "", topic: "", partition: 1 },
            resultRecordBatch: [createRecordBatch()],
          },
        ])
      );
      client.process_batch(createProcessBatchRequest([BigInt(1)])).then(() => {
        assert(
          spyFireExceptionServer.calledWith(
            "Coprocessors don't register in wasm engine: 1"
          )
        );
        done();
      });
    });

    it("should apply the right Coprocessor for the Request's topic", function (done) {
      const coprocessorId = BigInt(1);
      spyGetHandles.returns([
        createHandle({
          apply: () =>
            new Map([
              [
                "newTopic",
                createRecordBatch({
                  header: { recordCount: 1 },
                  records: [{ value: Buffer.from("new VALUE") }],
                }),
              ],
            ]),
        }),
      ]);
      client
        .process_batch(createProcessBatchRequest([coprocessorId], "BaseTopic"))
        .then((res) => {
          assert(spyGetHandles.called);
          assert(spyGetHandles.calledWith([coprocessorId]));
          const resultBatch = res.result[0];
          assert.strictEqual(resultBatch.ntp.topic, "BaseTopic.$newTopic$");
          assert.deepStrictEqual(
            resultBatch.resultRecordBatch.flatMap(({ records }) =>
              records.map((r) => r.value.toString())
            ),
            ["new VALUE"]
          );
          done();
        });
    });

    describe("Given an Error when applying the Coprocessor", function () {
      it("should skip the Request, if ErrorPolicy is SkipOnFailure", function (done) {
        const badApplyCoprocessor = (record: RecordBatch) =>
          // eslint-disable-next-line @typescript-eslint/ban-ts-comment
          // @ts-ignore
          record.bad.attribute;

        spyGetHandles.returns([
          createHandle({
            apply: badApplyCoprocessor,
            policyError: PolicyError.SkipOnFailure,
            inputTopics: ["topic"],
          }),
        ]);

        client
          .process_batch(createProcessBatchRequest([BigInt(1)]))
          .then((res) => {
            assert(!spyDeregister.called);
            assert.deepStrictEqual(res.result, []);
            done();
          });
      });

      it("should deregister the Coprocessor, if ErrorPolicy is Deregister", function (done) {
        const badApplyCoprocessor = (record: RecordBatch) =>
          // eslint-disable-next-line @typescript-eslint/ban-ts-comment
          // @ts-ignore
          record.bad.attribute;
        const handle = createHandle({
          apply: badApplyCoprocessor,
          policyError: PolicyError.Deregister,
          inputTopics: ["topic"],
        });
        spyMoveHandle.returns(Promise.resolve(handle));
        spyFindByCoprocessor.returns(handle);
        spyGetHandles.returns([handle]);

        client
          .process_batch(createProcessBatchRequest([BigInt(1)]))
          .then((res) => {
            assert(spyDeregister.called);
            assert(spyGetHandles.called);
            assert(spyFindByCoprocessor.called);
            assert(spyMoveHandle.called);
            assert(spyMoveHandle.calledWith(handle));
            assert.deepStrictEqual(res.result, []);
            done();
          });
      });
    });
  });
});
