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
import { SinonSandbox, createSandbox } from "sinon";
import { SupervisorClient } from "../../modules/rpc/serverAndClients/processBatch";
import { createRecordBatch } from "../../modules/public";
import { Script_ManagerServer as ManagementServer } from "../../modules/rpc/serverAndClients/server";
import FileManager from "../../modules/supervisors/FileManager";
import { ProcessBatchRequest } from "../../modules/domain/generatedRpc/generatedClasses";
import { createHandle } from "../testUtilities";
import { PolicyError, RecordBatch } from "../../modules/public/Coprocessor";
import assert = require("assert");
import * as chokidar from "chokidar";

let sinonInstance: SinonSandbox;
let server: ProcessBatchServer;
let client: SupervisorClient;
let manageServer: ManagementServer;

const createStubs = (sandbox: SinonSandbox) => {
  const watchMock = sandbox.stub(chokidar, "watch");
  watchMock.returns(({ on: sandbox.stub() } as unknown) as chokidar.FSWatcher);
  const readCoprocessorFolder = sandbox.stub(
    FileManager.prototype,
    "readCoprocessorFolder"
  );
  readCoprocessorFolder.returns(Promise.resolve());
  const spyFireExceptionServer = sandbox.stub(
    ProcessBatchServer.prototype,
    "fireException"
  );
  const spyGetHandles = sandbox.stub(
    Repository.prototype,
    "getHandlesByCoprocessorIds"
  );
  const spyFindByCoprocessor = sandbox.stub(
    Repository.prototype,
    "findByCoprocessor"
  );
  const spyMoveHandle = sandbox.stub(
    FileManager.prototype,
    "moveCoprocessorFile"
  );
  const spyDeregister = sandbox.spy(
    FileManager.prototype,
    "deregisterCoprocessor"
  );
  return {
    spyFireExceptionServer,
    spyGetHandles,
    spyFindByCoprocessor,
    spyMoveHandle,
    spyDeregister,
    watchMock,
  };
};

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

describe("Client", function () {
  it("create() should not return a client if fails to connect", () => {
    return SupervisorClient.create(40000)
      .then((_c) => false)
      .catch((_e) => true)
      .then((value) => {
        assert(value, "A client should not have been created");
      });
  });
});

describe("Server", function () {
  describe("Given a Request", function () {
    beforeEach(() => {
      sinonInstance = createSandbox();
      manageServer = new ManagementServer();
      manageServer.disable_copros = () => Promise.resolve({ inputs: [0] });
      manageServer.listen(43118);
      server = new ProcessBatchServer("a", "i", "s");
      server.listen(43000);
      return new Promise<void>((resolve, reject) => {
        return SupervisorClient.create(43000)
          .then((c) => {
            client = c;
            resolve();
          })
          .catch((e) => reject(e));
      });
    });

    afterEach(async () => {
      client.close();
      sinonInstance.restore();
      await server.closeConnection();
      await manageServer.closeConnection();
    });

    it(
      "should fail when the given recordProcessBatch doesn't have " +
        "coprocessor ids",
      function (done) {
        const { spyFireExceptionServer } = createStubs(sinonInstance);
        // eslint-disable-next-line @typescript-eslint/ban-ts-comment
        // @ts-ignore
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
      const { spyFireExceptionServer, spyGetHandles } = createStubs(
        sinonInstance
      );
      spyGetHandles.returns([]);
      spyFireExceptionServer.returns(
        /* FireException should throw an exception but there isn't way to
             listen that exception, for that reason this stub return a "correct"
             value in order to check the error, when the server response
             to client
           */
        Promise.resolve([
          {
            coprocessorId: "",
            ntp: { namespace: "", topic: "", partition: 1 },
            resultRecordBatch: [createRecordBatch()],
          },
        ]) as never
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
      const { spyGetHandles } = createStubs(sinonInstance);
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

    it(
      "if there is an error, should skip the Request, if ErrorPolicy is " +
        "SkipOnFailure",
      function (done) {
        const { spyGetHandles, spyDeregister } = createStubs(sinonInstance);
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
      }
    );

    it(
      "if there is an error, should deregister the Coprocessor, if " +
        "ErrorPolicy is Deregister",
      function (done) {
        const {
          spyGetHandles,
          spyDeregister,
          spyMoveHandle,
          spyFindByCoprocessor,
        } = createStubs(sinonInstance);
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
      }
    );
  });
});
