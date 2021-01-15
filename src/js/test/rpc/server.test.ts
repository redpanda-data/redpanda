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
import LogService from "../../modules/utilities/Logging";
const fs = require("fs");

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
      .then(() => false)
      .catch(() => true)
      .then((value) => {
        assert(value, "A client should not have been created");
      });
  });
});

describe("Server", function () {
  describe("Given a Request", function () {
    beforeEach(() => {
      sinonInstance = createSandbox();
      //Mock LogService
      sinonInstance.stub(LogService, "createLogger").returns({
        // eslint-disable-next-line @typescript-eslint/ban-ts-comment
        // @ts-ignore
        info: sinonInstance.stub(),
        // eslint-disable-next-line @typescript-eslint/ban-ts-comment
        // @ts-ignore
        error: sinonInstance.stub(),
      });
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

    it("should apply coprocessors to multiple record batches", function () {
      const { spyGetHandles } = createStubs(sinonInstance);
      // transform coprocessor function
      const uppercase = (record) => ({
        ...record,
        value: record.value.map((char) => {
          if (char > 97 && char < 122) {
            return char - 32;
          } else {
            return char;
          }
        }),
      });

      const coprocessors = [1, 2, 3].map((id) =>
        createHandle({
          globalId: BigInt(id),
          apply: (recordBatch) => {
            const result = new Map();
            const transformedRecord =
              // eslint-disable-next-line @typescript-eslint/ban-ts-comment
              // @ts-ignore
              recordBatch.map(({ header, records }) => ({
                header,
                records: records.map(uppercase),
              }));
            result.set("result", transformedRecord);
            return result;
          },
        })
      );

      spyGetHandles.returns(coprocessors);

      // it sends 101 record batches, for each of those this should be
      // apply 3 coprocessors.
      const requests = new Array(100).fill({
        recordBatch: [
          createRecordBatch({
            header: {
              recordCount: 1,
            },
            records: [{ value: Buffer.from("b") }],
          }),
        ],
        coprocessorIds: [BigInt(1), BigInt(2), BigInt(3)],
        ntp: { partition: 1, namespace: "", topic: "produce" },
      });

      return Promise.all([
        client.process_batch({
          requests,
        }),
      ]).then(([{ result }]) => {
        // 101 record batches * 3 coprocessor definition = 303
        assert(result.length === 300);
        result.forEach((processBatches) => {
          processBatches.resultRecordBatch.forEach((rb) => {
            rb.records.forEach((record) => {
              // each record must have "A", because the coprocessors
              // definition transform all letter to uppercase
              assert.strictEqual(record.value.toString(), "B");
              assert.strictEqual(record.valueLen, 1);
            });
          });
        });
      });
    });

    it("should close logger if there is a fatal exception", (done) => {
      const fsStub = sinonInstance.stub(fs, "writeFile").returns(null);
      sinonInstance.stub(LogService, "getPath").returns("a");
      const close = sinonInstance.stub(LogService, "close");
      close.returns(Promise.resolve());
      new ProcessBatchServer("a", "a", "a");
      // waiting for firing exception
      setTimeout(() => {
        assert(close.called);
        assert(fsStub.called);
        // validate FileManager exception, this exception happens when it tries
        // to read unexciting folder
        assert.strictEqual(
          fsStub.firstCall.args[1],
          "Error: ENOENT: no such file or directory, scandir 'a'"
        );
        done();
      }, 10);
    });

    it("should server process 100 requests", function () {
      const { spyGetHandles } = createStubs(sinonInstance);
      // transform coprocessor function
      const uppercase = (record) => ({
        ...record,
        value: record.value.map((char) => {
          if (char > 97 && char < 122) {
            return char - 32;
          } else {
            return char;
          }
        }),
      });

      const coprocessors = [1].map((id) =>
        createHandle({
          globalId: BigInt(id),
          apply: (recordBatch) => {
            const result = new Map();
            const transformedRecord =
              // eslint-disable-next-line @typescript-eslint/ban-ts-comment
              // @ts-ignore
              recordBatch.map(({ header, records }) => ({
                header,
                records: records.map(uppercase),
              }));
            result.set("result", transformedRecord);
            return result;
          },
        })
      );

      spyGetHandles.returns(coprocessors);
      const requests = new Array(100).fill({
        recordBatch: [
          createRecordBatch({
            header: {
              recordCount: 1,
            },
            records: [{ value: Buffer.from("b") }],
          }),
        ],
        coprocessorIds: [BigInt(1)],
        ntp: { partition: 1, namespace: "", topic: "produce" },
      });

      return Promise.all(
        requests.map((item) => client.process_batch({ requests: [item] }))
      ).then(([{ result }]) => {
        result.forEach((r) => {
          const e = r.resultRecordBatch[0].records[0].value;
          assert.deepStrictEqual(e, Buffer.from("B"));
        });
      });
    });
  });
});
