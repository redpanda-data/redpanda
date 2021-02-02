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
import { SinonSandbox, createSandbox } from "sinon";
import FileManager from "../../modules/supervisors/FileManager";
import Repository from "../../modules/supervisors/Repository";
import { Script_ManagerServer as ManagementServer } from "../../modules/rpc/serverAndClients/server";
import * as fs from "fs";
import { createHandle } from "../testUtilities";
import { hash64 } from "xxhash";
import * as chokidar from "chokidar";
import LogService from "../../modules/utilities/Logging";

let sinonInstance: SinonSandbox;
let server: ManagementServer;

const createStubs = (sandbox: SinonSandbox) => {
  const watchMock = sandbox.stub(chokidar, "watch");
  watchMock.returns(
    sandbox.createStubInstance(chokidar.FSWatcher, { on: sandbox.stub() })
  );
  const readdirFake = sandbox.stub(fs, "readdir");
  const readFolderStub = sandbox.stub(
    FileManager.prototype,
    "readCoprocessorFolder"
  );
  readFolderStub.returns(Promise.resolve());
  const moveCoprocessor = sandbox.stub(
    FileManager.prototype,
    "moveCoprocessorFile"
  );
  const getCoprocessor = sandbox.stub(FileManager.prototype, "getHandle");

  const enableCoprocessor = sandbox
    .stub(FileManager.prototype, "enableCoprocessor")
    .returns(Promise.resolve());

  const disableCoprocessor = sandbox
    .stub(FileManager.prototype, "disableCoprocessors")
    .returns(Promise.resolve());

  sandbox.stub(LogService, "createLogger").returns({
    // eslint-disable-next-line @typescript-eslint/ban-ts-comment
    // @ts-ignore
    info: sandbox.stub(),
    // eslint-disable-next-line @typescript-eslint/ban-ts-comment
    // @ts-ignore
    error: sandbox.stub(),
  });

  return {
    moveCoprocessor,
    getCoprocessor,
    readdirFake,
    readFolderStub,
    enableCoprocessor,
    disableCoprocessor,
    watchMock,
  };
};

describe("FileManager", () => {
  beforeEach(() => {
    sinonInstance = createSandbox();
    server = new ManagementServer();
    server.listen(43118);
  });

  afterEach(async () => {
    sinonInstance.restore();
    await server.closeConnection();
  });

  it(
    "should read the existing file into active directory and after that " +
      "read submit directory",
    () => {
      const { readFolderStub } = createStubs(sinonInstance);
      const repo = new Repository();
      new FileManager(repo, "submit", "active", "inactive");
      // wait for promise readFolderCoprocessor resolves
      setTimeout(() => {
        assert(readFolderStub.firstCall.calledWith(repo, "active"));
        assert(readFolderStub.secondCall.calledWith(repo, "submit"));
      }, 300);
    }
  );

  it("should add listen for new file event", (done) => {
    const { readFolderStub, watchMock } = createStubs(sinonInstance);
    const repo = new Repository();
    new FileManager(repo, "submit", "active", "inactive");
    // wait for promise readFolderCoprocessor resolves
    setTimeout(() => {
      assert(readFolderStub.firstCall.calledWith(repo, "active"));
      assert(watchMock.called);
      assert(watchMock.calledWith("submit"));
      done();
    }, 300);
  });

  it(
    "should add a coprocessor from file path," +
      "it should disable coprocessor topics and enable after adding" +
      "coprocessor",
    (done) => {
      const { moveCoprocessor, getCoprocessor } = createStubs(sinonInstance);
      const handle = createHandle();
      const repo = new Repository();
      moveCoprocessor.returns(Promise.resolve(handle));
      getCoprocessor.returns(Promise.resolve(handle));

      // override the enable_copros method in server
      server.enable_copros = () =>
        Promise.resolve({
          inputs: [
            {
              response: handle.coprocessor.inputTopics.map(() => 0),
              id: handle.coprocessor.globalId,
            },
          ],
        });

      const file = new FileManager(repo, "submit", "active", "inactive");
      file.addCoprocessor("active/file", repo);
      setTimeout(() => {
        assert(getCoprocessor.called);
        assert(getCoprocessor.calledWith("active/file"));
        assert(moveCoprocessor.called);
        assert(moveCoprocessor.calledWith(handle, "active"));
        assert(repo.size() === 1);
        done();
      }, 150);
    }
  );

  it(
    "if adding a new coprocessor to repository and that repository has a " +
      "coprocessor with same id, FileManager should disable the previous " +
      "coprocessor and enable the new one",
    (done) => {
      const handle = createHandle();
      const handle2 = createHandle({ inputTopics: ["anotherTopics"] });
      handle2.checksum = "different";
      const {
        moveCoprocessor,
        getCoprocessor,
        enableCoprocessor,
        disableCoprocessor,
      } = createStubs(sinonInstance);
      const repo = new Repository();
      moveCoprocessor.returns(Promise.resolve(handle));
      getCoprocessor.returns(Promise.resolve(handle));
      getCoprocessor.returns(Promise.resolve(handle2));

      const fileManager = new FileManager(repo, "submit", "active", "inactive");

      fileManager
        .addCoprocessor(handle.filename, repo)
        .then(() => {
          assert(enableCoprocessor.called);
          assert(!disableCoprocessor.called);
        })
        .then(() => fileManager.addCoprocessor(handle2.filename, repo))
        .then(() => {
          assert.strictEqual(enableCoprocessor.getCalls().length, 2);
          assert(disableCoprocessor.called);
        })
        .then(() => done());
    }
  );

  it("should remove a coprocessor from file path", (done) => {
    const handle = createHandle();
    const { moveCoprocessor, getCoprocessor, disableCoprocessor } = createStubs(
      sinonInstance
    );
    const repo = new Repository();

    // add coprocessor
    repo.add(handle);
    // mock value for moving and getCoprocessor
    moveCoprocessor.returns(Promise.resolve(handle));
    getCoprocessor.returns(Promise.resolve(handle));
    // override the disable_copros method in server
    server.disable_copros = () => Promise.resolve({ inputs: [0] });
    // add spy to server

    const file = new FileManager(repo, "submit", "active", "inactive");
    file.deregisterCoprocessor(handle.coprocessor).then(() => {
      assert(moveCoprocessor.called);
      assert(moveCoprocessor.calledWith(handle, "inactive"));
      assert(disableCoprocessor.called);
      assert(disableCoprocessor.calledWith([handle.coprocessor]));
      done();
    });
  });

  it(
    "should move from active to inactive a remove coprocessor, if it fails " +
      "on enable request",
    function (done) {
      const handle = createHandle();
      const {
        moveCoprocessor,
        getCoprocessor,
        enableCoprocessor,
      } = createStubs(sinonInstance);
      const repo = new Repository();
      const removeSpy = sinonInstance.spy(repo, "remove");
      moveCoprocessor.returns(Promise.resolve(handle));
      getCoprocessor.returns(Promise.resolve(handle));
      enableCoprocessor.returns(Promise.reject(Error("internal error")));
      // override the enable_copros method in server
      server.enable_copros = () =>
        Promise.resolve({
          inputs: [
            {
              response: handle.coprocessor.inputTopics.map(() => 1),
              id: handle.coprocessor.globalId,
            },
          ],
        });

      const file = new FileManager(repo, "submit", "active", "inactive");
      file
        .addCoprocessor("active/file", repo)
        .catch(() => console.log("expected fail"));

      setTimeout(() => {
        // try to add a coprocessor to repository
        assert(getCoprocessor.called);
        assert(getCoprocessor.calledWith("active/file"));
        assert(moveCoprocessor.called);
        assert(moveCoprocessor.firstCall.calledWith(handle, "active"));
        // enable fail
        assert(moveCoprocessor.secondCall.calledWith(handle, "inactive"));
        assert(removeSpy.called);
        assert(removeSpy.calledWith(handle));
        assert(repo.size() === 0);
        done();
      }, 150);
    }
  );

  it("should compress an Error Matrix", function () {
    const errors = [
      [new Error("a"), new Error("b")],
      [new Error("c"), new Error("d")],
      [new Error("e"), new Error("f")],
    ];
    const result = FileManager.prototype.compactErrors(errors);
    const expectedErrorMessage = "a, b, c, d, e, f";
    assert.strictEqual(expectedErrorMessage, result.message);
  });

  it("should remove a coprocessor, if it's removed from active folder", function (done) {
    const handle = createHandle({
      globalId: hash64(Buffer.from("file"), 0).readBigUInt64LE(),
    });
    const { getCoprocessor, moveCoprocessor, disableCoprocessor } = createStubs(
      sinonInstance
    );
    moveCoprocessor.returns(Promise.resolve(handle));
    getCoprocessor.returns(Promise.resolve(handle));

    const repo = new Repository();
    const removeSpy = sinonInstance.spy(repo, "remove");
    const file = new FileManager(repo, "submit", "active", "inactive");

    repo.add(handle);

    file.removeHandleFromFilePath(handle.filename, repo).then(() => {
      assert(removeSpy.called);
      assert(disableCoprocessor.called);
      assert(removeSpy.withArgs(handle));
      done();
    });
  });

  it(
    "should remove a coprocessor, if it's removed from active folder and " +
      "memory, although disable_coproc request fails",
    function () {
      const handle = createHandle({
        globalId: hash64(Buffer.from("file"), 0).readBigUInt64LE(),
      });
      const {
        getCoprocessor,
        moveCoprocessor,
        disableCoprocessor,
      } = createStubs(sinonInstance);
      moveCoprocessor.returns(Promise.resolve(handle));
      getCoprocessor.returns(Promise.resolve(handle));
      disableCoprocessor.reset();
      disableCoprocessor.returns(Promise.reject("error"));

      const repo = new Repository();
      const removeSpy = sinonInstance.spy(repo, "remove");
      const file = new FileManager(repo, "submit", "active", "inactive");

      repo.add(handle);

      return file.removeHandleFromFilePath(handle.filename, repo).then(() => {
        assert(removeSpy.called);
        assert(disableCoprocessor.called);
        assert.strictEqual(repo.size(), 0);
        assert(removeSpy.withArgs(handle));
        assert.rejects(disableCoprocessor.firstCall.returnValue);
      });
    }
  );
});
