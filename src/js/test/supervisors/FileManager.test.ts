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
import {
  Script_ManagerClient as ManagementClient,
  Script_ManagerServer as ManagementServer,
} from "../../modules/rpc/serverAndClients/server";
import * as fs from "fs";
import { createHandle } from "../testUtilities";

const INotifyWait = require("inotifywait");
let sinonInstance: SinonSandbox;
let server: ManagementServer;
let client: ManagementClient;

const createStubs = (sandbox: SinonSandbox) => {
  sandbox.stub(INotifyWait.prototype);
  const readdirFake = sandbox.stub(fs, "readdir");
  const moveCoprocessor = sandbox.stub(
    FileManager.prototype,
    "moveCoprocessorFile"
  );
  const getCoprocessor = sandbox.stub(FileManager.prototype, "getHandle");
  return {
    moveCoprocessor,
    getCoprocessor,
    readdirFake,
  };
};

describe("FileManager", () => {
  beforeEach(() => {
    sinonInstance = createSandbox();
    server = new ManagementServer();
    server.listen(4300);
    client = new ManagementClient(4300);
  });

  afterEach(async () => {
    sinonInstance.restore();
    client.close();
    await server.closeConnection();
  });

  it("should read the existing file into active directory", () => {
    const { readdirFake } = createStubs(sinonInstance);
    const repo = new Repository();
    new FileManager(repo, "submit", "active", "inactive", client);
    assert(readdirFake.firstCall.calledWith("active"));
  });

  it("should add listen for new file event", () => {
    const repo = new Repository();
    const { readdirFake } = createStubs(sinonInstance);
    const updateFile = sinonInstance.stub(
      FileManager.prototype,
      "updateRepositoryOnNewFile"
    );
    new FileManager(repo, "submit", "active", "inactive", client);
    assert(readdirFake.firstCall.calledWith("active"));
    assert(updateFile.called);
    assert(updateFile.calledWith(repo));
  });

  it(
    "should add a coprocessor from file path," +
      "it should disable coprocessor topics and enable after adding" +
      "coprocessor",
    (done) => {
      const handle = createHandle();
      const repo = new Repository();
      const { moveCoprocessor, getCoprocessor } = createStubs(sinonInstance);
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

      const file = new FileManager(
        repo,
        "submit",
        "active",
        "inactive",
        client
      );
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

  it("should remove a coprocessor from file path", (done) => {
    const handle = createHandle();
    const repo = new Repository();
    const { moveCoprocessor, getCoprocessor } = createStubs(sinonInstance);

    // add coprocessor
    repo.add(handle);
    // mock value for moving and getCoprocessor
    moveCoprocessor.returns(Promise.resolve(handle));
    getCoprocessor.returns(Promise.resolve(handle));
    // override the disable_copros method in server
    server.disable_copros = () => Promise.resolve({ inputs: [0] });
    // add spy to server
    const spyDisable = sinonInstance.spy(client, "disable_copros");

    const file = new FileManager(repo, "submit", "active", "inactive", client);
    file.deregisterCoprocessor(handle.coprocessor);
    setTimeout(() => {
      assert(moveCoprocessor.called);
      assert(moveCoprocessor.calledWith(handle, "inactive"));
      assert(spyDisable.called);
      assert(spyDisable.calledWith({ inputs: [handle.coprocessor.globalId] }));
      done();
    }, 150);
  });
});
