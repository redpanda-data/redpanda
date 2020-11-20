import {
  RegistrationClient,
  RegistrationServer,
} from "./example/generatedServer";
import { reset, spy } from "sinon";
import assert = require("assert");

let server: RegistrationServer;
let client: RegistrationClient;

describe("rpc generator", function () {
  beforeEach(function () {
    reset();
    server = new RegistrationServer();
    server.listen(8003);
    client = new RegistrationClient(8003);
  });

  afterEach(async () => {
    client.close();
    await server.closeConnection();
  });

  it("should send a record between server and client", function (done) {
    // override server method
    server.enable_topics = (_) => Promise.resolve({ inputs: [0] });
    client
      .enable_topics({
        inputs: ["1"],
        age: 1,
        data: Buffer.from(""),
        id: BigInt(1),
        isCool: true,
        name: "vectorized",
      })
      .then((response) => {
        assert.strictEqual(response.inputs.length, 1);
        assert.deepStrictEqual(response.inputs, [0]);
        done();
      });
  });

  it("should server method calls with client request", function (done) {
    // override server method
    server.enable_topics = (_) => Promise.resolve({ inputs: [0] });
    const serverSpy = spy(server, "enable_topics");

    const request = {
      inputs: ["1"],
      age: 1,
      data: Buffer.from(""),
      id: BigInt(1),
      isCool: true,
      name: "vectorized",
    };

    client.enable_topics(request).then((response) => {
      assert.deepStrictEqual(serverSpy.firstCall.firstArg, request);
      assert.deepStrictEqual(
        serverSpy.firstCall.returnValue,
        Promise.resolve(response)
      );
      done();
    });
  });

  it("should server support big payload ( > 65Kb )", function (done) {
    server.enable_topics = (_) => Promise.resolve({ inputs: [0] });
    const serverSpy = spy(server, "process");
    // make request bigger than 65kb (500029)
    const request = {
      inputs: "1,".repeat(100000).split(","),
      age: 1,
      data: Buffer.from(""),
      id: BigInt(1),
      isCool: true,
      name: "vectorized",
    };

    client.enable_topics(request).then((_) => {
      const rpc = serverSpy.firstCall.args[0];
      assert.strictEqual(rpc.payloadSize > 66000, true);
      done();
    });
  });

  it("should server support small payload ( < 65Kb )", function (done) {
    server.enable_topics = (_) => Promise.resolve({ inputs: [0] });
    const serverSpy = spy(server, "process");
    const request = {
      inputs: "1,".repeat(100).split(","),
      age: 1,
      data: Buffer.from(""),
      id: BigInt(1),
      isCool: true,
      name: "vectorized",
    };

    client.enable_topics(request).then((_) => {
      const rpc = serverSpy.firstCall.args[0];
      assert.strictEqual(rpc.payloadSize < 65000, true);
      done();
    });

    client.enable_topics(request).then((_) => {
      const rpc = serverSpy.firstCall.args[0];
      assert.strictEqual(rpc.payloadSize < 65000, true);
      done();
    });
  });
});
