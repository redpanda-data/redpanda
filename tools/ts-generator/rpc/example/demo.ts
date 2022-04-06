/**
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

import { RegistrationClient, RegistrationServer } from "./generatedServer";
import {
  DisableTopicsReply,
  EnableTopicsReply,
  MetadataInfo,
} from "./generatedType";
import * as assert from "assert";

const request: MetadataInfo = {
  inputs: ["test 1", "test 2", "test 3"],
  name: "another cool test ".repeat(1e3),
  isCool: true,
  data: Buffer.from("RedPanda is cool".repeat(1e3)),
  age: 3,
  id: BigInt(123443212384571234),
};

class ServerImplementation extends RegistrationServer {
  disable_topics(input: MetadataInfo): Promise<DisableTopicsReply> {
    assert.deepStrictEqual(input, request);
    return Promise.resolve({ inputs: input.inputs.map(() => 0) });
  }

  enable_topics(input: MetadataInfo): Promise<EnableTopicsReply> {
    return Promise.resolve({ inputs: input.inputs.map(() => 1) });
  }
}

// Create a server
const server = new ServerImplementation();
// Listen a server on 4301 port
server.listen(4301);

// Create a client, and connect to 4301 port
const client = new RegistrationClient(4301);

client
  .enable_topics(request)
  .then((response) => {
    assert.deepStrictEqual(
      response.inputs,
      request.inputs.map(() => 1)
    );
  })
  .then(() => server.closeConnection())
  .then(() => console.log("close connection"))
  .catch((e) => {
    server.closeConnection();
    console.error("close connection with error: ", e);
  });
