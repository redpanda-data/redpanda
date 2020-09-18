import { RegistrationClient, RegistrationServer } from "./generatedServer";
import {
  DisableTopicsReply,
  EnableTopicsReply,
  MetadataInfo,
} from "./generatedType";
import * as assert from "assert";

class ServerImplementation extends RegistrationServer {
  disable_topics(input: MetadataInfo): Promise<DisableTopicsReply> {
    return Promise.resolve({ inputs: [0] });
  }

  enable_topics(input: MetadataInfo): Promise<EnableTopicsReply> {
    return Promise.resolve({ inputs: [1] });
  }
}

// Create a server
const server = new ServerImplementation();
// Listen a server on 4301 port
server.listen(4301);

// Create a client, and connect to 4301 port
const client = new RegistrationClient(4301);

client
  .enable_topics({ inputs: ["topic1"] })
  .then((response) => {
    assert.deepStrictEqual(response.inputs, [1]);
  })
  .then(() => server.closeConnection())
  .then(() => console.log("close connection"))
  .catch((e) => console.error("error close connection ", e));
