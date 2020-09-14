import {
  createConnection,
  Socket,
  createServer,
  Server as NetServer,
} from "net";
import { hash64 } from "xxhash";
import { calculate } from "fast-crc32c";

import {
  MetadataInfo,
  EnableTopicsReply,
  DisableTopicsReply,
  RpcHeader,
} from "./generatedType";

function createCrc32(buffer: Buffer, start, end): number {
  return calculate(buffer.subarray(start, end));
}

function validateRpcHeader(
  buffer: Buffer,
  offset = 0
): [RpcHeader, boolean, number] {
  const [rpcHeader] = RpcHeader.fromBytes(buffer, offset);
  const crc32 = createCrc32(buffer, 5, 26);
  return [rpcHeader, rpcHeader.headerChecksum === crc32, crc32];
}

function generateRpcHeader(
  buffer: Buffer,
  size: number,
  meta: number,
  correlationId: number
) {
  const rpc: RpcHeader = {
    version: 0,
    headerChecksum: 0,
    compression: 0,
    payloadSize: size,
    meta,
    correlationId,
    payloadChecksum: BigInt(
      hash64(buffer.slice(26, 26 + size), 0).readBigUInt64LE()
    ),
  };
  RpcHeader.toBytes(rpc, buffer, 0);
  buffer.writeUInt32LE(createCrc32(buffer, 5, 26), 1);
}

export class RegistrationServer {
  constructor() {
    this.server = createServer(this.executeMethod.bind(this));
    this.handleNewConnection();
  }

  listen(port: number) {
    this.server.listen(port);
  }

  executeMethod(socket: Socket) {
    socket.on("readable", () => {
      if (socket.readableLength > 26) {
        const [rpcHeader, crc32Validation, crc32] = validateRpcHeader(
          socket.read(26)
        );
        if (!crc32Validation) {
          throw (
            `Crc32 inconsistent, expect: ${rpcHeader.headerChecksum}` +
            `generated: ${crc32}`
          );
        } else {
          switch (rpcHeader.meta) {
            case 2473210401: {
              const [value] = MetadataInfo.fromBytes(
                socket.read(rpcHeader.payloadSize)
              );
              this.enable_topics(value).then((output: EnableTopicsReply) => {
                const buffer = Buffer.alloc(100);
                const size = EnableTopicsReply.toBytes(output, buffer, 26) - 26;
                generateRpcHeader(buffer, size, 200, rpcHeader.correlationId);
                socket.write(buffer.slice(0, 26 + size));
              });
              break;
            }
            case 1680827556: {
              const [value] = MetadataInfo.fromBytes(
                socket.read(rpcHeader.payloadSize)
              );
              this.disable_topics(value).then((output: DisableTopicsReply) => {
                const buffer = Buffer.alloc(100);
                const size =
                  DisableTopicsReply.toBytes(output, buffer, 26) - 26;
                generateRpcHeader(buffer, size, 200, rpcHeader.correlationId);
                socket.write(buffer.slice(0, 26 + size));
              });
              break;
            }
            default: {
              const buffer = Buffer.alloc(30);
              generateRpcHeader(buffer, 0, 404, rpcHeader.correlationId);
              socket.write(buffer);
            }
          }
        }
      }
    });
  }

  enable_topics(input: MetadataInfo): Promise<EnableTopicsReply> {
    return Promise.resolve(null);
  }

  disable_topics(input: MetadataInfo): Promise<DisableTopicsReply> {
    return Promise.resolve(null);
  }

  handleNewConnection() {
    this.server.on("connection", (conn) => {
      const key = conn.remoteAddress + ":" + conn.remotePort;
      this.connections.set(key, conn);
      conn.on("close", () => {
        this.connections.delete(key);
      });
    });
  }

  closeConnection(): Promise<void> {
    return new Promise((resolve, reject) => {
      try {
        this.server.close();
        for (const connection of this.connections.values()) {
          connection.destroy();
        }
        resolve();
      } catch (e) {
        reject(e);
      }
    });
  }

  server: NetServer;
  connections: Map<string, Socket> = new Map();
}

export class RegistrationClient {
  constructor(port: number) {
    this.client = createConnection({ port }, () => {
      console.log("connect with Redpanda :)");
      this.client.on("data", (data) => {
        const [rpc] = RpcHeader.fromBytes(data, 0);
        this.requestProcess.get(rpc.correlationId)(data);
      });
    });
  }

  send(buffer: Buffer, size: number, meta: number): number {
    const correlationId = ++this.correlationId;
    generateRpcHeader(buffer, size, meta, correlationId);
    this.client.write(buffer.slice(0, 26 + size));
    return this.correlationId;
  }

  enable_topics(input: MetadataInfo): Promise<EnableTopicsReply> {
    return new Promise((resolve, reject) => {
      try {
        const buffer = Buffer.alloc(100);
        const size = MetadataInfo.toBytes(input, buffer, 26);
        const correlationId = this.send(buffer, size, 2473210401);
        this.requestProcess.set(correlationId, (data: Buffer) => {
          try {
            const [rpcHeader, crc32Validation, crc32] = validateRpcHeader(
              buffer
            );
            if (!crc32Validation) {
              throw (
                `Crc32 inconsistent, expect: ${rpcHeader.headerChecksum}` +
                `generated: ${crc32}`
              );
            } else {
              resolve(EnableTopicsReply.fromBytes(data, 26)[0]);
            }
          } catch (e) {
            reject(e);
          }
        });
      } catch (e) {
        reject(e);
      }
    });
  }

  disable_topics(input: MetadataInfo): Promise<DisableTopicsReply> {
    return new Promise((resolve, reject) => {
      try {
        const buffer = Buffer.alloc(100);
        const size = MetadataInfo.toBytes(input, buffer, 26);
        const correlationId = this.send(buffer, size, 1680827556);
        this.requestProcess.set(correlationId, (data: Buffer) => {
          try {
            const [rpcHeader, crc32Validation, crc32] = validateRpcHeader(
              buffer
            );
            if (!crc32Validation) {
              throw (
                `Crc32 inconsistent, expect: ${rpcHeader.headerChecksum}` +
                `generated: ${crc32}`
              );
            } else {
              resolve(DisableTopicsReply.fromBytes(data, 26)[0]);
            }
          } catch (e) {
            reject(e);
          }
        });
      } catch (e) {
        reject(e);
      }
    });
  }

  requestProcess = new Map<number, (buffer) => void>();
  client: Socket;
  correlationId = 0;
}
