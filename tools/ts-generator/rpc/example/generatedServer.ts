/**
 * Copyright 2020 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

import {
  createConnection,
  Socket,
  createServer,
  Server as NetServer,
} from "net";
import { XXHash64 } from "xxhash";
import { calculate } from "fast-crc32c";
import { IOBuf } from "../../../../src/js/modules/utilities/IOBuf";

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
  buffer: IOBuf,
  reserve: IOBuf,
  size: number,
  meta: number,
  correlationId: number
) {
  const hasher = new XXHash64(0);
  buffer.forEach((fragment) => {
    if (fragment.getSize() === 512) {
      hasher.update(fragment.buffer.slice(26, fragment.used));
    } else {
      hasher.update(fragment.buffer.slice(0, fragment.used));
    }
  });
  const rpc: RpcHeader = {
    version: 0,
    headerChecksum: 0,
    compression: 0,
    payloadSize: size,
    meta,
    correlationId,
    payloadChecksum: hasher.digest().readBigUInt64BE(),
  };
  const auxHeader = Buffer.alloc(26);
  RpcHeader.toBytes(rpc, IOBuf.createFromBuffers([auxHeader]));
  rpc.headerChecksum = createCrc32(auxHeader.slice(0, 26), 5, 26);
  RpcHeader.toBytes(rpc, reserve);
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
                const buffer = new IOBuf();
                const rpcHeaderReserve = buffer.getReserve(26);
                const size = EnableTopicsReply.toBytes(output, buffer);
                generateRpcHeader(
                  buffer,
                  rpcHeaderReserve,
                  size,
                  200,
                  rpcHeader.correlationId
                );
                buffer.forEach((fragment) => socket.write(fragment.buffer));
              });
              break;
            }
            case 1680827556: {
              const [value] = MetadataInfo.fromBytes(
                socket.read(rpcHeader.payloadSize)
              );
              this.disable_topics(value).then((output: DisableTopicsReply) => {
                const buffer = new IOBuf();
                const rpcHeaderReserve = buffer.getReserve(26);
                const size = DisableTopicsReply.toBytes(output, buffer);
                generateRpcHeader(
                  buffer,
                  rpcHeaderReserve,
                  size,
                  200,
                  rpcHeader.correlationId
                );
                buffer.forEach((fragment) => socket.write(fragment.buffer));
              });
              break;
            }
            default: {
              const buffer = new IOBuf();
              const rpcHeaderReserve = buffer.getReserve(26);
              generateRpcHeader(
                buffer,
                rpcHeaderReserve,
                0,
                404,
                rpcHeader.correlationId
              );
              buffer.forEach((fragment) => socket.write(fragment.buffer));
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
      console.log("Established connection with redpanda");
      this.client.on("data", (data) => {
        const [rpc] = RpcHeader.fromBytes(data, 0);
        this.responseHandlers.get(rpc.correlationId)(data);
      });
    });
  }

  send(
    buffer: IOBuf,
    headerReserve: IOBuf,
    size: number,
    meta: number
  ): number {
    const correlationId = ++this.correlationId;
    generateRpcHeader(buffer, headerReserve, size, meta, correlationId);
    buffer.forEach((fragment) => this.client.write(fragment.buffer));
    return this.correlationId;
  }

  enable_topics(input: MetadataInfo): Promise<EnableTopicsReply> {
    return new Promise((resolve, reject) => {
      try {
        const buffer = new IOBuf();
        const rpcHeaderReserve = buffer.getReserve(26);
        const size = MetadataInfo.toBytes(input, buffer);
        const correlationId = this.send(
          buffer,
          rpcHeaderReserve,
          size,
          2473210401
        );
        this.responseHandlers.set(correlationId, (data: Buffer) => {
          try {
            const [rpcHeader, crc32Validation, crc32] = validateRpcHeader(data);
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
        const buffer = new IOBuf();
        const rpcHeaderReserve = buffer.getReserve(26);
        const size = MetadataInfo.toBytes(input, buffer);
        const correlationId = this.send(
          buffer,
          rpcHeaderReserve,
          size,
          1680827556
        );
        this.responseHandlers.set(correlationId, (data: Buffer) => {
          try {
            const [rpcHeader, crc32Validation, crc32] = validateRpcHeader(data);
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

  close() {
    this.client.destroy();
  }

  // Map for indexing the server response handlers, where the key is the
  // correlationId, and the value is a function that takes a buffer response,
  // and transform it into the expected output type
  responseHandlers = new Map<number, (buffer) => void>();
  client: Socket;
  correlationId = 0;
}
