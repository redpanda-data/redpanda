/**
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

import {
  createConnection,
  Socket,
  createServer,
  Server as NetServer,
} from "net";
import { XXHash64 } from "xxhash";
import { calculate } from  "fast-crc32c/impls/js_crc32c";
import { IOBuf } from "../../../../src/js/modules/utilities/IOBuf";

import {
  MetadataInfo,
  EnableTopicsReply,
  DisableTopicsReply,
  RpcHeader,
} from "./generatedType";

const rpcHeaderSize = 26;
type ApplyFn = (rpcH: RpcHeader, buf: Buffer, socket: Socket) => void;

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
    payloadChecksum: hasher.digest().readBigUInt64LE(),
  };
  const auxHeader = Buffer.alloc(26);
  RpcHeader.toBytes(rpc, IOBuf.createFromBuffers([auxHeader]));
  rpc.headerChecksum = createCrc32(auxHeader.slice(0, 26), 5, 26);
  RpcHeader.toBytes(rpc, reserve);
}
/**
 * it adds listener for "data" event, when this listener is fired the data
 * buffer is going to pass to readBufferRequest, in this case, that buffer
 * represents a new request.
 */
const startReadRequest = (fn: ApplyFn) => (socket: Socket): void => {
  socket.once("data", (data) => readBufferRequest(data, fn, socket));
};

/**
 * Given a buffer, it reads an RPC Header. When it has the payload size from the RPC
 * Header, it reads the rest of the buffer in order to have all the bytes
 * needed to process the request, if the payload is bigger than the current
 * buffer, it calls readNextChunk in order to wait for the next "data" event with
 * the rest of the buffer.
 */
const readBufferRequest = (buffer: Buffer, fn: ApplyFn, socket: Socket) => {
  const [rpcHeader, crcValidation, crc32] = validateRpcHeader(
    buffer.slice(0, rpcHeaderSize)
  );
  if (!crcValidation) {
    throw (
      `Crc32 inconsistent, expected: ${rpcHeader.headerChecksum} ` +
      `generated: ${crc32}`
    );
  }
  const size = rpcHeader.payloadSize;
  if (buffer.length - rpcHeaderSize >= size) {
    const result = buffer.slice(rpcHeaderSize, size + rpcHeaderSize);
    startReadRequest(fn)(socket);
    return fn(rpcHeader, result, socket);
  } else {
    const bytesForReading = size - (buffer.length - rpcHeaderSize);
    return readNextChunk(socket, bytesForReading, fn)
      .then((nextBuffer) =>
        Buffer.concat([buffer.slice(rpcHeaderSize), nextBuffer])
      )
      .then((result) => fn(rpcHeader, result, socket));
  }
};

/**
 * given payload size, it waits for n "data" events until have the complete data
 * size.
 * @param socket
 * @param size
 * @param fn
 */
const readNextChunk = (
  socket: Socket,
  size: number,
  fn: ApplyFn
): Promise<Buffer> => {
  return new Promise<Buffer>((resolve) => {
    socket.once("data", (data) => {
      if (data.length > size) {
        readBufferRequest(data.slice(size), fn, socket);
        return resolve(data.slice(0, size));
      } else if (data.length === size) {
        startReadRequest(fn)(socket);
        return resolve(data.slice(0, size));
      } else {
        return readNextChunk(
          socket,
          size - data.length,
          fn
        ).then((nextBuffer) => resolve(Buffer.concat([data, nextBuffer])));
      }
    });
  });
};

export class RegistrationServer {
  constructor() {
    this.server = createServer(this.executeMethod.bind(this));
    this.handleNewConnection();
    this.process = this.process.bind(this);
  }

  listen(port: number) {
    this.server.listen(port);
  }

  process(rpcHeader: RpcHeader, payload: Buffer, socket: Socket) {
    switch (rpcHeader.meta) {
      case 2473210401: {
        const [value] = MetadataInfo.fromBytes(payload);
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
          buffer.forEach((fragment) =>
            socket.write(fragment.buffer.slice(0, fragment.used))
          );
        });
        break;
      }
      case 1680827556: {
        const [value] = MetadataInfo.fromBytes(payload);
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
          buffer.forEach((fragment) =>
            socket.write(fragment.buffer.slice(0, fragment.used))
          );
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
        buffer.forEach((fragment) =>
          socket.write(fragment.buffer.slice(0, fragment.used))
        );
      }
    }
  }

  executeMethod(socket: Socket) {
    startReadRequest(this.process)(socket);
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
    this.process = this.process.bind(this);
    this.client = createConnection({ port }, () => {
      console.log("Established connection with redpanda");
      startReadRequest(this.process)(this.client);
    });
  }

  process(rpcHeader: RpcHeader, payload: Buffer, socket: Socket) {
    this.responseHandlers.get(rpcHeader.correlationId)(payload);
  }

  send(
    buffer: IOBuf,
    headerReserve: IOBuf,
    size: number,
    meta: number
  ): number {
    const correlationId = ++this.correlationId;
    generateRpcHeader(buffer, headerReserve, size, meta, correlationId);
    buffer.forEach((fragment) =>
      this.client.write(fragment.buffer.slice(0, fragment.used))
    );
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
            resolve(EnableTopicsReply.fromBytes(data)[0]);
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
            resolve(DisableTopicsReply.fromBytes(data)[0]);
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
