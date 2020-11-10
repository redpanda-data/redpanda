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

const bytesNoPayload = 26;
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

const getAndValidateRpcHeader = (data: Buffer): RpcHeader => {
  const [rpcHeader, crc32Validation, crc32] = validateRpcHeader(
    data.slice(0, 26)
  );
  if (!crc32Validation) {
    throw (
      `Crc32 inconsistent, expect: ${rpcHeader.headerChecksum} ` +
      `generated: ${crc32}`
    );
  } else {
    return rpcHeader;
  }
};

/**
 * add "data" event listener, when the "data" event is emitted it reads the rpc
 * header, and read the payload size, and wait n "data" events until have
 * the complete payload size. this because node js split request when it is
 * bigger than 65000
 * @param fn
 */
const startReadRequest = (fn: ApplyFn) => (socket: Socket): void => {
  socket.once("data", (data) => {
    const rpcHeader = getAndValidateRpcHeader(data);
    const size = rpcHeader.payloadSize;
    if (data.length - bytesNoPayload >= size) {
      const bufferResult = data.slice(bytesNoPayload, size + bytesNoPayload);
      return fn(rpcHeader, bufferResult, socket);
    } else {
      const bytesForReading = size - (data.length - bytesNoPayload);
      return readNextChunk(socket, bytesForReading, fn)
        .then((nextBuffer) =>
          Buffer.concat([data.slice(bytesNoPayload, size), nextBuffer])
        )
        .then((result) => fn(rpcHeader, result, socket));
    }
  });
};

/**
 * given a buffer with request, it read a rpc header and rpc header payload size
 * if the buffer has a complete payload size it return buffer, Otherwise, it
 * waits for n "data" events with necessary data for complete payload size.
 * @param buffer
 * @param fn
 * @param socket
 */
const readRequestBuffer = (buffer: Buffer, fn: ApplyFn, socket: Socket) => {
  const rpcHeader = getAndValidateRpcHeader(buffer);
  const size = rpcHeader.payloadSize;
  if (buffer.length - bytesNoPayload >= size) {
    const result = buffer.slice(bytesNoPayload, size + bytesNoPayload);
    return fn(rpcHeader, result, socket);
  } else {
    const bytesForReading = size - (buffer.length - bytesNoPayload);
    return readNextChunk(socket, bytesForReading, fn)
      .then((nextBuffer) =>
        Buffer.concat([buffer.slice(bytesNoPayload), nextBuffer])
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
      if (data.length >= size) {
        if (data.length - size > 0) {
          readRequestBuffer(data.slice(size), fn, socket);
        } else {
          startReadRequest(fn)(socket);
        }
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
