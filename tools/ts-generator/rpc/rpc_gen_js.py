#!/usr/bin/env python3
# Copyright 2020 Redpanda Data, Inc.
#
# Licensed as a Redpanda Enterprise file under the Redpanda Community
# License (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
# https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

import sys
import os
import logging
import json
import zlib
from shutil import copy

# 3rd party
from jinja2 import Template

sys.path.append(os.path.dirname(__file__))
logger = logging.getLogger('rp')

imports_template = """
import {
  createConnection,
  Socket,
  createServer,
  Server as NetServer,
  AddressInfo,
} from "net";
import { XXHash64 } from "xxhash";
import { calculate } from  "fast-crc32c/impls/js_crc32c";
import { IOBuf } from "../../utilities/IOBuf";

import {
{%- for import in js_imports%}
  {{import}},
{%- endfor %}
  RpcHeader
} from "{{js_include}}"

const rpcHeaderSize = 26;
type ApplyFn = (rpcH: RpcHeader, buf: Buffer, socket: Socket) => void

function createCrc32(buffer: Buffer, start, end): number {
  return calculate(buffer.subarray(start, end));
}

function validateRpcHeader(
  buffer: Buffer,
  offset = 0): [RpcHeader, boolean, number] {
    const [rpcHeader] = RpcHeader.fromBytes(buffer, offset)
    const crc32 = createCrc32(buffer, 5, 26)
    return [rpcHeader, rpcHeader.headerChecksum === crc32, crc32]
}

function generateRpcHeader(
  buffer: IOBuf,
  reserve: IOBuf,
  size: number,
  meta: number,
  correlationId: number){
    const hasher = new XXHash64(0)
    buffer.forEach((fragment) => {
      if(fragment.getSize() === 512) {
        hasher.update(fragment.buffer.slice(26, fragment.used))
      } else {
        hasher.update(fragment.buffer.slice(0, fragment.used))
      }
    })
    const rpc: RpcHeader = {
      version: 0,
      headerChecksum: 0,
      compression: 0,
      payloadSize: size,
      meta,
      correlationId,
      payloadChecksum: hasher.digest().readBigUInt64LE()
    };
    const auxHeader = Buffer.alloc(26)
    RpcHeader.toBytes(rpc, IOBuf.createFromBuffers([auxHeader]));
    rpc.headerChecksum = createCrc32(auxHeader.slice(0, 26), 5, 26)
    RpcHeader.toBytes(rpc, reserve);
}

function writeToSocketWithCheck(
  buffer: IOBuf,
  socket: Socket){
    try {
      buffer.forEach((fragment =>
        socket.write(fragment.buffer.slice(0, fragment.used))
      ));
    }
    catch(e) {
      console.log('Fail write to socket: ', e);
      socket.destroy();
      socket.emit("close", true);
    }
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
  try {
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
    const availableBytesOnBuffer = buffer.length - rpcHeaderSize;
    if (availableBytesOnBuffer == size) {
      const result = buffer.slice(rpcHeaderSize, size + rpcHeaderSize);
      startReadRequest(fn)(socket)
      return fn(rpcHeader, result, socket);
    } else if (availableBytesOnBuffer > size) {
      const result = buffer.slice(rpcHeaderSize, size + rpcHeaderSize);
      const restBuffer = buffer.slice(size + rpcHeaderSize)
      if( restBuffer.length >= rpcHeaderSize ) {
        // in this case, we can read a rpc header from residual buffer
        readBufferRequest(buffer.slice(size + rpcHeaderSize), fn, socket);
        return fn(rpcHeader, result, socket);
      } else {
        // in this case, we can't read a rpc header from residual buffer, therefore
        // we need to wait for next chunk, before we continue reading 
        return readNextChunk(socket, 0, fn, true)
          .then(nextChunk => {
            readBufferRequest(
              Buffer.concat([restBuffer, nextChunk]),
              fn,
              socket
            );
            return fn(rpcHeader, result, socket)
          })
      }
    } else {
      const bytesForReading = size - availableBytesOnBuffer;
      return readNextChunk(socket, bytesForReading, fn)
        .then((nextBuffer) =>
          Buffer.concat([buffer.slice(rpcHeaderSize), nextBuffer])
        )
        .then((result) => fn(rpcHeader, result, socket));
    }
  }
  catch(e) {
    console.log('Read request error: ', e);
    socket.destroy();
    socket.emit("close", true);
  }
};

/**
 * given payload size, it waits for n "data" events until have the complete data
 * size.
 * @param socket
 * @param size
 * @param fn
 * @param returnComplete
 */
const readNextChunk = (
  socket: Socket,
  size: number,
  fn: ApplyFn,
  returnComplete?: boolean
): Promise<Buffer> => {
  return new Promise<Buffer>((resolve) => {
    socket.once("data", (data) => {
      if (returnComplete === true) {
        return resolve(data)
      } else if (data.length > size) {
        const isCompleteChunk = (data.length - size) >= 26
        if (isCompleteChunk) {
          readBufferRequest(data.slice(size), fn, socket);
        } else {
          readNextChunk(socket, 0, fn, true)
            .then((nextChunk) => readBufferRequest(
              Buffer.concat([data.slice(size), nextChunk]), fn, socket));
        }
        return resolve(data.slice(0, size))
      } else if (data.length === size) {
        startReadRequest(fn)(socket);
        return resolve(data);
      } else {
        return readNextChunk(socket, size - data.length, fn).then(
          (nextBuffer) => resolve(Buffer.concat([data, nextBuffer]))
        );
      }
    });
  });
};

"""

server_template = """
export class {{service_name.title()}}Server {
  constructor() {
    this.server = createServer(this.executeMethod.bind(this));
    this.handleNewConnection()
    this.process = this.process.bind(this)
  }

  // Sometimes JS tests fail in CI because "port already in use"
  // error when the server tries to listen. The problem maybe because
  // previous instance of the test didn't shutdown completely. Here
  // we tried to fix it by listening to random port.
  listenRandomPort() {
    return new Promise<number>((resolve, reject) => {
      this.server.on("error", (err: NodeJS.ErrnoException) => {
        console.error("Server listen error: ", err);
        reject(err);
      });

      this.server.listen(() => {
        const { port } = this.server.address() as AddressInfo;
        resolve(port);
      });
    });
  }

  listen(port: number) {
    this.server.on("error", (err: NodeJS.ErrnoException) => {
      console.error("Server listen error: ", err);
    });

    this.server.listen(port);
  }

  process(rpcHeader: RpcHeader, payload: Buffer, socket: Socket) {
    switch (rpcHeader.meta) {
      {%- for method in methods %}
      case {{method.id}}: {
        const [value] = {{method.input_ts}}
          .fromBytes(payload)
        this.{{method.name}}(value)
          .then((output: {{method.output_ts}}) => {
              const buffer = new IOBuf();
              const rpcHeaderReserve = buffer.getReserve(26)
              const size = {{method.output_ts}}
                .toBytes(output, buffer)
              generateRpcHeader(
                buffer,
                rpcHeaderReserve,
                size,
                200,
                rpcHeader.correlationId
              )
              writeToSocketWithCheck(buffer, socket);
          })
        break;
      }
      {%- endfor %}
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
        writeToSocketWithCheck(buffer, socket);
      }
    }
  }

  executeMethod(socket: Socket) {
    startReadRequest(this.process)(socket)
  }

  {% for method in methods %}
  {{method.name}}(input: {{method.input_ts}}): Promise<{{method.output_ts}}> {
    return Promise.resolve(null)
  }
  {% endfor %}

  handleNewConnection() {
    this.server.on('connection', (conn) => {
      const key = conn.remoteAddress + ':' + conn.remotePort;
      this.connections.set(key, conn);
      conn.on("error", function(err) {
        console.log("Connection error: ", err);
        conn.destroy();
        conn.emit("close", true);
      })
      conn.on('close', () => {
        this.connections.delete(key);
      });
    });
  }

  closeConnection(): Promise<void> {
    return new Promise((resolve, reject) => {
      try {
        this.server.close()
        for (const connection of this.connections.values()) {
          connection.destroy()
        }
        resolve()
      } catch (e) {
        reject(e)
      }
    })
  }

  server: NetServer;
  connections: Map<string, Socket> = new Map();
}
"""

client_template = """
export class {{service_name.title()}}Client {
  private constructor(client: Socket) {
    // false because a constructor represents an already connected client
    this.process = this.process.bind(this);
    this.isDisconnected = false;
    this.client = client;
    startReadRequest(this.process)(this.client);
    this.client.on('end', () => {
      this.isDisconnected = true;
    });
  }

  static create(port: number) {
    return new Promise<{{service_name.title()}}Client>((resolve, reject) => {
      let client = createConnection({ port }, () => {
        resolve(new {{service_name.title()}}Client(client));
      }).on('error', err => reject(err));
    });
  }

  isConnected() { return !this.isDisconnected; }

  process(rpcHeader: RpcHeader, payload: Buffer, socket: Socket) {
    this.responseHandlers.get(rpcHeader.correlationId)(payload);
  }

  send(buffer: IOBuf, headerReserve: IOBuf, size: number, meta: number): number{
    const correlationId = ++this.correlationId;
    generateRpcHeader(buffer, headerReserve, size, meta, correlationId);
    buffer.forEach((fragment =>
      this.client.write(fragment.buffer.slice(0, fragment.used))
    ));
    return this.correlationId;
  }

  rawSend(buffer: IOBuf): number{
    buffer.forEach((fragment =>
      this.client.write(fragment.buffer.slice(0, fragment.used))
    ));
    return this.correlationId;
  }
  
  {% for method in methods %}
  {{method.name}}(input: {{method.input_ts}}): Promise<{{method.output_ts}}> {
    return new Promise((resolve, reject) => {
      try{
        const buffer = new IOBuf();
        const rpcHeaderReserve = buffer.getReserve(26);
        const size = {{method.input_ts}}.toBytes(input, buffer)
        const correlationId = this.send(
          buffer,
          rpcHeaderReserve,
          size,
          {{method.id}}
        );
        this.responseHandlers.set(correlationId, (data: Buffer) => {
          try {
              resolve({{method.output_ts}}.fromBytes(data)[0])
            } catch(e) {reject(e)}
          }
        )} catch(e){ reject(e) }
    })
  }
 {% endfor %}

  close() {
    this.client.destroy()
  }

  // Map for indexing the server response handlers, where the key is the
  // correlationId, and the value is a function that takes a buffer response,
  // and transform it into the expected output type
  responseHandlers = new Map<number, (buffer) => void>()
  client: Socket;
  isDisconnected: boolean;
  correlationId = 0;
}
"""


def read_file(name):
    with open(name, 'r') as f:
        try:
            return json.load(f)
        except:
            logger.error(
                "Error: try to read input file, but there is "
                "a problem with json format ", name)


def create_class(json):
    tpl = Template(imports_template + server_template + client_template)
    return tpl.render(json)


def write(code_generated, out_path):
    open(out_path, 'w').write(code_generated)


def save_in_file(generated_code, path):
    (dir_name, file_name) = os.path.split(path)
    try:
        os.makedirs(dir_name)
    except FileExistsError:
        pass
    finally:
        write(generated_code, path)


def add_import_list(service):
    imports = []
    for m in service["methods"]:
        imports.append(m["input_ts"])
        imports.append(m["output_ts"])

    service["js_imports"] = list(dict.fromkeys(imports))
    return service


def add_id_to_method(service):
    logger.info(service)

    service["id"] = zlib.crc32(
        bytes("%s:%s" % (service["namespace"], service["service_name"]),
              "utf-8"))

    def _xor_id(m):
        mid = ("%s:" % service["namespace"]).join(
            [m["name"], m["input_type"], m["output_type"]])
        return service["id"] ^ zlib.crc32(bytes(mid, 'utf-8'))

    for m in service["methods"]:
        m["id"] = _xor_id(m)

    return service


def add_ts_type(service):
    def to_camel_case(snake):
        components = snake.split('_')
        return ''.join(x.title() for x in components[0:])

    for method in service["methods"]:
        method["input_ts"] = to_camel_case(method["input_type"])
        method["output_ts"] = to_camel_case(method["output_type"])
    return service


def main():
    import argparse

    def generate_options():
        parser = argparse.ArgumentParser(
            description='deserializer and serializer code generator')
        parser.add_argument(
            '--log',
            type=str,
            default='INFO',
            help='info,debug, type log levels. i.e: --log=debug')
        parser.add_argument('--server-define-file',
                            type=str,
                            required=True,
                            help='input file in .json format for the codegen')
        parser.add_argument('--output-file',
                            type=str,
                            required=True,
                            help='output header file for the codegen')
        return parser

    parser = generate_options()
    options, program_options = parser.parse_known_args()
    logger.info("%s" % options)
    json_file = add_import_list(
        add_ts_type(add_id_to_method(read_file(options.server_define_file))))
    save_in_file(create_class(json_file), options.output_file)


if __name__ == '__main__':
    main()
