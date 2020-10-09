#!/usr/bin/env python3
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
  Server as NetServer
} from "net";
import { XXHash64 } from "xxhash";
import { calculate } from "fast-crc32c";
import { IOBuf } from "../../utilities/IOBuf";

import {
{%- for import in js_imports%}
  {{import}},
{%- endfor %}
  RpcHeader
} from "{{js_include}}"

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
      payloadChecksum: hasher.digest().readBigUInt64BE()
    };
    const auxHeader = Buffer.alloc(26)
    RpcHeader.toBytes(rpc, IOBuf.createFromBuffers([auxHeader]));
    rpc.headerChecksum = createCrc32(auxHeader.slice(0, 26), 5, 26)
    RpcHeader.toBytes(rpc, reserve);
}

"""

server_template = """
export class {{service_name.title()}}Server {
  constructor() {
    this.server = createServer(this.executeMethod.bind(this));
    this.handleNewConnection()
  }
  
  listen(port: number){
    this.server.listen(port)
  }
  
  executeMethod(socket: Socket) {
    socket.on("readable", () => {
      if (socket.readableLength > 26) {
        const [rpcHeader, crc32Validation, crc32] =
          validateRpcHeader(socket.read(26));
        if (!crc32Validation){
          throw(`Crc32 inconsistent, expect: ${rpcHeader.headerChecksum}`+
            `generated: ${crc32}`
          )
        } else {
          switch (rpcHeader.meta) {
            {%- for method in methods %}
            case {{method.id}}: {
              const [value] = {{method.input_ts}}
                .fromBytes(socket.read(rpcHeader.payloadSize))
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
                    buffer.forEach((fragment => socket.write(fragment.buffer)))
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
              buffer.forEach((fragment => socket.write(fragment.buffer)));
            }
          }
        }
      }
    })
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
  constructor(port: number) {
    this.client = createConnection({ port }, () => {
    console.log("Established connection with redpanda");
    this.client.on("data", data => {
      const [rpc] = RpcHeader.fromBytes(data, 0);
        this.responseHandlers.get(rpc.correlationId)(data)
      });
    })
  }
    
  send(buffer: IOBuf, headerReserve: IOBuf, size: number, meta: number): number{
    const correlationId = ++this.correlationId;
    generateRpcHeader(buffer, headerReserve, size, meta, correlationId);
    buffer.forEach((fragment => this.client.write(fragment.buffer)));
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
            const [rpcHeader, crc32Validation, crc32] =
              validateRpcHeader(data)
            if(!crc32Validation){
              throw(`Crc32 inconsistent, expect: ${rpcHeader.headerChecksum}`+
                `generated: ${crc32}`
              )
            } else {
              resolve({{method.output_ts}}.fromBytes(data, 26)[0])
            }
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
