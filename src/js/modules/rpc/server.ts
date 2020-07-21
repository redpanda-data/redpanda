import { Socket, createServer, Server as NetServer } from "net";
import { Deserializer, Serializer } from "./parser";
import { RecordBatch } from "./types";
import { RpcXxhash64 } from "../hashing/xxhash";
import { RpcHeaderCrc32 } from "../hashing/crc32";
import "buffer";

export class Server {
  public constructor() {
    this.server = createServer((socket: Socket) => {
      this.do_on_data(socket);
      this.do_on_disconnect(socket);
    });
    this.toBytes = new Serializer();
    this.fromBytes = new Deserializer();
  }

  /**
   * Starts the server on the given port.
   * @param port
   */
  public listen(port: number): Promise<void> {
    return new Promise((resolve, reject) => {
      try {
        this.server.listen(port, "127.0.0.1", null, resolve);
      } catch (e) {
        reject(e);
      }
    });
  }
  private do_on_data(socket: Socket) {
    socket.on("data", (data: Buffer) => {
      let header = this.fromBytes.rpcHeader(data);
      let payload = data.subarray(26);
      this.fromBytes.verifyPayload(payload, header.payloadChecksum);
      let input: Array<RecordBatch> = this.fromBytes.recordBatchReader(payload);
      let output: Array<Array<RecordBatch>> = [input];

      let bytes = this.toBytes.materializedRecordBatchReader(output);
      header.payload = header.payload + 4;
      header.payloadChecksum = RpcXxhash64(bytes);
      let hbytes = this.toBytes.rpcHeader(header);

      let crc = RpcHeaderCrc32(hbytes);
      header.headerChecksum = crc;
      hbytes = this.toBytes.rpcHeader(header);

      socket.write(Buffer.concat([hbytes, bytes]));
    });
  }

  private server: NetServer;
  private toBytes: Serializer;
  private fromBytes: Deserializer;
}
