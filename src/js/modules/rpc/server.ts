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

  /**
   * Close server connection
   */
  public close(): Promise<void> {
    return new Promise((resolve, reject) =>
      this.server.close((err) => {
        err && reject(err);
        this.coprocessorFileManager.close().then(resolve).catch(reject);
      })
    );
  }

  /**
   * Close coprocessor filesystem watcher process
   */
  public closeCoprocessorManager() {
    return this.coprocessorFileManager.close();
  }

  /**
   * Apply Coprocessors to CoprocessorRequest when it arrives
   * @param socket
   */
  private executeCoprocessorOnRequest = (socket: Socket) => {
    socket.on("readable", (data: Buffer) => {
      /**
       * TODO: https://app.clubhouse.io/vectorized/story/959
       */
      // @ts-ignore
      this.applyCoprocessor(data)
        .then(() => socket.write(""))
        .catch((e) => console.error("error: ", e));
    });
  };

  /**
   * Given a CoprocessorRequest, it'll find and execute Coprocessor by its
   * CoprocessorRequest's topic, if there is an exception when applying the
   * coprocessor function it handles the error by its ErrorPolicy
   * @param coprocessorRequest
   */
  private applyCoprocessor(
    coprocessorRequest: CoprocessorRequest
  ): Promise<CoprocessorRecordBatch[]> {
    const coprocessors =
      this.coprocessorRepository
        .getCoprocessorsByTopics()
        .get(coprocessorRequest.getTopic()) || [];
    const results = coprocessors.map((coprocessor) => {
      try {
        return Promise.resolve(
        coprocessorRequest
          .getRecords()
          .map(coprocessor.apply)
        );
      } catch (e) {
        return this.handleErrorByCoprocessorPolicy(
          coprocessor,
          coprocessorRequest,
          e
        );
      }
    });
    // @ts-ignore
    return Promise.allSettled(results).then((coprocessorResults) => {
      const array = [];
      coprocessorResults.forEach((result) => {
        if (result.status === "rejected") {
          console.error(result.reason);
        } else {
          array.push(result.value);
        }
      });
      return array;
    });
  }

  /**
   * Handle an error using the given Coprocessor's ErrorPolicy
   * @param coprocessor
   * @param coprocessorRequest
   * @param error
   */
  private handleErrorByCoprocessorPolicy(
    coprocessor: Coprocessor,
    coprocessorRequest: CoprocessorRequest,
    error: Error
  ): Promise<CoprocessorRecordBatch> {
    const errorMessage = this.createMessageError(
      coprocessor,
      coprocessorRequest,
      error
    );
    switch (coprocessor.policyError) {
      case PolicyError.Deregister:
        return this.coprocessorFileManager
          .deregisterCoprocessor(coprocessor)
          .then((_) => Promise.reject(errorMessage));
      case PolicyError.SkipOnFailure:
        return Promise.reject(errorMessage);
      default:
        return Promise.reject(errorMessage);
    }
  }

  private createMessageError(
    coprocessor: Coprocessor,
    coprocessorRequest: CoprocessorRequest,
    error: Error
  ): string {
    return (
      `Failed to apply coprocessor ${coprocessor.globalId} to request's id ` +
      `${coprocessorRequest.getId()}: ${error.message}`
    );
  }
  private server: NetServer;
  private toBytes: Serializer;
  private fromBytes: Deserializer;
}
