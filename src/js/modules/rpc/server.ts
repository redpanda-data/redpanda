import { Socket, createServer, Server as NetServer } from "net";
import Repository from "../supervisors/Repository";
import FileManager from "../supervisors/FileManager";
import { Coprocessor, PolicyError } from "../public/Coprocessor";
import {
  ProcessBatchRequest,
  RpcHeader,
} from "../domain/generatedRpc/generatedClasses";
import BF from "../domain/generatedRpc/functions";
import { ManagementClient } from "./serverAndClients/server";
import { IOBuf } from "../utilities/IOBuf";

export class Server {
  public constructor(
    activeDir: string,
    inactiveDir: string,
    submitDir: string
  ) {
    this.managementClient = new ManagementClient(43188);
    this.applyCoprocessor = this.applyCoprocessor.bind(this);
    this.repository = new Repository();
    this.fileManager = new FileManager(
      this.repository,
      submitDir,
      activeDir,
      inactiveDir,
      this.managementClient
    );
    this.server = createServer(this.executeCoprocessorOnRequest);
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
        this.fileManager.close().then(resolve).catch(reject);
      })
    );
  }

  /**
   * Close coprocessor filesystem watcher process
   */
  public closeCoprocessorManager(): Promise<void> {
    return this.fileManager.close();
  }

  /**
   * Apply Coprocessors to Request when it arrives
   * @param socket
   */
  private executeCoprocessorOnRequest = (socket: Socket) => {
    socket.on("readable", () => {
      if (socket.readableLength > 26) {
        const [rpcHeader] = RpcHeader.fromBytes(socket.read(26));
        const [processBatchRequests] = BF.readArray()(
          socket.read(rpcHeader.payloadSize),
          0,
          (auxBuffer, auxOffset) =>
            BF.readObject(auxBuffer, auxOffset, ProcessBatchRequest)
        );
        if (rpcHeader.compression != 0) {
          throw "Rpc Header has an unexpect compression value:";
        }
        Promise.all(processBatchRequests.map(this.applyCoprocessor)).then(
          (result) => {
            const iobuf = new IOBuf();
            RpcHeader.toBytes(rpcHeader, iobuf);
            BF.writeArray(true)(result.flat(), iobuf, (item, auxBuffer) =>
              BF.writeObject(auxBuffer, ProcessBatchRequest, item)
            );
            iobuf.forEach((fragment) => socket.write(fragment.buffer));
          }
        );
      }
    });
  };

  /**
   * Given a Request, it'll find and execute Coprocessor by its
   * Request's topic, if there is an exception when applying the
   * coprocessor function it handles the error by its ErrorPolicy
   * @param processBatchRequest
   */
  private applyCoprocessor(
    processBatchRequest: ProcessBatchRequest
  ): Promise<ProcessBatchRequest[]> {
    const handleTable = this.repository
      .getCoprocessorsByTopics()
      .get(processBatchRequest.npt.topic);
    if (handleTable) {
      const results = handleTable.apply(
        processBatchRequest,
        this.handleErrorByPolicy.bind(this)
      );
      // eslint-disable-next-line @typescript-eslint/ban-ts-comment
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
    } else {
      return Promise.resolve([]);
    }
  }

  /**
   * Handle an error using the given Coprocessor's ErrorPolicy
   * @param coprocessor
   * @param processBatchRequest
   * @param error
   */
  private handleErrorByPolicy(
    coprocessor: Coprocessor,
    processBatchRequest: ProcessBatchRequest,
    error: Error
  ): Promise<ProcessBatchRequest> {
    const errorMessage = this.createMessageError(
      coprocessor,
      processBatchRequest,
      error
    );
    switch (coprocessor.policyError) {
      case PolicyError.Deregister:
        return this.fileManager
          .deregisterCoprocessor(coprocessor)
          .then(() => Promise.reject(errorMessage));
      case PolicyError.SkipOnFailure:
        return Promise.reject(errorMessage);
      default:
        return Promise.reject(errorMessage);
    }
  }

  private createMessageError(
    coprocessor: Coprocessor,
    processBatchRequest: ProcessBatchRequest,
    error: Error
  ): string {
    return (
      `Failed to apply coprocessor ${coprocessor.globalId} to request's id ` +
      `${processBatchRequest.recordBatch.header.baseOffset}: ${error.message}`
    );
  }

  private server: NetServer;
  private readonly repository: Repository;
  private fileManager: FileManager;
  managementClient: ManagementClient;
}
