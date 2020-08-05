import { Socket, createServer, Server as NetServer } from "net";
import { Request } from "../domain/Request";
import Repository from "../supervisors/Repository";
import FileManager from "../supervisors/FileManager";
import {
  Coprocessor,
  CoprocessorRecordBatch,
  PolicyError,
} from "../public/Coprocessor";

export class Server {
  public constructor(
    activeDir: string,
    inactiveDir: string,
    submitDir: string
  ) {
    this.applyCoprocessor = this.applyCoprocessor.bind(this);
    this.repository = new Repository();
    this.fileManager = new FileManager(
      this.repository,
      submitDir,
      activeDir,
      inactiveDir
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
    socket.on("readable", (data: Buffer) => {
      /**
       * TODO: https://app.clubhouse.io/vectorized/story/959
       */
      // eslint-disable-next-line @typescript-eslint/ban-ts-comment
      // @ts-ignore
      this.applyCoprocessor(data)
        .then(() => socket.write(""))
        .catch((e) => console.error("error: ", e));
    });
  };

  /**
   * Given a Request, it'll find and execute Coprocessor by its
   * Request's topic, if there is an exception when applying the
   * coprocessor function it handles the error by its ErrorPolicy
   * @param request
   */
  private applyCoprocessor(
    request: Request
  ): Promise<CoprocessorRecordBatch[]> {
    const handleTable = this.repository
      .getCoprocessorsByTopics()
      .get(request.getTopic());
    if (handleTable) {
      const results = handleTable.apply(
        request,
        this.handleErrorByCoprocessorPolicy.bind(this)
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
   * @param request
   * @param error
   */
  private handleErrorByCoprocessorPolicy(
    coprocessor: Coprocessor,
    request: Request,
    error: Error
  ): Promise<CoprocessorRecordBatch> {
    const errorMessage = this.createMessageError(coprocessor, request, error);
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
    request: Request,
    error: Error
  ): string {
    return (
      `Failed to apply coprocessor ${coprocessor.globalId} to request's id ` +
      `${request.getId()}: ${error.message}`
    );
  }
  private server: NetServer;
  private readonly repository: Repository;
  private fileManager: FileManager;
}
