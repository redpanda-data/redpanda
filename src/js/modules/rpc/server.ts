import Repository from "../supervisors/Repository";
import FileManager from "../supervisors/FileManager";
import { Coprocessor, PolicyError } from "../public/Coprocessor";
import {
  ProcessBatchReply,
  ProcessBatchReplyItem,
  ProcessBatchRequest,
  ProcessBatchRequestItem,
} from "../domain/generatedRpc/generatedClasses";
import { Script_ManagerClient as ManagementClient } from "./serverAndClients/server";
import { SupervisorServer } from "./serverAndClients/processBatch";

export class ProcessBatchServer extends SupervisorServer {
  private readonly repository: Repository;
  private fileManager: FileManager;
  managementClient: ManagementClient;

  constructor(activeDir: string, inactiveDir: string, submitDir: string) {
    super();
    this.managementClient = new ManagementClient(43118);
    this.applyCoprocessor = this.applyCoprocessor.bind(this);
    this.repository = new Repository();
    this.fileManager = new FileManager(
      this.repository,
      submitDir,
      activeDir,
      inactiveDir,
      this.managementClient
    );
  }

  fireException(message: string): Promise<never> {
    return Promise.reject(new Error(message));
  }

  process_batch(input: ProcessBatchRequest): Promise<ProcessBatchReply> {
    const failRequest = input.requests.find(
      (request) => request.coprocessorIds.length === 0
    );
    if (failRequest) {
      return this.fireException("Bad request: request without coprocessor ids");
    } else {
      return Promise.all(
        input.requests.map(this.applyCoprocessor)
      ).then((result) => ({ result: result.flat() }));
    }
  }

  /**
   * Given a Request, it'll find and execute Coprocessor by its
   * Request's topic, if there is an exception when applying the
   * coprocessor function it handles the error by its ErrorPolicy
   * @param requestItem
   */
  private applyCoprocessor(
    requestItem: ProcessBatchRequestItem
  ): Promise<ProcessBatchReplyItem[]> {
    const results = this.repository.applyCoprocessor(
      requestItem.coprocessorIds,
      requestItem,
      this.handleErrorByPolicy.bind(this),
      this.fireException
    );

    return Promise.allSettled(results).then((coprocessorResults) => {
      const array: ProcessBatchReplyItem[][] = [];
      coprocessorResults.forEach((result) => {
        if (result.status === "rejected") {
          console.error(result.reason);
        } else {
          array.push(result.value);
        }
      });
      return array.flat();
    });
  }

  /**
   * Handle an error using the given Coprocessor's ErrorPolicy
   * @param coprocessor
   * @param processBatchRequest
   * @param error
   */
  private handleErrorByPolicy(
    coprocessor: Coprocessor,
    processBatchRequest: ProcessBatchRequestItem,
    error: Error
  ): Promise<never> {
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
    processBatchRequest: ProcessBatchRequestItem,
    error: Error
  ): string {
    return (
      `Failed to apply coprocessor ${coprocessor.globalId} to request's id :` +
      `${processBatchRequest.recordBatch
        .map((rb) => rb.header.baseOffset)
        .join(", ")}: ${error.message}`
    );
  }
}
