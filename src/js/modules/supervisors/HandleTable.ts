import { Handle } from "../domain/Handle";
import { Coprocessor } from "../public/Coprocessor";
import { Server } from "../rpc/server";
import { ProcessBatchRequest } from "../domain/generatedRpc/generatedClasses";

export class HandleTable {
  constructor() {
    this.coprocessors = new Map<bigint, Handle>();
  }

  registerHandle(handle: Handle): void {
    this.coprocessors.set(handle.coprocessor.globalId, handle);
  }

  deregisterHandle(handle: Handle): void {
    this.coprocessors.delete(handle.coprocessor.globalId);
  }

  findHandleById(handle: Handle): Handle | undefined {
    return this.coprocessors.get(handle.coprocessor.globalId);
  }

  findHandleByCoprocessor(coprocessor: Coprocessor): Handle | undefined {
    return this.coprocessors.get(coprocessor.globalId);
  }

  /**
   * Given a Request, apply every coprocessor on handleTable defined for each
   * RecordBatch's topic
   * @param processBatchRequest, Request instance
   * @param handleError, function that handle error on when apply coprocessor
   *
   * Note: Server["handleErrorByPolicy"] is a ts helper to specify
   * an object's property type.
   * https://www.typescriptlang.org/docs/handbook/release-notes/
   * typescript-2-1.html#keyof-and-lookup-types
   */
  apply(
    processBatchRequest: ProcessBatchRequest,
    handleError: Server["handleErrorByPolicy"]
  ): Promise<ProcessBatchRequest>[] {
    return [...this.coprocessors.values()].map((handle) => {
      // Convert int16 to uint16 and check if have an unexpected compression
      if (((processBatchRequest.recordBatch.header.attrs >>> 0) & 0x7) != 0) {
        throw (
          "Record Batch has an unexpect compression value: baseOffset" +
          processBatchRequest.recordBatch.header.baseOffset
        );
      }
      try {
        //TODO: https://app.clubhouse.io/vectorized/story/1257
        //pass functor to apply function
        const resultRecordBatch = handle.coprocessor.apply(
          processBatchRequest.recordBatch
        );

        return Promise.resolve({
          ...processBatchRequest,
          recordBatch: resultRecordBatch,
        });
      } catch (e) {
        return handleError(handle.coprocessor, processBatchRequest, e);
      }
    });
  }

  size(): number {
    return this.coprocessors.size;
  }

  private readonly coprocessors: Map<bigint, Handle>;
}
