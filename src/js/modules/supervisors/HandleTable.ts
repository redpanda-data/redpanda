import { CoprocessorHandle } from "../domain/CoprocessorManager";
import { Coprocessor, CoprocessorRecordBatch } from "../public/Coprocessor";
import { CoprocessorRequest } from "../domain/CoprocessorRequest";
import { Server } from "../rpc/server";

export class HandleTable {
  constructor() {
    this.coprocessors = new Map<number, CoprocessorHandle>();
  }

  registerHandle(handle: CoprocessorHandle): void {
    this.coprocessors.set(handle.coprocessor.globalId, handle);
  }

  deregisterHandle(handle: CoprocessorHandle): void {
    this.coprocessors.delete(handle.coprocessor.globalId);
  }

  findHandleById(handle: CoprocessorHandle): CoprocessorHandle | undefined {
    return this.coprocessors.get(handle.coprocessor.globalId);
  }

  findHandleByCoprocessor(
    coprocessor: Coprocessor
  ): CoprocessorHandle | undefined {
    return this.coprocessors.get(coprocessor.globalId);
  }

  /**
   * Given a Request, apply every coprocessor on handleTable defined for each
   * RecordBatch's topic
   * @param request, Request instance
   * @param handleError, function that handle error on when apply coprocessor
   *
   * Note: Server["handleErrorByCoprocessorPolicy"] is a ts helper to specify
   * an object's property type.
   * https://www.typescriptlang.org/docs/handbook/release-notes/
   * typescript-2-1.html#keyof-and-lookup-types
   */
  apply(
    request: CoprocessorRequest,
    handleError: Server["handleErrorByCoprocessorPolicy"]
  ): Promise<CoprocessorRecordBatch>[] {
    return request.getRecords().flatMap((recordBatch) =>
      [...this.coprocessors.values()].map((handle) => {
        try {
          return Promise.resolve(handle.coprocessor.apply(recordBatch));
        } catch (e) {
          return handleError(handle.coprocessor, request, e);
        }
      })
    );
  }
  size(): number {
    return this.coprocessors.size;
  }
  private readonly coprocessors: Map<number, CoprocessorHandle>;
}
