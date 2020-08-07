import { Handle } from "../domain/Handle";
import { Coprocessor, RecordBatch } from "../public/Coprocessor";
import { Request } from "../domain/Request";
import { Server } from "../rpc/server";

export class HandleTable {
  constructor() {
    this.coprocessors = new Map<number, Handle>();
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
   * @param request, Request instance
   * @param handleError, function that handle error on when apply coprocessor
   *
   * Note: Server["handleErrorByPolicy"] is a ts helper to specify
   * an object's property type.
   * https://www.typescriptlang.org/docs/handbook/release-notes/
   * typescript-2-1.html#keyof-and-lookup-types
   */
  apply(
    request: Request,
    handleError: Server["handleErrorByPolicy"]
  ): Promise<RecordBatch>[] {
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
  private readonly coprocessors: Map<number, Handle>;
}
