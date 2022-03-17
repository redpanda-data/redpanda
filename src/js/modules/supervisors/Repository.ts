/**
 * Copyright 2020 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

import { Handle } from "../domain/Handle";
import { Coprocessor, PolicyError, RecordBatch } from "../public/Coprocessor";
import {
  ProcessBatchReplyItem,
  ProcessBatchRequestItem,
} from "../domain/generatedRpc/generatedClasses";
import { ProcessBatchServer } from "../rpc/server";
import {
  calculateRecordBatchSize,
  calculateRecordLength,
  copyRecordBatch,
} from "../public";
import LogService from "../utilities/Logging";

/**
 * Repository is a container for Handles.
 */
class Repository {
  private logger = LogService.createLogger("FileManager");
  private wasmLogger = LogService.createLogger("coproc");
  constructor() {
    this.handles = new Map();
  }

  /**
   * this method adds a new Handle to the repository
   * @param handle
   */
  add(handle: Handle): Handle {
    const currentHandleTable = this.handles.get(handle.coprocessor.globalId);
    if (currentHandleTable) {
      this.remove(handle);
    }
    this.handles.set(handle.coprocessor.globalId, handle);
    return handle;
  }

  /**
   *
   * findByGlobalId method receives a coprocessor and returns a
   * Handle if there exists one with the same global ID as the given
   * coprocessor. Returns undefined otherwise.
   * @param handle
   */
  findByGlobalId = (handle: Handle): Handle | undefined =>
    this.handles.get(handle.coprocessor.globalId);

  /**
   * Given a Coprocessor, try to find one with the same global ID and return it
   * if it exists, returns undefined otherwise
   * @param coprocessor
   */
  findByCoprocessor(coprocessor: Coprocessor): Handle | undefined {
    return this.handles.get(coprocessor.globalId);
  }

  /**
   * remove a handle from the handle map
   * @param handle
   */
  remove(handle: Handle): void {
    this.handles.delete(handle.coprocessor.globalId);
  }

  /**
   * returns a handle list by given ids
   * @param ids
   */
  getHandlesByCoprocessorIds(ids: bigint[]): Handle[] {
    return ids.reduce((prev, id) => {
      const handle = this.handles.get(id);
      if (handle) {
        prev.push(handle);
      }
      return prev;
    }, []);
  }

  size(): number {
    return this.handles.size;
  }

  createEmptyProcessBatchReplay(
    handle: Handle,
    requestItem: ProcessBatchRequestItem
  ): ProcessBatchReplyItem {
    return {
      coprocessorId: BigInt(handle.coprocessor.globalId),
      source: requestItem.ntp,
      ntp: {
        ...requestItem.ntp,
        topic: `${requestItem.ntp.topic}`,
      },
      resultRecordBatch: [],
    };
  }

  validateResultApply(
    requestItem: ProcessBatchRequestItem,
    handle: Handle,
    handleError: ProcessBatchServer["handleErrorByPolicy"],
    resultRecordBatch: Map<string, RecordBatch>
  ): Promise<Map<string, RecordBatch>> {
    if (resultRecordBatch instanceof Map) {
      return Promise.resolve(resultRecordBatch);
    } else {
      const error = new Error(
        `Wasm function (${handle.coprocessor.globalId}) ` +
          `didn't return a Promise<Map<string, RecordBatch>>`
      );
      return Promise.reject(
        handleError(handle, requestItem, error, PolicyError.Deregister)
      );
    }
  }

  transformMapResultToArray(
    requestItem: ProcessBatchRequestItem,
    handle: Handle,
    recordBatch: RecordBatch,
    resultRecordBatch: Map<string, RecordBatch>
  ): ProcessBatchReplyItem[] {
    const results: ProcessBatchReplyItem[] = [];
    if (resultRecordBatch.size === 0) {
      /*
             Coprocessor returns a empty Map, in this case, it responses to
             empty process batch replay to Redpanda in order to avoid, send this
             request again.
            */

      results.push(this.createEmptyProcessBatchReplay(handle, requestItem));
    } else {
      // Coprocessor return a Map with values
      for (const [key, value] of resultRecordBatch) {
        value.records = value.records.map((record) => {
          record.valueLen = record.value.length;
          record.length = calculateRecordLength(record);
          return record;
        });
        value.header.sizeBytes = calculateRecordBatchSize(value.records);
        value.header.term = recordBatch.header.term;
        value.header.recordCount = value.records.length;
        results.push({
          coprocessorId: BigInt(handle.coprocessor.globalId),
          source: requestItem.ntp,
          ntp: {
            ...requestItem.ntp,
            topic: `${requestItem.ntp.topic}._${key}_`,
          },
          resultRecordBatch: [value],
        });
      }
    }
    return results;
  }

  applyCoprocessor(
    CoprocessorIds: bigint[],
    requestItem: ProcessBatchRequestItem,
    handleError: ProcessBatchServer["handleErrorByPolicy"],
    createException: ProcessBatchServer["fireException"]
  ): Promise<ProcessBatchReplyItem[]>[] {
    const handles = this.getHandlesByCoprocessorIds(requestItem.coprocessorIds);
    if (handles.length != requestItem.coprocessorIds.length) {
      const nonExistHandle = requestItem.coprocessorIds.filter(
        (id) => !handles.find((handle) => handle.coprocessor.globalId === id)
      );
      return [
        createException(
          "Coprocessors not registered in wasm engine: " + nonExistHandle
        ),
      ];
    } else {
      return handles.reduce((prev, handle) => {
        const apply = requestItem.recordBatch.map((recordBatch) => {
          // Convert int16 to uint16 and check if have an unexpected compression
          if (((recordBatch.header.attrs >>> 0) & 0x7) != 0) {
            throw (
              "Record Batch has an unexpected compression value: baseOffset" +
              recordBatch.header.baseOffset
            );
          }
          try {
            return handle.coprocessor
              .apply(copyRecordBatch(recordBatch), this.wasmLogger)
              .then((resultMap) =>
                this.validateResultApply(
                  requestItem,
                  handle,
                  handleError,
                  resultMap
                )
              )
              .then((resultMap) =>
                this.transformMapResultToArray(
                  requestItem,
                  handle,
                  recordBatch,
                  resultMap
                )
              )
              .catch((responseError: ProcessBatchReplyItem) => {
                return responseError;
              });
          } catch (e) {
            return handleError(handle, requestItem, e);
          }
        });
        return prev.concat(apply);
      }, []);
    }
  }

  removeAll(): Array<bigint> {
    const ids = [...this.handles.entries()].map(([id]) => id);
    this.handles.clear();
    return ids;
  }

  /**
   * Map with coprocessor ID -> Handle
   * @private
   */
  private readonly handles: Map<bigint, Handle>;
}

export default Repository;
