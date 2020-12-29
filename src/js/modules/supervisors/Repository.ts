/**
 * Copyright 2020 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

import { Handle } from "../domain/Handle";
import { Coprocessor } from "../public/Coprocessor";
import {
  ProcessBatchReplyItem,
  ProcessBatchRequestItem,
} from "../domain/generatedRpc/generatedClasses";
import { ProcessBatchServer } from "../rpc/server";
import {
  calculateRecordBatchSize,
  calculateRecordLength,
  createRecordBatch,
} from "../public";

/**
 * Repository is a container for Handles.
 */
class Repository {
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
  remove = (handle: Handle): void => {
    this.handles.delete(handle.coprocessor.globalId);
  };

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
          "Coprocessors don't register in wasm engine: " + nonExistHandle
        ),
      ];
    } else {
      return handles.reduce((prev, handle) => {
        const apply = requestItem.recordBatch.map((recordBatch) => {
          // Convert int16 to uint16 and check if have an unexpected compression
          if (((recordBatch.header.attrs >>> 0) & 0x7) != 0) {
            throw (
              "Record Batch has an unexpect compression value: baseOffset" +
              recordBatch.header.baseOffset
            );
          }
          try {
            //TODO: https://app.clubhouse.io/vectorized/story/1257
            //pass functor to apply function
            const resultRecordBatch = handle.coprocessor.apply(
              createRecordBatch(recordBatch)
            );

            const results: ProcessBatchReplyItem[] = [];
            for (const [key, value] of resultRecordBatch) {
              value.records = value.records.map((record) => {
                record.length = calculateRecordLength(record);
                record.valueLen = record.value.length;
                return record;
              });
              value.header.sizeBytes = calculateRecordBatchSize(value.records);
              value.header.term = recordBatch.header.term;
              results.push({
                coprocessorId: BigInt(handle.coprocessor.globalId),
                ntp: {
                  ...requestItem.ntp,
                  topic: `${requestItem.ntp.topic}.$${key}$`,
                },
                resultRecordBatch: [value],
              });
            }
            return Promise.resolve(results);
          } catch (e) {
            return handleError(handle.coprocessor, requestItem, e);
          }
        });
        return prev.concat(apply);
      }, []);
    }
  }

  /**
   * Map with coprocessor ID -> Handle
   * @private
   */
  private readonly handles: Map<bigint, Handle>;
}

export default Repository;
