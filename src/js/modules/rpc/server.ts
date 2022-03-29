/**
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

import Repository from "../supervisors/Repository";
import { Coprocessor, PolicyError } from "../public/Coprocessor";
import {
  DisableCoprocessorData,
  DisableCoprosReply,
  DisableCoprosRequest,
  EmptyRequest,
  EnableCoprocessor,
  EnableCoprocessorRequestData,
  EnableCoprosReply,
  EnableCoprosRequest,
  ProcessBatchReply,
  ProcessBatchReplyItem,
  ProcessBatchRequest,
  ProcessBatchRequestItem,
  StateSizeT,
} from "../domain/generatedRpc/generatedClasses";
import { SupervisorServer } from "./serverAndClients/rpcServer";
import { Handle } from "../domain/Handle";
import errors, { DisableResponseCode } from "./errors";
import { Logger } from "winston";
import Logging from "../utilities/Logging";
import requireNative from "./require-native";
import { isNtpEqual } from "../utilities/domain";
import { find } from "../utilities/Map";

function groupByTopic(
  processBatchReplyItems: ProcessBatchReplyItem[]
): ProcessBatchReplyItem[] {
  interface GroupByResultTopic {
    id: [ProcessBatchReplyItem["ntp"], ProcessBatchReplyItem["coprocessorId"]];
    value: ProcessBatchReplyItem;
  }

  const values = processBatchReplyItems
    .reduce((prev, result) => {
      const prevProcessBatch = find(
        prev,
        ([ntp, coprocId]) =>
          isNtpEqual(result.ntp, ntp) && coprocId === result.coprocessorId
      );

      if (prevProcessBatch === undefined) {
        return prev.set([result.ntp, result.coprocessorId], result);
      } else {
        const [ntpIdTuple, prevResult] = prevProcessBatch;
        if (
          prevResult.resultRecordBatch === undefined ||
          result.resultRecordBatch === undefined
        ) {
          return prev.set(ntpIdTuple, {
            ...prevResult,
            resultRecordBatch: undefined,
          });
        }
        const newResult: ProcessBatchReplyItem = {
          ...prevResult,
          resultRecordBatch: prevResult.resultRecordBatch.concat(
            result.resultRecordBatch
          ),
        };
        return prev.set(ntpIdTuple, newResult);
      }
    }, new Map<GroupByResultTopic["id"], GroupByResultTopic["value"]>())
    .values();
  return [...values];
}

export class ProcessBatchServer extends SupervisorServer {
  private readonly repository: Repository;
  private logger: Logger;

  constructor() {
    super();
    // TODO Can lookup the port redpanda is listening for copros on in the redpanda.yaml file
    this.logger = Logging.createLogger("server");
    this.applyCoprocessor = this.applyCoprocessor.bind(this);
    this.repository = new Repository();
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
      return Promise.all(input.requests.map(this.applyCoprocessor)).then(
        (result) => ({ result: result.flat() })
      );
    }
  }

  enable_coprocessors(input: EnableCoprosRequest): Promise<EnableCoprosReply> {
    const ids = input.coprocessors.map((script) => script.id);
    this.logger.info(`request enable wasm script: ${ids}`);
    const responses = input.coprocessors.map((definition) =>
      this.validateEnableCoprocInput(definition, this.logger)
    );
    return Promise.resolve({ responses });
  }

  disable_coprocessors(
    input: DisableCoprosRequest
  ): Promise<DisableCoprosReply> {
    const responses = input.ids.map<DisableCoprocessorData>((id) => {
      try {
        const [handle] = this.repository.getHandlesByCoprocessorIds([id]);
        if (handle === undefined) {
          this.logger.info(`error on disable wasm script doesn't exist: ${id}`);
          return errors.createDisableDoesNotExist(id);
        }
        this.logger.info(`wasm script disabled on nodejs engine: ${id}`);
        this.repository.remove(handle);
        return errors.createDisableSuccess(id);
      } catch (_) {
        this.logger.info(`error on disable wasm script: ${id}`);
        return errors.createDisableInternalError(id);
      }
    });
    return Promise.resolve({ responses });
  }

  disable_all_coprocessors(input: EmptyRequest): Promise<DisableCoprosReply> {
    const ids = this.repository.removeAll();
    const responses = ids.map<DisableCoprocessorData>((id) => ({
      id,
      disableResponseCode: DisableResponseCode.success,
    }));
    this.logger.info(`Disable all wasm scripts: ${ids}`);
    return Promise.resolve({ responses });
  }

  heartbeat(input: EmptyRequest): Promise<StateSizeT> {
    return Promise.resolve({ size: BigInt(this.repository.size()) });
  }

  validateEnableCoprocInput(
    handleDef: EnableCoprocessor,
    logger: Logger
  ): EnableCoprocessorRequestData {
    const { id, source_code } = handleDef;
    const [coprocessor, err] = this.loadCoprocFromString(id, source_code);
    if (err !== undefined) {
      return err;
    }
    const handle: Handle = {
      coprocessor,
      checksum: "",
    };
    const prevHandle = this.repository.findByCoprocessor(handle.coprocessor);
    if (prevHandle != undefined) {
      logger.info(`error on load wasm script: ${handleDef.id}`);
      return errors.createResponseScriptIdAlreadyExists(prevHandle);
    } else {
      if (handle.coprocessor.inputTopics.length === 0) {
        logger.info(`wasm script doesn't have topics: ${handleDef.id}`);
        return errors.createResponseScriptWithoutTopics(handle);
      }
      const validTopics = handle.coprocessor.inputTopics.reduce(
        (prev, [topic, _]) => errors.validateKafkaTopicName(topic) && prev,
        true
      );
      if (!validTopics) {
        logger.info(`invalid topic on wasm script: ${handleDef.id}`);
        return errors.createResponseScriptInvalidTopic(handle);
      }
      logger.info(
        `wasm script loaded on nodejs engine : ${handleDef.id} with topics ${handle.coprocessor.inputTopics}`
      );
      this.repository.add(handle);
      return errors.createResponseSuccess(handle);
    }
  }

  loadCoprocFromString(
    id: bigint,
    script: Buffer
  ): [Coprocessor, EnableCoprocessorRequestData] {
    /**
     * use a Function constructor, that function allows execute javascript
     * from a string, the first and second arguments are the parameter for
     * that function and the last one is the js string program.
     */
    const loadScript = (module, requireNative) =>
      Function("module", "require", script.toString())(module, requireNative);
    /**
     * Create a custom type for result function, as coprocessor script return
     * a exports.default value, ResultFunction type represents that result.
     */
    type ResultFunction = { exports?: { default?: Coprocessor } };
    /**
     * We create a 'module' result where our function save the object that
     * coprocessor script exports.
     */
    const module: ResultFunction = {
      exports: {},
    };
    /**
     * pass our module object and nodeJs require function.
     */
    try {
      loadScript(module, requireNative);
    } catch (e) {
      this.logger.error(`error on load wasm script: ${id}, ${e.message}`);
      return [undefined, errors.validateLoadScriptError(e, id, script)];
    }

    const handle = module?.exports?.default;
    if (handle === undefined) {
      this.logger.error(
        "Error on load script, script doesn't export anything. " +
          "Possible cause: script not packaged using NPM."
      );
      return [undefined, errors.validateLoadScriptError(null, id, script)];
    }
    if (!errors.validateWasmAttributes(handle, id, this.logger)) {
      return [undefined, errors.validateLoadScriptError(null, id, script)];
    }
    handle.globalId = id;
    return [handle, undefined];
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

    return Promise.all(results).then(
      (coprocessorResults) => groupByTopic(coprocessorResults.flat()),
      (e) => {
        this.logger.error(e);
        return [];
      }
    );
  }

  /**
   * Handle an error using the given Coprocessor's ErrorPolicy
   * @param handle
   * @param processBatchRequest
   * @param error
   * @param policyError, optional, by default this function takes value from
   * coprocessor.
   */
  public handleErrorByPolicy(
    handle: Handle,
    processBatchRequest: ProcessBatchRequestItem,
    error: Error,
    policyError = handle.coprocessor.policyError
  ): Promise<ProcessBatchReplyItem> {
    const coprocessor = handle.coprocessor;
    const errorMessage = this.createMessageError(
      coprocessor,
      processBatchRequest,
      error
    );
    switch (policyError) {
      case PolicyError.Deregister:
        this.logger.error(
          `Deregistering wasm transform ${coprocessor.globalId} due ` +
            `to active error policy "deregister"`
        );
        this.repository.remove(handle);
        return Promise.resolve({
          source: processBatchRequest.ntp,
          ntp: processBatchRequest.ntp,
          coprocessorId: coprocessor.globalId,
          resultRecordBatch: undefined,
        });
      case PolicyError.SkipOnFailure:
        return Promise.resolve({
          source: processBatchRequest.ntp,
          ntp: processBatchRequest.ntp,
          coprocessorId: coprocessor.globalId,
          resultRecordBatch: [],
        });
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
