/**
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

import {
  DisableCoprocessorData,
  EnableCoprocessor,
  EnableCoprocessorMetadataItem,
  EnableCoprocessorRequestData,
} from "../domain/generatedRpc/generatedClasses";
import { Handle } from "../domain/Handle";
import { Coprocessor } from "../public/Coprocessor";
import { Logger } from "winston";

export enum EnableResponseCodes {
  success,
  internalError,
  scriptIdAlreadyExists,
  scriptContainsInvalidTopic,
  scriptContainsNoTopics,
  scriptContainsSyntaxError,
}

export enum DisableResponseCode {
  success,
  internalError,
  scriptDoesNotExist,
}

const maxSizeTopicName = 249;
const validateKafkaTopicName = (topic: string): boolean => {
  const validChars = /^[a-zA-Z0-9\.\_\-]*$/;
  if (topic.length === 0) {
    return false;
  } else if (topic == "." || topic == "..") {
    return false;
  } else if (topic.length > maxSizeTopicName) {
    return false;
  } else if (!validChars.test(topic)) {
    return false;
  }
  return true;
};

type EnableCoprocResponse<A> = (handle: A) => EnableCoprocessorRequestData;
type HandleEnableResponse = EnableCoprocResponse<Handle>;
type SimpleEnableResponse = EnableCoprocResponse<EnableCoprocessor>;
type DisableCoprocResponse = (id: bigint) => DisableCoprocessorData;

const validateLoadScriptError = (
  e: Error,
  id: bigint,
  script: Buffer
): EnableCoprocessorRequestData => {
  if (e instanceof SyntaxError) {
    return createResponseScriptSyntaxError({ id, source_code: script });
  } else {
    return createResponseInternalError({ id, source_code: script });
  }
};

const createResponseSuccess: HandleEnableResponse = (handle: Handle) => ({
  enableResponseCode: EnableResponseCodes.success,
  scriptMetadata: {
    id: handle.coprocessor.globalId,
    inputTopic:
      handle.coprocessor.inputTopics.map<EnableCoprocessorMetadataItem>(
        ([topic, policy]) => ({
          topic,
          ingestion_policy: policy,
        })
      ),
  },
});

const createResponseInternalError: SimpleEnableResponse = (handleDef) => ({
  enableResponseCode: EnableResponseCodes.internalError,
  scriptMetadata: { id: handleDef.id, inputTopic: [] },
});

const createResponseScriptIdAlreadyExists: HandleEnableResponse = (handle) => ({
  enableResponseCode: EnableResponseCodes.scriptIdAlreadyExists,
  scriptMetadata: {
    id: handle.coprocessor.globalId,
    inputTopic:
      handle.coprocessor.inputTopics.map<EnableCoprocessorMetadataItem>(
        ([topic, policy]) => ({
          topic,
          ingestion_policy: policy,
        })
      ),
  },
});

const createResponseScriptInvalidTopic: HandleEnableResponse = (handle) => ({
  enableResponseCode: EnableResponseCodes.scriptContainsInvalidTopic,
  scriptMetadata: {
    id: handle.coprocessor.globalId,
    inputTopic: [],
  },
});

const createResponseScriptWithoutTopics: HandleEnableResponse = (handle) => ({
  enableResponseCode: EnableResponseCodes.scriptContainsNoTopics,
  scriptMetadata: {
    id: handle.coprocessor.globalId,
    inputTopic: [],
  },
});

const createResponseScriptSyntaxError: SimpleEnableResponse = (handleDef) => ({
  // the enableResponseCode should be:
  // enableResponseCode: EnableResponseCodes.EnableResponseCodes.internalError,
  // but redpanda doesn't support that error yet
  enableResponseCode: EnableResponseCodes.internalError,
  scriptMetadata: { id: handleDef.id, inputTopic: [] },
});

const createDisableInternalError: DisableCoprocResponse = (id) => ({
  id,
  disableResponseCode: DisableResponseCode.internalError,
});

const createDisableSuccess: DisableCoprocResponse = (id) => ({
  id,
  disableResponseCode: DisableResponseCode.success,
});

const createDisableDoesNotExist: DisableCoprocResponse = (id) => ({
  id,
  disableResponseCode: DisableResponseCode.scriptDoesNotExist,
});

const validateWasmAttributes = (
  handle: Coprocessor,
  id: bigint,
  logger: Logger
): boolean => {
  if (handle === undefined) {
    logger.error(`Wasm script doesn't export anything, script id ${id}`);
    return false;
  }
  if (handle.inputTopics === undefined || handle.inputTopics.length === 0) {
    logger.error(`Wasm script doesn't have topics, script id ${id}`);
    return false;
  }
  if (handle.apply === undefined) {
    logger.error(`Wasm script doesn't have apply function, script id ${id}`);
    return false;
  }
  if (handle.policyError === undefined) {
    logger.error(`Wasm script doesn't have policy error, script id ${id}`);
    return false;
  }
  return true;
};

export default {
  validateLoadScriptError,
  validateKafkaTopicName,
  createResponseSuccess,
  createResponseInternalError,
  createResponseScriptIdAlreadyExists,
  createResponseScriptInvalidTopic,
  createResponseScriptWithoutTopics,
  createResponseScriptSyntaxError,
  createDisableInternalError,
  createDisableSuccess,
  createDisableDoesNotExist,
  validateWasmAttributes,
};
