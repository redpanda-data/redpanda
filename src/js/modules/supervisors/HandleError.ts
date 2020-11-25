import {
  DisableCoprosReply,
  EnableCoprosReply,
} from "../domain/generatedRpc/enableDisableCoprocClasses";
import { Coprocessor } from "../public/Coprocessor";

export enum EnableResponseCode {
  success,
  internalError,
  invalidIngestionPolicy,
  scriptIdAlreadyExist,
  topicDoesNotExist,
  invalidTopic,
  materializedTopic,
}

export enum DisableResponseCode {
  success,
  internalError,
  scriptDoesNotExist,
}

export const validateDisableResponseCode = (
  responses: DisableCoprosReply,
  coprocessor: Coprocessor[]
): Error[] => {
  return responses.inputs.reduceRight<Error[]>((errors, code, index) => {
    switch (code) {
      case DisableResponseCode.internalError: {
        errors.push(
          new Error(
            `internal error: wasm function ID: ${coprocessor[index].globalId}`
          )
        );
        break;
      }
      case DisableResponseCode.scriptDoesNotExist: {
        errors.push(
          new Error(
            `error: script with ID ${coprocessor[index].globalId}` +
              "doesn't exist."
          )
        );
      }
    }
    return errors;
  }, []);
};

export const validateEnableResponseCode = (
  responses: EnableCoprosReply
): Error[][] => {
  return responses.inputs.map((response) => {
    const { id, response: codes } = response;
    return codes.reduceRight<Error[]>((errors, code) => {
      switch (code) {
        case EnableResponseCode.internalError: {
          errors.push(new Error(`internal error: wasm id function: ${id}`));
          break;
        }
        case EnableResponseCode.invalidIngestionPolicy: {
          errors.push(new Error(`error: invalid ingestion policy`));
          break;
        }
        case EnableResponseCode.invalidTopic: {
          errors.push(new Error(`error: invalid topic`));
          break;
        }
        case EnableResponseCode.topicDoesNotExist: {
          errors.push(new Error(`error: invalid doesn't exist`));
          break;
        }
        case EnableResponseCode.scriptIdAlreadyExist: {
          errors.push(new Error(`error: wasm function already register`));
          break;
        }
        case EnableResponseCode.materializedTopic: {
          errors.push(new Error(`error: materialized topic`));
          break;
        }
        case EnableResponseCode.success: {
          break;
        }
        default:
          break;
      }
      return errors;
    }, []);
  });
};
