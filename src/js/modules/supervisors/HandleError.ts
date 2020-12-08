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
  scriptDoesNotExist = 7,
}

export const validateDisableResponseCode = (
  responses: DisableCoprosReply,
  coprocessors: Coprocessor[]
): Error[] => {
  if (responses.inputs.length !== coprocessors.length) {
    throw new Error(
      "inconsistent response for disable coprocessors, " +
        `the disabled coprocessor response for ${coprocessors.join(", ")} ` +
        "doesn't have the same number item results, expected: " +
        `${coprocessors.length} result: ${responses.inputs.length}`
    );
  }
  return responses.inputs.reduceRight<Error[]>((errors, code, index) => {
    switch (code) {
      case DisableResponseCode.internalError: {
        errors.push(
          new Error(`wasm function ID: ${coprocessors[index].globalId}`)
        );
        break;
      }
      case DisableResponseCode.scriptDoesNotExist: {
        errors.push(
          new Error(
            `script with ID ${coprocessors[index].globalId} ` + "doesn't exist."
          )
        );
      }
    }
    return errors;
  }, []);
};

export const validateEnableResponseCode = (
  responses: EnableCoprosReply,
  coprocessors: Coprocessor[]
): Error[][] => {
  if (responses.inputs.length !== coprocessors.length) {
    throw new Error(
      "inconsistent response for enable coprocessors, " +
        `the enable coprocessor response for ${coprocessors.join(", ")} ` +
        "doesn't have the same number item results, expected: " +
        `${coprocessors.length} result: ${responses.inputs.length}`
    );
  }
  return responses.inputs.map((response, index) => {
    const { id, response: codes } = response;
    return codes.reduceRight<Error[]>((errors, code, topicIndex) => {
      const coprocessor = coprocessors[index];
      switch (code) {
        case EnableResponseCode.internalError: {
          errors.push(new Error(`wasm function ID: ${id}`));
          break;
        }
        case EnableResponseCode.invalidIngestionPolicy: {
          errors.push(new Error(`invalid ingestion policy`));
          break;
        }
        case EnableResponseCode.invalidTopic: {
          errors.push(
            new Error(`invalid topic "${coprocessor.inputTopics[topicIndex]}"`)
          );
          break;
        }
        case EnableResponseCode.topicDoesNotExist: {
          errors.push(
            new Error(
              `topic "${coprocessor.inputTopics[topicIndex]}" ` +
                `doesn't exist`
            )
          );
          break;
        }
        case EnableResponseCode.scriptIdAlreadyExist: {
          errors.push(
            new Error(
              "wasm function already register, ID: " + coprocessor.globalId
            )
          );
          break;
        }
        case EnableResponseCode.materializedTopic: {
          errors.push(new Error("materialized topic"));
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
