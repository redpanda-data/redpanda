import {
  DisableResponseCode,
  EnableResponseCode,
  validateDisableResponseCode,
  validateEnableResponseCode,
} from "../../modules/supervisors/HandleError";
import * as assert from "assert";
import { createMockCoprocessor } from "../testUtilities";

describe("handle error enable and disable coprocessor", () => {
  [
    {
      codes: [EnableResponseCode.internalError],
      errorNumber: 1,
    },
    {
      codes: [EnableResponseCode.materializedTopic],
      errorNumber: 1,
    },
    {
      codes: [EnableResponseCode.scriptIdAlreadyExist],
      errorNumber: 1,
    },
    {
      codes: [EnableResponseCode.invalidTopic],
      errorNumber: 1,
    },
    {
      codes: [EnableResponseCode.invalidIngestionPolicy],
      errorNumber: 1,
    },
    {
      codes: [EnableResponseCode.topicDoesNotExist],
      errorNumber: 1,
    },
    {
      codes: [EnableResponseCode.success],
      errorNumber: 0,
    },
    {
      codes: [
        EnableResponseCode.topicDoesNotExist,
        EnableResponseCode.invalidTopic,
      ],
      errorNumber: 2,
    },
    {
      codes: [
        EnableResponseCode.topicDoesNotExist,
        EnableResponseCode.internalError,
      ],
      errorNumber: 2,
    },
  ].forEach(({ codes, errorNumber }) => {
    it(
      `should validateEnableCodeResponse returns an array with ` +
        `${errorNumber} errors, when it receives ${codes} enable ` +
        `response code`,
      function () {
        const [errors] = validateEnableResponseCode(
          {
            inputs: [
              {
                id: BigInt(1),
                response: codes,
              },
            ],
          },
          [createMockCoprocessor()]
        );
        assert.ok(errors.length === errorNumber);
      }
    );
  });

  [
    {
      codes: [DisableResponseCode.internalError],
      errorNumber: 1,
    },
    {
      codes: [DisableResponseCode.scriptDoesNotExist],
      errorNumber: 1,
    },
    {
      codes: [DisableResponseCode.success],
      errorNumber: 0,
    },
    {
      codes: [
        DisableResponseCode.internalError,
        DisableResponseCode.scriptDoesNotExist,
      ],
      errorNumber: 2,
      coprocessors: [
        createMockCoprocessor(BigInt(1)),
        createMockCoprocessor(BigInt(2)),
      ],
    },
    {
      codes: [
        DisableResponseCode.internalError,
        DisableResponseCode.scriptDoesNotExist,
      ],
      errorNumber: 2,
      fail: true,
    },
  ].forEach(({ codes, errorNumber, coprocessors, fail = false }) => {
    it(
      `should validateDisableCodeResponse returns an array with ` +
        `${errorNumber} errors, when it receives ${codes} disable ` +
        `response code`,
      function () {
        try {
          const errors = validateDisableResponseCode(
            {
              inputs: codes,
            },
            coprocessors || [createMockCoprocessor(BigInt(1))]
          );
          assert.ok(errors.length === errorNumber);
        } catch (e) {
          assert.ok(fail, "it should be fail");
        }
      }
    );
  });
});
