import { Coprocessor, PolicyError } from "../modules/public/Coprocessor";
import { Handle } from "../modules/domain/Handle";
import { HandleTable } from "../modules/supervisors/HandleTable";

export const createMockCoprocessor = (
  globalId: Coprocessor["globalId"] = BigInt(1),
  inputTopics: Coprocessor["inputTopics"] = ["topicA"],
  policyError: Coprocessor["policyError"] = PolicyError.SkipOnFailure,
  apply: Coprocessor["apply"] = () => undefined
): Coprocessor => ({
  globalId,
  inputTopics,
  policyError,
  apply,
});

export const createHandle = (coprocessor?: Partial<Coprocessor>): Handle => ({
  coprocessor: createMockCoprocessor(
    coprocessor?.globalId,
    coprocessor?.inputTopics,
    coprocessor?.policyError,
    coprocessor?.apply
  ),
  checksum: "check",
  filename: "file",
});

export const createHandleTable = (
  handle: Partial<Handle> = createHandle()
): HandleTable => {
  const handleTable = new HandleTable();
  handleTable.registerHandle({ ...createHandle(), ...handle });
  return handleTable;
};
