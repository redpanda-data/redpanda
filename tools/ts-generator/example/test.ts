import { IOBuf } from "../../../src/js/modules/utilities/IOBuf";
import { Class1, Class2, Class3 } from "./generated";
import * as assert from "assert";

const classSigned: Class2 = {
  numberSigned8: 8,
  numberSigned16: 24,
  numberSigned32: 123,
};

const classUSigned: Class3 = {
  numberUSigned8: 255,
  numberUSigned16: 65535,
  numberUSigned32: 4294967295,
  optional: undefined,
};

const class1: Class1 = {
  bigint: BigInt(1234567890),
  arrayValue: ["value 1", "value 2"],
  booleanValue: true,
  bufferValue: Buffer.from("Buffer Value"),
  stringValue: "value 3",
  varintValue: BigInt(123789),
  classSigned: classSigned,
  classUSigned: classUSigned,
  mixCustomType: [undefined, "string", undefined, undefined, "other string"],
};

type GeneratorTest = {
  test: string;
  assertFn: (ws: number, classResult: Class1, rs: number) => void;
};
const tests: GeneratorTest[] = [
  {
    test: "the write result size should same to read result size",
    assertFn: (ws, classResult, rs) => {
      assert.strictEqual(ws, rs);
    },
  },
];

describe("Generator Code", () => {
  tests.forEach(({ test, assertFn }) => {
    const preTest = (): [number, Class1, number] => {
      // Create buffer where the binary data is going to be save
      const io = new IOBuf();
      // Write into the buffer
      const writtenByteSize = Class1.toBytes(class1, io);
      // Read data from the buffer
      const bufferResult = io.getIterable().slice(writtenByteSize);
      const result = Class1.fromBytes(bufferResult);
      return [writtenByteSize, ...result];
    };
    it(test, function () {
      assertFn(...preTest());
    });
  });
});
