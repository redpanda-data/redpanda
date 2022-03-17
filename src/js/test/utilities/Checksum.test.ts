/**
 * Copyright 2020 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

import { getChecksumFromFile } from "../../modules/utilities/Checksum";
import * as assert from "assert";
import { join } from "path";

describe("Checksum utilities", () => {
  describe("When we call getChecksumFromFile", () => {
    const expectChecksum =
      "83e60a8e53690ea88e3970e8edfb703700e0a7e8d566ab9fa19a8598d612917e";
    describe("Given a existing file", () => {
      it("Should get file content's checksum", (done) => {
        const checksumPromise = getChecksumFromFile(
          join(__dirname, "/CoprocessorTest.ts")
        );
        checksumPromise
          .then((checksum) => {
            assert.equal(checksum, expectChecksum, "checksum unexpected");
            done();
          })
          .catch(done);
      });
    });
    describe("Given a unexiting file", () => {
      it("should it return a Promise reject", () => {
        const checksumPromise = getChecksumFromFile("NoFile.t");
        assert.rejects(checksumPromise, {
          code: "ENOENT",
        });
      });
    });
  });
});
