/**
 * Copyright 2020 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

import { getChecksumFromFile } from "../../modules/utilities/Checksum";
import * as assert from "assert";
import { join } from "path";

describe("Checksum utilities", () => {
  describe("When we call getChecksumFromFile", () => {
    const expectChecksum =
      "0374d40e1d2b885e0cb2f23f181d50443b6473ef819c10f3dc3e73e9b8b9f800";
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
