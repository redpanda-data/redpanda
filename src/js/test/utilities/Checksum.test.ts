import {getChecksumFromFile} from "../../modules/utilities/Checksum";
import * as assert from "assert";
import {join} from "path"

describe("Checksum utilities", () => {
  describe("When we call getChecksumFromFile", () => {
    const expectChecksum = "9W4k/uZKoPMZxDJyvj7rvcO8NYWGFKtd7MOR4UA9Wls="
    describe("Given a existing file", () => {
      it("Should get file content's checksum", (done) => {
        const checksumPromise = getChecksumFromFile(join(__dirname, "/CoprocessorTest.js"))
        checksumPromise.then(checksum => {
          assert.equal(checksum, expectChecksum, "checksum unexpected")
          done()
        }).catch(done)
      })
    })
    describe("Given a unexiting file", () => {
      it('should it return a Promise reject', () => {
        const checksumPromise = getChecksumFromFile("NoFile.t")
        assert.rejects(checksumPromise, {
          code: "ENOENT"
        })
      });
    })
  })
})