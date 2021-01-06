import { LogService } from "../../modules/utilities/Logging";
import assert = require("assert");
import { readFile } from "fs";
import * as fs from "fs";
import * as path from "path";

describe("Logging Wrap", () => {
  const logPath = path.resolve("./test/utilities/logs.log");

  before(() => {
    try {
      fs.unlink(logPath, () => {
        console.log("remove file");
      });
    } catch (e) {
      undefined;
    }
  });

  it("shouldn't change log path after set the first time ", function () {
    const LoggerWrapInstance = new LogService();
    LoggerWrapInstance.setPath(logPath);
    assert.strictEqual(LoggerWrapInstance.getPath(), logPath);
    LoggerWrapInstance.setPath("another/path");
    assert.strictEqual(LoggerWrapInstance.getPath(), logPath);
  });

  it("should wait for closing loggers", function (done) {
    const LoggerWrapInstance = new LogService();
    LoggerWrapInstance.setPath(logPath);
    const logger1 = LoggerWrapInstance.createLogger("test");
    const logger2 = LoggerWrapInstance.createLogger("test2");

    logger1.info("test1.1");
    logger1.info("test1.2");
    logger2.info("test2.1");
    logger2.info("test2.2");

    LoggerWrapInstance.close().then(() => {
      // add a timeout in order to wait for creating log file, when we put small
      // logs on winston it doesn't create the log file immediately
      setTimeout(() => {
        readFile(logPath, (err, data) => {
          if (err) {
            done(err);
          }
          // it doesn't check all log format because the time changes for each
          // test, just check the log message.
          assert(data.toString().includes("[test] info: test1.1\n"));
          assert(data.toString().includes("[test] info: test1.2\n"));
          assert(data.toString().includes("[test2] info: test2.1\n"));
          assert(data.toString().includes("[test2] info: test2.2\n"));
          done();
        });
      }, 10);
    });
  });
});
