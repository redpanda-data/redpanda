/**
 * Copyright 2020 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

import { ProcessBatchServer } from "./server";
import { safeLoadAll } from "js-yaml";
import { resolve } from "path";
import * as fs from "fs";
import LogService from "../utilities/Logging";

// read yaml config file
const readConfigFile = (confPath: string): Promise<Record<string, any>> => {
  return fs.promises
    .readFile(confPath)
    .then((file) => safeLoadAll(file)[0])
    .catch((e) => {
      return Promise.reject(
        new Error(`Error reading config file: ${e.message}`)
      );
    });
};

export const closeProcess = (e: Error): Promise<void> => {
  return LogService.close()
    .then(() => {
      if (LogService.getPath() == undefined) {
        console.error(
          `Failed before logger initialized with exception: ${e.message}`
        );
      } else {
        fs.writeFile(
          LogService.getPath(),
          `Error: ${e.message}`,
          { flag: "a+" },
          (err) => {
            if (err) {
              console.error(
                "failing on write exception on " +
                  LogService.getPath() +
                  " Error: " +
                  err.message
              );
            }
          }
        );
      }
    })
    .then(() => {
      console.log("Closing process");
      process.exit(1);
    });
};

function main() {
  // read config file path argument
  const configPathArg = process.argv.splice(2)[0];
  const defaultConfigPath = "/etc/redpanda/redpanda.yaml";
  const defaultLogFile = "/var/lib/redpanda/coprocessor/logs/wasm";
  // resolve config path or assign default value
  const configPath = configPathArg ? resolve(configPathArg) : defaultConfigPath;

  readConfigFile(configPath)
    .then((config) => {
      const port = config?.redpanda?.coproc_supervisor_server?.port || 43189;
      const logFilePath = config?.coproc_engine?.logFilePath || defaultLogFile;
      LogService.setPath(logFilePath);
      const logger = LogService.createLogger("service");
      logger.info("Starting redpanda wasm service...");
      logger.info(`Reading from config file: ${configPath}`);
      const service = new ProcessBatchServer();
      process.on("SIGINT", function () {
        service
          .closeConnection()
          .then(() => LogService.close())
          .then(() => process.exit());
      });
      logger.info(`Starting redpanda wasm service on port: ${port}`);
      service.listen(port);
    })
    .catch(closeProcess);
}

main();
