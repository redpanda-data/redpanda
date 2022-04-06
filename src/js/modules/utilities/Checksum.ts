/**
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

import { createHash } from "crypto";
import { createReadStream } from "fs";

export const getChecksumFromFile = (filePath: string): Promise<string> => {
  return new Promise((resolve, reject) => {
    try {
      const readFileIntoMemory = createReadStream(filePath);
      const hash = createHash("sha256");
      readFileIntoMemory.on("data", (data) => {
        try {
          hash.update(data);
        } catch (e) {
          reject(e);
        }
      });
      readFileIntoMemory.on("end", () => {
        try {
          const checksum = hash.digest("hex");
          resolve(checksum);
        } catch (e) {
          reject(e);
        }
      });
      readFileIntoMemory.on("error", (e) => {
        reject(e);
      });
    } catch (e) {
      reject(e);
    }
  });
};
