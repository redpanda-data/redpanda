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
          resolve(e);
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
