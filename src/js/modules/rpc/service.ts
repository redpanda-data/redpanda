import { ProcessBatchServer } from "./server";
import { safeLoadAll } from "js-yaml";
import { join, resolve } from "path";
import * as fs from "fs";
import { promisify } from "util";

// read yaml config file
const readConfigFile = (confPath: string): Promise<Record<string, any>> => {
  try {
    return fs.promises.readFile(confPath)
      .then(file => safeLoadAll(file)[0]);
  } catch (e) {
    return Promise.reject(new Error(`Error reading config file: ${e}`));
  }
};

/*
  validate scaffolding on coprocessor path, if the expected folders don't
  exist, it'll create them
*/
const validateOrCreateScaffolding = (directoryPath: string): Promise<void> => {
  const exists = promisify(fs.exists);
  const validations = ["active", "inactive", "submit"].map(folder => {
    const path = join(directoryPath, folder);
    return exists(path)
      .then(exist => {
        if (!exist) {
          console.log(path);
          return fs.promises.mkdir(path, { recursive: true });
        }
      });
  });
  return Promise.all(validations).then(() => null);
};

// read config file path argument
const configPathArg = process.argv.splice(2)[0];
const defaultConfigPath = "/var/lib/redpanda/conf/redpanda.yaml";
const defaultCoprocessorPath = "/var/lib/redpanda/coprocessor"
// resolve config path or assign default value
const configPath = configPathArg ? resolve(configPathArg) : defaultConfigPath;

readConfigFile(configPath).then((config => {
  const port = config?.redpanda?.coproc_supervisor_server || 43189;
  const path = config?.coproc_engine?.path || defaultCoprocessorPath;
  validateOrCreateScaffolding(path).then(() => {
    const service = new ProcessBatchServer(
      path + "/active",
      path + "/inactive",
      path + "/submit"
    );
    service.listen(port);
  });
}));
