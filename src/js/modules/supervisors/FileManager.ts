/**
 * Copyright 2020 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

import { FSWatcher, watch } from "chokidar";
import { rename, readdir } from "fs";
import { promisify } from "util";
import Repository from "./Repository";
import { Handle } from "../domain/Handle";
import LogService from "../utilities/Logging";
import { getChecksumFromFile } from "../utilities/Checksum";
import { Coprocessor, PolicyInjection } from "../public/Coprocessor";
import { Script_ManagerClient as ManagementClient } from "../rpc/serverAndClients/server";
import * as path from "path";
import { hash64 } from "xxhash";
import {
  EnableResponseCode as EnableCode,
  validateDisableResponseCode,
  validateEnableResponseCode,
} from "./HandleError";
import { closeProcess } from "../rpc/service";

/**
 * FileManager class is an inotify implementation, it receives a
 * Repository and updates this object when to  add a new file in
 * submit folder and read previous files from the active folder when
 * this class is instanced
 */
class FileManager {
  private submitDirWatcher: FSWatcher;
  private activeDirWatcher: FSWatcher;
  private managementClient: ManagementClient;
  private logger = LogService.createLogger("FileManager");
  constructor(
    private repository: Repository,
    private submitDir: string,
    private activeDir: string,
    private inactiveDir: string
  ) {
    this.readCoprocessorFolder(repository, this.activeDir, false)
      .then(() => this.readCoprocessorFolder(repository, this.submitDir))
      .then(() => this.startWatchers(repository))
      .catch((e) => {
        // fatal error close
        closeProcess(e);
      });
  }

  /**
   * AddCoprocessor gets coprocessor from filePath and decides if the
   * coprocessor is moving between active and inactive directories
   * @param filePath, path of a coprocessor that we want to load and add to
   *        Repository
   * @param repository, coprocessor container
   * @param validatePrevCoprocessor
   */
  addCoprocessor(
    filePath: string,
    repository: Repository,
    validatePrevCoprocessor = true
  ): Promise<Handle> {
    return this.getHandle(filePath).then((handle) => {
      const prevHandle = repository.findByGlobalId(handle);
      if (prevHandle) {
        if (prevHandle.checksum === handle.checksum) {
          return this.moveCoprocessorFile(handle, this.inactiveDir);
        } else {
          return this.moveCoprocessorFile(prevHandle, this.inactiveDir)
            .then(() =>
              this.deregisterCoprocessor(prevHandle.coprocessor).catch((e) => {
                this.logger.error(e.message);
                return Promise.resolve();
              })
            )
            .then(() => this.moveCoprocessorFile(handle, this.activeDir))
            .then((newHandle) => repository.add(newHandle))
            .then((newHandle) =>
              this.enableCoprocessor(
                [newHandle.coprocessor],
                validatePrevCoprocessor
              )
                .then(() => newHandle)
                .catch((error) =>
                  this.moveCoprocessorFile(newHandle, this.inactiveDir)
                    .then(() => this.repository.remove(newHandle))
                    .then(() => Promise.reject(error))
                )
            );
        }
      } else {
        return this.moveCoprocessorFile(handle, this.activeDir)
          .then((newHandle) => repository.add(newHandle))
          .then((newHandle) =>
            this.enableCoprocessor(
              [newHandle.coprocessor],
              validatePrevCoprocessor
            )
              .then(() => newHandle)
              .catch((errors) =>
                this.moveCoprocessorFile(newHandle, this.inactiveDir)
                  .then(() => this.repository.remove(newHandle))
                  .then(() => Promise.reject(errors))
              )
          );
      }
    });
  }

  /**
   * reads the files in the given folder, loads them as Handles
   * and adds them to the given Repository
   * @param repository
   * @param folder
   * @param validatePrevExist
   */
  readCoprocessorFolder(
    repository: Repository,
    folder: string,
    validatePrevExist = true
  ): Promise<void> {
    const readdirPromise = promisify(readdir);
    return readdirPromise(folder).then((files) => {
      files.forEach((file) =>
        this.addCoprocessor(
          path.join(folder, file),
          repository,
          validatePrevExist
        ).catch((e) => this.logger.error(e.message))
      );
    });
    //TODO: implement winston for loggin information and error handler
  }

  /**
   * Updates the given Repository instance when a coprocessor is removed
   * from folder.
   * @param filePath, removed handle path
   * @param repository, is a coprocessor container
   */
  removeHandleFromFilePath(
    filePath: string,
    repository: Repository
  ): Promise<void> {
    const name = path.basename(filePath, ".js");
    const id = hash64(Buffer.from(name), 0).readBigUInt64LE();
    const [handle] = repository.getHandlesByCoprocessorIds([id]);
    if (!handle) {
      this.logger.error(
        `Trying to disable a removed coprocessor from 'active' folder but it` +
          `wasn't loaded in memory, file name: ${name}.js`
      );
      return Promise.resolve();
    } else {
      this.repository.remove(handle);
      return this.disableCoprocessors([handle.coprocessor])
        .then(() => {
          this.logger.info(
            `disabled coprocessor: ID ${handle.coprocessor.globalId} ` +
              `filename: '${name}.js'`
          );
        })
        .catch((err) => {
          this.logger.error(
            `disable_coprocessors RPC returned with errors: ${err}`
          );
        });
    }
  }

  /**
   * allow closing the inotify process
   */
  close = (): Promise<void> =>
    this.submitDirWatcher.close().then(this.activeDirWatcher.close);

  /**
   * Deregister the given Coprocessor and move the file where it's defined to
   * the 'inactive' folder.
   * @param coprocessor is a Coprocessor implementation.
   */
  deregisterCoprocessor(coprocessor: Coprocessor): Promise<Handle> {
    const handle = this.repository.findByCoprocessor(coprocessor);
    if (handle) {
      return this.disableCoprocessors([handle.coprocessor])
        .then(() => this.moveCoprocessorFile(handle, this.inactiveDir))
        .then((coprocessor) => {
          this.repository.remove(coprocessor);
          return coprocessor;
        });
    } else {
      return Promise.reject(
        new Error(
          `A coprocessor with ID ${coprocessor.globalId} hasn't been loaded`
        )
      );
    }
  }

  /**
   * receive an Error array, and return one error with all error messages.
   * @param errors
   */
  compactErrors(errors: Error[][]): Error {
    const message = errors
      .flatMap((error) => error.map(({ message }) => message))
      .join(", ");
    return new Error(message);
  }

  /**
   * Receives a coprocessor list, and sends a request to Redpanda for disabling
   * them. The response has the following structure:
   * [<topic status>]
   *
   * Possible coprocessor statuses:
   *   0 = success
   *   1 = internal error
   *   2 = script doesn't exist
   * @param coprocessors
   */
  disableCoprocessors(coprocessors: Coprocessor[]): Promise<void> {
    this.logger.info(
      `Initiating RPC call to redpandas coprocessor service at endpoint - ` +
        `disable_coprocessors with data: ${coprocessors}`
    );
    return this.getClient().then((client) => {
      return client
        .disable_copros({
          inputs: coprocessors.map((coproc) => coproc.globalId),
        })
        .then((response) => {
          const errors = validateDisableResponseCode(response, coprocessors);
          if (errors.length > 0) {
            const compactedErrors = this.compactErrors([errors]);
            this.logger.error(
              `disable_coprocessors() RPC returned with ` +
                `errors: ${compactedErrors}`
            );
            return Promise.reject(compactedErrors);
          }
          return Promise.resolve();
        })
        .catch((e) => {
          return Promise.reject(
            `disable_coprocessors() RPC failed: ${e.message}`
          );
        });
    });
  }

  /**
   * Receives a coprocessor list, and sends a request to Redpanda for enabling
   * them. The response has the following structure:
   * [{<coprocessorId>, [<topic status>]}]
   *
   * Possible topic statuses:
   *   0 = success
   *   1 = internal error
   *   2 = invalid ingestion policy
   *   3 = script id already exist
   *   4 = topic doesn't exist
   *   5 = invalid topic
   *   6 = materialized topic
   * @param coprocessors
   * @param validatePrevCoprocessor
   */
  enableCoprocessor(
    coprocessors: Coprocessor[],
    validatePrevCoprocessor = true
  ): Promise<void> {
    if (coprocessors.length == 0) {
      return Promise.resolve();
    }
    this.logger.info(
      `Initiating RPC call to redpandas coprocessor service at endpoint - ` +
        `enable_coprocessors with data: ${coprocessors}`
    );
    return this.getClient().then((client) => {
      return client
        .enable_copros({
          coprocessors: coprocessors.map((coproc) => ({
            id: coproc.globalId,
            topics: coproc.inputTopics.map((topic) => ({
              topic,
              injectionPolicy: PolicyInjection.LastOffset,
            })),
          })),
        })
        .then((enableResponse) => {
          if (!validatePrevCoprocessor) {
            enableResponse.inputs = enableResponse.inputs.map(
              ({ response, id }) => ({
                id,
                response: response.filter(
                  (code) => code !== EnableCode.scriptIdAlreadyExist
                ),
              })
            );
          }
          const errors = validateEnableResponseCode(
            enableResponse,
            coprocessors
          );
          if (errors.find((errors) => errors.length > 0)) {
            const compactedErrors = this.compactErrors(errors);
            this.logger.error(
              `enable_coprocessors RPC returned with some error: ` +
                `${compactedErrors}`
            );
            return Promise.reject(compactedErrors);
          }
          return Promise.resolve();
        });
    });
  }

  /**
   * Loads a JS file with the given filename, and returns a Handle for it, which
   * also contains the file's content's checksum.
   * @param filename, path of the file that we need to get coprocessor
   *                  information.
   */
  getHandle(filename: string): Promise<Handle> {
    return new Promise<Handle>((resolve, reject) => {
      try {
        const script = require(filename);
        delete require.cache[filename];
        const fileChecksum = getChecksumFromFile(filename);
        const name = path.basename(filename, ".js");
        const id = hash64(Buffer.from(name), 0).readBigUInt64LE();
        const coprocessor = script.default;
        coprocessor.globalId = id;
        fileChecksum
          .then((checksum) =>
            resolve({
              coprocessor,
              checksum,
              filename,
            })
          )
          .catch(reject);
      } catch (e) {
        reject(e);
      }
    });
  }

  /**
   * moveCoprocessorFile moves coprocessor from the current filepath to
   * destination path, also changes the name of the file for its checksum value
   * and adds its SHA hash as a prefix.
   * @param coprocessor, is a coprocessor loaded in memory
   * @param destination, destination path
   */
  moveCoprocessorFile(
    coprocessor: Handle,
    destination: string
  ): Promise<Handle> {
    const renamePromise = promisify(rename);
    const name = path.basename(coprocessor.filename, ".js");
    let destinationPath;
    /** Each coprocessor needs an ID, which is calculated based on the
     * coprocessor script filename using xxhash64. When the script is detected
     * in the /submit folder, the coprocessor engine calculates the hash based
     * on its filename, minus its extension (e.g.  wasm.js -> hash(wasm)). When
     * the engine moves a script from /active to /inactive, the filename changes
     * following this format: `<filename>.js.vectorized.<sha256>.bk **/
    if (destination == this.activeDir) {
      destinationPath = `${destination}/${name}.js`;
    } else {
      destinationPath = `${destination}/${name}.js.vectorized.${coprocessor.checksum}.bk`;
    }
    return renamePromise(coprocessor.filename, destinationPath).then(() => ({
      ...coprocessor,
      filename: destinationPath,
    }));
  }

  /**
   * add event listeners for:
   * update file: updates the given Repository instance when a new coprocessor
   * file is added.
   * remove file: removes from given Repository instance the deleted coprocessor
   * definition
   *
   * @param repository
   */
  private startWatchers(repository: Repository): void {
    this.submitDirWatcher = watch(this.submitDir).on("add", (filePath) => {
      this.logger.info(`Detected new file in submit dir: ${filePath}`);
      this.addCoprocessor(filePath, repository).catch((e) => {
        this.logger.error(`addCoprocessor failed with exception: ${e.message}`);
      });
    });
    this.activeDirWatcher = watch(this.activeDir).on("unlink", (filePath) => {
      this.logger.info(`Detected removed file from active dir: ${filePath}`);
      this.removeHandleFromFilePath(filePath, repository);
    });
  }

  /**
   * Lazily load the ManagementClient
   * Retires and sleeps 1s between configurable max number of attempts
   *
   * @param retries - Max number of connection attempts
   * @return Promise with management client on success or error on failure
   */
  private getClient(): Promise<ManagementClient> {
    if (!this.managementClient) {
      return ManagementClient.create(43118)
        .then((client) => {
          this.logger.info(
            "Succeeded in establishing a connection to redpanda"
          );
          this.managementClient = client;
          return client;
        })
        .catch((err) => {
          this.logger.warn(
            `Failed to connect to redpanda, retrying again, reason: ${err.message}`
          );
          return new Promise((resolve, reject) => {
            setTimeout(() => {
              this.getClient()
                .then((response) => resolve(response))
                .catch((error) => reject(error));
            }, 1000);
          });
        });
    }
    return Promise.resolve(this.managementClient);
  }
}

export default FileManager;
