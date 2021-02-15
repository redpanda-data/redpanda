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
import { rename, readdir, unlink } from "fs";
import { promisify } from "util";
import Repository from "./Repository";
import { Handle } from "../domain/Handle";
import LogService from "../utilities/Logging";
import { getChecksumFromFile } from "../utilities/Checksum";
import { Coprocessor } from "../public/Coprocessor";
import * as path from "path";
import { hash64 } from "xxhash";
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
              Promise.resolve()
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
            Promise.resolve()
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
      /**
       * this case is possible when a coprocessor is disabled either by
       * 'rpk wasm disable' or by the error policy, so the file is moved
       * from the active folder to the inactive folder.
       */
      return Promise.resolve();
    } else {
      this.repository.remove(handle);
      return Promise.resolve();
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
      return Promise.resolve()
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
      } catch (fileErrors) {
        const name = path.basename(filename);
        this.moveFile(filename, path.join(this.inactiveDir, name)).then(() =>
          this.logger.warn(
            `Delete wasm definition file, ${filename}, error: ${fileErrors}`
          )
        );
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
    return this.moveFile(coprocessor.filename, destinationPath).then(() => ({
      ...coprocessor,
      filename: destinationPath,
    }));
  }

  private moveFile(origin: string, destination: string): Promise<void> {
    const renamePromise = promisify(rename);
    return renamePromise(origin, destination);
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
    this.submitDirWatcher = watch(this.submitDir, {
      awaitWriteFinish: true,
    }).on("add", (filePath) => {
      this.logger.info(`Detected new file in submit dir: ${filePath}`);
      this.addCoprocessor(filePath, repository).catch((e) => {
        this.logger.error(`addCoprocessor failed with exception: ${e.message}`);
      });
    });
    this.activeDirWatcher = watch(this.activeDir, {
      awaitWriteFinish: true,
    }).on("unlink", (filePath) => {
      this.logger.info(`Detected removed file from active dir: ${filePath}`);
      this.removeHandleFromFilePath(filePath, repository);
    });
  }
}

export default FileManager;
