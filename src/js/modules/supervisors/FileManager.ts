import * as Inotify from "inotifywait";
import { rename, readdir } from "fs";
import { promisify } from "util";
import Repository from "./Repository";
import { Handle } from "../domain/Handle";
import { getChecksumFromFile } from "../utilities/Checksum";
import { Coprocessor } from "../public/Coprocessor";
import { ManagementClient } from "../rpc/serverAndClients/server";
import * as path from "path";
import { hash64 } from "xxhash";

/**
 * FileManager class is an inotify implementation, it receives a
 * Repository and updates this object when to  add a new file in
 * submit directory and read previous files from the active directory when
 * this class is instanced
 */
class FileManager {
  constructor(
    private repository: Repository,
    private submitDir: string,
    private activeDir: string,
    private inactiveDir: string,
    public managementClient: ManagementClient
  ) {
    try {
      this.watcher = new Inotify(this.submitDir);
      this.readActiveCoprocessor(repository);
      this.updateRepositoryOnNewFile(repository);
    } catch (e) {
      console.error(e);
      //TODO: implement winston for loggin information and error handler
    }
  }

  /**
   * AddCoprocessor gets coprocessor from filePath and decides if the
   * coprocessor is moving between active and inactive directories
   * @param filePath, path of a coprocessor that we want to load and add to
   *        Repository
   * @param repository, coprocessor container
   * @param validatePrevCoprocessor, this flag is used for validation or not, if
   *              there is a coprocessor in Repository with the same
   *              global Id and different checksum, it will decide if id should
   *              update coprocessor or move the file to the inactive folder.
   */
  addCoprocessor(
    filePath: string,
    repository: Repository,
    validatePrevCoprocessor = true
  ): Promise<Handle> {
    return this.getHandle(filePath).then((handle) => {
      const preCoprocessor = repository.findByGlobalId(handle);
      if (preCoprocessor && validatePrevCoprocessor) {
        if (preCoprocessor.checksum === handle.checksum) {
          return this.moveCoprocessorFile(handle, this.inactiveDir);
        } else {
          return this.moveCoprocessorFile(preCoprocessor, this.inactiveDir)
            .then(() => repository.remove(preCoprocessor))
            .then(() => this.moveCoprocessorFile(handle, this.activeDir))
            .then((newCoprocessor) =>
              repository.add(newCoprocessor).then(() => newCoprocessor)
            );
        }
      } else {
        return this.moveCoprocessorFile(handle, this.activeDir)
          .then((newHandle) => {
            repository.add(newHandle);
            return newHandle;
          })
          .then((newHandle) =>
            this.enableTopic([newHandle.coprocessor]).then(() => newHandle)
          );
      }
    });
  }

  /**
   * reads the files in the "active" folder, loads them as Handles
   * and adds them to the given Repository
   * @param repository
   */
  readActiveCoprocessor(repository: Repository): void {
    const readdirPromise = promisify(readdir);
    readdirPromise(this.activeDir)
      .then((files) => {
        files.forEach((file) =>
          this.addCoprocessor(`${this.activeDir}/${file}`, repository, false)
        );
      })
      .catch(console.error);
    //TODO: implement winston for loggin information and error handler
  }

  /**
   * Updates the given Repository instance when a new coprocessor
   * file is added.
   * @param repository, is a coprocessor container
   */
  updateRepositoryOnNewFile(repository: Repository): void {
    return this.watcher.on("add", (filePath) => {
      this.addCoprocessor(filePath, repository).catch(console.error);
      //TODO: implement winston for logging information and error handler
    });
  }

  /**
   * allow closing the inotify process
   */
  close = (): Promise<void> => {
    return new Promise((resolve, reject) => {
      try {
        this.watcher.close();
        return resolve();
      } catch (e) {
        reject(e);
      }
    });
  };

  /**
   * Deregister the given Coprocessor and move the file where it's defined to
   * the 'inactive' directory.
   * @param coprocessor is a Coprocessor implementation.
   */
  deregisterCoprocessor(coprocessor: Coprocessor): Promise<Handle> {
    const handle = this.repository.findByCoprocessor(coprocessor);
    if (handle) {
      this.disableCoprocessors([handle.coprocessor])
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
   * Receives a coprocessor list, and sends a request to Redpanda for disabling
   * them. The response has the following structure:
   * [<topic status>]
   *
   * Possible coprocessor statuses:
   *   0 = success
   *   1 = topic never enabled
   *   2 = invalid coprocessor
   *   3 = materialized coprocessor
   *   4 = internal error
   * @param coprocessor
   * @param validateNeverEnabled
   */
  disableCoprocessors(
    coprocessor: Coprocessor[],
    validateNeverEnabled = true
  ): Promise<void> {
    return this.managementClient
      .disable_copros({ inputs: coprocessor.map((coproc) => coproc.globalId) })
      .then((disableResponse) => {
        const isValid = (condition: (n: number) => boolean) =>
          disableResponse.inputs.find(condition);
        const condition = validateNeverEnabled
          ? (coproc) => coproc > 0
          : (coproc) => coproc > 1;
        const invalidCoprocessor = isValid(condition);
        if (invalidCoprocessor > 0) {
          return Promise.reject(
            new Error(
              "Is not possible to disable coprocessors with ids: " +
                invalidCoprocessor
            )
          );
        } else {
          return Promise.resolve();
        }
      });
  }

  /**
   * Receives a coprocessor list, and sends a request to Redpanda for enabling
   * them. The response has the following structure:
   * [{<coprocessorId>, [<topic status>]}]
   *
   * Possible topic statuses:
   *   0 = success
   *   1 = topic already enabled
   *   2 = topic does not exist
   *   3 = invalid coprocessor
   *   4 = materialized coprocessor
   *   5 = internal error
   * @param coprocessors
   * @param validateAlreadyEnabled
   */
  enableTopic(
    coprocessors: Coprocessor[],
    validateAlreadyEnabled = true
  ): Promise<void> {
    if (coprocessors.length == 0) {
      return Promise.resolve();
    } else {
      return this.managementClient
        .enable_copros({
          coprocessors: coprocessors.map((coproc) => ({
            id: coproc.globalId,
            topics: coproc.inputTopics,
          })),
        })
        .then((enableResponse) => {
          const isValid = (condition: (n: number) => boolean) =>
            enableResponse.inputs.filter((coprocessorStatus) =>
              coprocessorStatus.response.find(condition)
            );
          const condition = validateAlreadyEnabled
            ? (coproc) => coproc > 0
            : (coproc) => coproc > 1;
          const invalidCoprocessor = isValid(condition);
          if (invalidCoprocessor.length > 0) {
            return Promise.reject(
              new Error(
                `Is not possible to enable coprocessors with ids:` +
                  invalidCoprocessor.join(", ")
              )
            );
          } else {
            return Promise.resolve();
          }
        });
    }
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

  private watcher: Inotify;
}

export default FileManager;
