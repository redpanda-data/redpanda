import * as Inotify from "inotifywait";
import { rename, readdir } from "fs";
import { promisify } from "util";
import CoprocessorRepository from "./CoprocessorRepository";
import { CoprocessorHandle } from "../domain/CoprocessorManager";
import { getChecksumFromFile } from "../utilities/Checksum";
import { Coprocessor } from "../public/Coprocessor";

/**
 * CoprocessorFileManager class is an inotify implementation, it receives a
 * CoprocessorRepository and updates this object when to  add a new file in submit directory
 * and read previous files from the active directory when this class is instanced
 */
class CoprocessorFileManager {
  constructor(
    private coprocessorRepository: CoprocessorRepository,
    private submitDir: string,
    private activeDir: string,
    private inactiveDir: string
  ) {
    try {
      this.watcher = new Inotify(this.submitDir);
      this.readActiveCoprocessor(coprocessorRepository);
      this.updateRepositoryOnNewFile(coprocessorRepository);
    } catch (e) {
      console.error(e);
      //TODO: implement winston for loggin information and error handler
    }
  }

  /**
   * AddCoprocessor gets coprocessor from filePath and decides if the coprocessor is
   * moving between active and inactive directories
   * @param filePath, path of a coprocessor that we want to load and add to CoprocessorRepository
   * @param coprocessorRepository, coprocessor container
   * @param validatePrevCoprocessor, this flag is used for validation or not, if there is a
   *              coprocessor in CoprocessorRepository with the same global Id and
   *              different checksum, it will decide if id should update coprocessor or move the
   *              file to the inactive folder.
   */
  addCoprocessor = (
    filePath: string,
    coprocessorRepository: CoprocessorRepository,
    validatePrevCoprocessor: boolean = true
  ): Promise<CoprocessorHandle> =>
    this.getCoprocessor(filePath).then((coprocessor) => {
      const preCoprocessor = coprocessorRepository.findByGlobalId(coprocessor);
      if (preCoprocessor && validatePrevCoprocessor) {
        if (preCoprocessor.checksum === coprocessor.checksum) {
          return this.moveCoprocessorFile(coprocessor, this.inactiveDir);
        } else {
          return this.moveCoprocessorFile(preCoprocessor, this.inactiveDir)
            .then(() => coprocessorRepository.remove(preCoprocessor))
            .then(() => this.moveCoprocessorFile(coprocessor, this.activeDir))
            .then((newCoprocessor) => {
              coprocessorRepository.add(newCoprocessor);
              return newCoprocessor;
            });
        }
      } else {
        return this.moveCoprocessorFile(coprocessor, this.activeDir).then(
          (newCoprocessor) => {
            coprocessorRepository.add(newCoprocessor);
            return newCoprocessor;
          }
        );
      }
    });

  /**
   * reads the files in the "active" folder, loads them as CoprocessorHandles and
   * adds them to the given CoprocessorRepository
   * @param coprocessorRepository
   */
  readActiveCoprocessor(coprocessorRepository: CoprocessorRepository): void {
    const readdirPromise = promisify(readdir);
    readdirPromise(this.activeDir)
      .then((files) => {
        files.forEach((file) =>
          this.addCoprocessor(
            `${this.activeDir}/${file}`,
            coprocessorRepository,
            false
          )
        );
      })
      .catch(console.error);
    //TODO: implement winston for loggin information and error handler
  }

  /**
   * Updates the given CoprocessorRepository instance when a new coprocessor
   * file is added.
   * @param coprocessorRepository, is a coprocessor container
   */
  updateRepositoryOnNewFile = (
    coprocessorRepository: CoprocessorRepository
  ) => {
    this.watcher.on("add", (filePath) => {
      this.addCoprocessor(filePath, coprocessorRepository).catch(console.error);
      //TODO: implement winston for logging information and error handler
    });
  };

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
   * Deregister the given Coprocessor and move the file where it's defined to the
   * 'inactive' directory.
   * @param coprocessor is a Coprocessor implementation.
   */
  deregisterCoprocessor(coprocessor: Coprocessor): Promise<CoprocessorHandle> {
    const coprocessorHandle = this.coprocessorRepository.findByCoprocessor(
      coprocessor
    );
    if (coprocessorHandle) {
      return this.moveCoprocessorFile(coprocessorHandle, this.inactiveDir).then(
        (coprocessor) => {
          this.coprocessorRepository.remove(coprocessor);
          return coprocessor;
        }
      );
    } else {
      return Promise.reject(
        new Error(
          `The given coprocessor with ID ${coprocessor.globalId} hasn't been loaded`
        )
      );
    }
  }

  /**
   * receives a path and it gets the js file and create a checksum for content path file.
   * @param filename, path of the file that we need to get coprocessor information
   */
  private getCoprocessor = (filename: string): Promise<CoprocessorHandle> => {
    return new Promise<CoprocessorHandle>((resolve, reject) => {
      try {
        const script = require(filename);
        delete require.cache[filename];
        const fileChecksum = getChecksumFromFile(filename);
        fileChecksum
          .then((checksum) =>
            resolve({
              coprocessor: new script.default(),
              checksum,
              filename,
            })
          )
          .catch(reject);
      } catch (e) {
        reject(e);
      }
    });
  };

  /**
   * moveCoprocessorFile moves coprocessor from the current filepath to destination path, also changes the
   * name of the file for its checksum value and adds its SHA hash as a prefix.
   * @param coprocessor, is a coprocessor loaded in memory
   * @param destination, destination path
   */
  private moveCoprocessorFile = (
    coprocessor: CoprocessorHandle,
    destination: string
  ): Promise<CoprocessorHandle> => {
    const renamePromise = promisify(rename);
    const newFileName = `${destination}/sha265-${coprocessor.checksum}.js`;
    return renamePromise(coprocessor.filename, newFileName).then((_) => ({
      ...coprocessor,
      filename: newFileName,
    }));
  };

  private watcher: Inotify;
}

export default CoprocessorFileManager;
