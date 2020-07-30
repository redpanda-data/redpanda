import { CoprocessorHandle } from "../domain/CoprocessorManager";
import { Coprocessor } from "../public/Coprocessor";
import { HandleTable } from "./HandleTable";

/**
 * CoprocessorsRepository is a container for CoprocessorHandles.
 */
class CoprocessorRepository {
  constructor() {
    this.coprocessors = new Map();
  }

  /**
   * this method adds a new CoprocessorHandle to the repository
   * @param coprocessor
   */
  add(coprocessor: CoprocessorHandle): Promise<void> {
    const addHandle = () => {
      coprocessor.coprocessor.inputTopics.forEach((topic) => {
        const currentHandleTable = this.coprocessors.get(topic);
        if (currentHandleTable) {
          currentHandleTable.registerHandle(coprocessor);
        } else {
          this.coprocessors.set(topic, new HandleTable());
          this.coprocessors.get(topic).registerHandle(coprocessor);
        }
      });
    };

    if (this.findByGlobalId(coprocessor)) {
      return this.remove(coprocessor).then(addHandle);
    } else {
      return Promise.resolve(addHandle());
    }
  }

  /**
   *
   * findByGlobalId method receives a coprocessor and returns a
   * CoprocessorHandle if there exists one with the same global ID as the given
   * coprocessor. Returns undefined otherwise.
   * @param handle
   */
  findByGlobalId = (
    handle: CoprocessorHandle
  ): CoprocessorHandle | undefined => {
    for (const [, tableHandle] of this.coprocessors) {
      const existingHandle = tableHandle.findHandleById(handle);
      if (existingHandle) {
        return existingHandle;
      }
    }
  };

  /**
   * Given a Coprocessor, try to find one with the same global ID and return it
   * if it exists, returns undefined otherwise
   * @param coprocessor
   */
  findByCoprocessor = (
    coprocessor: Coprocessor
  ): CoprocessorHandle | undefined => {
    for (const [, tableHandle] of this.coprocessors) {
      const existingHandle = tableHandle.findHandleByCoprocessor(coprocessor);
      if (existingHandle) {
        return existingHandle;
      }
    }
  };

  /**
   * removeCoprocessor method remove a coprocessor from the coprocessor map
   * @param handle
   */
  remove = (handle: CoprocessorHandle): Promise<void> => {
    return new Promise((resolve, reject) => {
      try {
        for (const [, handleTable] of this.coprocessors) {
          handleTable.deregisterHandle(handle);
        }
        resolve();
      } catch (e) {
        reject(
          new Error(
            "Error removing coprocessor with ID " +
              `${handle.coprocessor.globalId}: ${e.message}`
          )
        );
      }
    });
  };
  /**
   * getCoprocessors returns the map of CoprocessorHandles indexed by their
   * topics
   */
  getCoprocessorsByTopics(): Map<string, HandleTable> {
    return this.coprocessors;
  }

  private readonly coprocessors: Map<string, HandleTable>;
}

export default CoprocessorRepository;
