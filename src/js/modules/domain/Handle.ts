import { Coprocessor } from "../public/Coprocessor";

/**
 * Handle interface represents a coprocessor loaded into memory
 * @coprocessor: is a Coprocessor implementation instance
 * @filename: the path to the Javascript file where the coprocessor is defined
 * @checksum: is the the file's content checksum
 */
export interface Handle {
  coprocessor: Coprocessor;
  filename: string;
  checksum: string;
}
