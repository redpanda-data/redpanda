/**
 * Copyright 2020 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

type WriteInt = (
  buf: Buffer,
  value: number | bigint,
  offset?: number
) => number;
type ReadInt = (buf: Buffer, offset?: number) => number | bigint;
/**
 * Fragment sizes is an array how demonstrate how fragments should be grown
 */
export const fragmentSizes = [
  512, 768, 1152, 1728, 2592, 3888, 5832, 8748, 13122, 19683, 29525, 44288,
  66432, 99648, 131072,
];

const getNextSize = (currentSize: number) => {
  const maxSize = fragmentSizes[fragmentSizes.length - 1];
  if (currentSize > maxSize) {
    return maxSize;
  } else {
    return fragmentSizes.find((size) => currentSize < size) || maxSize;
  }
};

export class IOBuf {
  private fragmentList: Fragment[] = [];
  private currentWriteIndex = 0;
  /**
   * It's a buffer used for integer operations which need to be applied to 2
   * distinct buffers
   * example:
   * For example, if an int32 needs to be written, but the current fragment only
   * has 2 free bytes available, then 2 bytes will be written to the current one
   * and the remaining 2 will be written in the next fragment.
   * Process:
   * Write the number into auxBuffer, and then copy it from there onto the other
   * 2 fragments
   * auxBuf's size is 8 because javascript support
   * until 64 bytes (8 B) operation.
   * @private
   */
  private auxBuf: Buffer = Buffer.alloc(8);

  private incrementCurrentIndex() {
    this.currentWriteIndex++;
  }

  private resetCurrentIndex() {
    this.currentWriteIndex = 0;
  }

  appendInt8(value: number): number {
    const writeBuffer: WriteInt = (buffer, value1, offset1) =>
      buffer.writeInt8(Number(value1), offset1);
    return this.writeStandardInt(value, writeBuffer, 1);
  }

  appendUInt8(value: number): number {
    const writeBuffer: WriteInt = (buffer, value1, offset1) =>
      buffer.writeUInt8(Number(value1), offset1);
    return this.writeStandardInt(value, writeBuffer, 1);
  }

  appendInt16LE(value: number): number {
    const writeBuffer: WriteInt = (buffer, value1, offset1) =>
      buffer.writeInt16LE(Number(value1), offset1);
    return this.writeStandardInt(value, writeBuffer, 2);
  }

  appendUInt16LE(value: number): number {
    const writeBuffer: WriteInt = (buffer, value1, offset1) =>
      buffer.writeUInt16LE(Number(value1), offset1);
    return this.writeStandardInt(value, writeBuffer, 2);
  }

  appendInt32LE(value: number): number {
    const writeBuffer: WriteInt = (buffer, value1, offset1) =>
      buffer.writeInt32LE(Number(value1), offset1);
    return this.writeStandardInt(value, writeBuffer, 4);
  }

  appendUInt32LE(value: number): number {
    const writeBuffer: WriteInt = (buffer, value1, offset1) =>
      buffer.writeUInt32LE(Number(value1), offset1);
    return this.writeStandardInt(value, writeBuffer, 4);
  }

  appendBigInt64LE(value: bigint): number {
    const writeBuffer: WriteInt = (buffer, value1, offset1) =>
      buffer.writeBigInt64LE(BigInt(value1), offset1);
    return this.writeStandardInt(value, writeBuffer, 8);
  }

  appendBigUInt64LE(value: bigint): number {
    const writeBuffer: WriteInt = (buffer, value1, offset1) =>
      buffer.writeBigUInt64LE(BigInt(value1), offset1);
    return this.writeStandardInt(value, writeBuffer, 8);
  }

  appendBuffer(value: Buffer): number {
    let fragment = this.getLastFragment();
    let bytesForWriting = value.length;
    while (bytesForWriting > 0) {
      const wroteBytes = fragment.append(
        value.slice(value.length - bytesForWriting, value.length)
      );
      bytesForWriting -= wroteBytes;
      if (bytesForWriting > 0) {
        fragment = this.getNextFragmentOrAppend(fragment);
      }
    }
    return value.length;
  }

  appendString(value: string): number {
    let fragment = this.getLastFragment();
    const stringSize = Buffer.byteLength(value);
    let bytesForWriting = stringSize;
    while (bytesForWriting !== 0) {
      const wroteBytes = fragment.buffer.write(value, fragment.used);
      value = value.slice(
        fragment.buffer.toString(
          undefined,
          fragment.used,
          fragment.used + wroteBytes
        ).length
      );
      fragment.used += wroteBytes;
      bytesForWriting -= wroteBytes;
      if (bytesForWriting !== 0) {
        fragment = this.getNextFragmentOrAppend(fragment);
      }
    }
    return stringSize;
  }

  length(): number {
    return this.fragmentList.length;
  }

  totalChainSize(): number {
    return this.fragmentList.reduce(
      (prev, fragment) => prev + fragment.used,
      0
    );
  }

  getFragmentByIndex(index: number): Fragment {
    return this.fragmentList[index];
  }

  forEach(fn: (value: Fragment, index: number) => void): void {
    this.fragmentList.forEach(fn);
  }

  /**
   * clean fragments and reset its state
   */
  clean(): void {
    this.fragmentList.forEach((fragment) => {
      fragment.buffer.fill(0);
      fragment.used = 0;
    });
    this.resetCurrentIndex();
  }

  /**
   * Returns an IOBuf which represents a space in the given IOBuf.
   * This is useful when it’s necessary to reserve space in an IOBuf beforehand.
   * @param size of the reserve space
   * example:
   *  [101001-----------------------] <- IOBuf
   *  [101001[------]---------------]
   *            ^^____________________ get reserve
   *  [101001[------]11000----------] <- IOBuf can continue writing without use
   *                                     reserve space
   *  [101001[10----]11000----------] <- writing in reserve
   */
  getReserve(size: number): IOBuf {
    let fragment = this.getLastFragment();
    const reserves: Buffer[] = [];
    while (size > 0) {
      const auxReserve = fragment.buffer.slice(
        fragment.used,
        fragment.used + size
      );
      size -= auxReserve.length;
      fragment.used += auxReserve.length;
      reserves.push(auxReserve);
      if (size > 0) {
        fragment = this.getNextFragmentOrAppend(fragment);
      }
    }
    return IOBuf.createFromBuffers(reserves);
  }

  getIterable(): FragmentIterator {
    return new FragmentIterator(this.fragmentList);
  }

  static createFromBuffers(buffers: Buffer[]): IOBuf {
    const iob = new IOBuf();
    iob.fragmentList = buffers.map(Fragment.fromBuffer);
    return iob;
  }

  /**
   * find the first fragment with free space, otherwise it returns the last
   * fragment in the list.
   */
  private getLastFragment = (): Fragment => {
    const lastFragment = this.fragmentList[this.currentWriteIndex];
    if (lastFragment) {
      return lastFragment;
    } else {
      return this.appendNewFragment(lastFragment?.getSize());
    }
  };

  /**
   * given a fragment, it tries to find the next fragment in the list, or
   * appends a new one.
   * @param fragment
   */
  private getNextFragmentOrAppend = (fragment: Fragment): Fragment => {
    const nextFragment = this.fragmentList[this.currentWriteIndex + 1];
    if (nextFragment) {
      return nextFragment;
    } else {
      return this.appendNewFragment(fragment.getSize());
    }
  };

  private appendNewFragment = (size?: number): Fragment => {
    const nextSize = size ? getNextSize(size) : fragmentSizes[0];
    const fragment = new Fragment(nextSize);
    const newIndex = this.fragmentList.push(fragment);
    // if newIndex is 1, it means this.fragment added the first element, in this
    // case, the currentIndex shouldn't increment because for 1 element the
    // index must be 0 not 1.
    if (newIndex !== 1) {
      this.incrementCurrentIndex();
    }
    return fragment;
  };

  /**
   * write a number in fragment or fragments
   * @param value the number that will be written
   * @param writeIntFn is the function that will be used to write the value into
   *        the fragment
   * @param byteLength is bytes length for the value
   * @private
   */
  private writeStandardInt(
    value: number | bigint,
    writeIntFn: WriteInt,
    byteLength: number
  ): number {
    let fragment = this.getLastFragment();
    writeIntFn(this.auxBuf, value);
    let bytesForWriting = byteLength;
    while (bytesForWriting > 0) {
      bytesForWriting -= fragment.append(
        this.auxBuf.slice(byteLength - bytesForWriting, byteLength)
      );
      if (bytesForWriting > 0) {
        fragment = this.getNextFragmentOrAppend(fragment);
      }
    }
    return byteLength;
  }
}

export class FragmentIterator {
  fragments: Fragment[] = [];
  currentFragmentIndex = 0;
  readOffset = 0;
  auxBuf: Buffer = Buffer.alloc(8);

  constructor(fragments: Fragment[]) {
    this.fragments = fragments;
  }

  getCurrent(): Fragment {
    return this.fragments[this.currentFragmentIndex];
  }

  size(): number {
    return this.fragments.length;
  }

  readInt8(): number {
    const readBuffer: ReadInt = (buf, offset1) => buf.readInt8(offset1);
    return Number(this.readStandardInt(readBuffer, 1));
  }

  readUInt8(): number {
    const readBuffer: ReadInt = (buf, offset1) => buf.readUInt8(offset1);
    return Number(this.readStandardInt(readBuffer, 1));
  }

  readInt16LE(): number {
    const readBuffer: ReadInt = (buf, offset1) => buf.readInt16LE(offset1);
    return Number(this.readStandardInt(readBuffer, 2));
  }

  readUInt16LE(): number {
    const readBuffer: ReadInt = (buf, offset1) => buf.readUInt16LE(offset1);
    return Number(this.readStandardInt(readBuffer, 2));
  }

  readInt32LE(): number {
    const readBuffer: ReadInt = (buf, offset1) => buf.readInt32LE(offset1);
    return Number(this.readStandardInt(readBuffer, 4));
  }

  readUInt32LE(): number {
    const readBuffer: ReadInt = (buf, offset1) => buf.readUInt32LE(offset1);
    return Number(this.readStandardInt(readBuffer, 4));
  }

  readBigInt64LE(): bigint {
    const readBuffer: ReadInt = (buf, offset1) => buf.readBigInt64LE(offset1);
    return BigInt(this.readStandardInt(readBuffer, 8));
  }

  readBigUInt64LE(): bigint {
    const readBuffer: ReadInt = (buf, offset1) => buf.readBigUInt64LE(offset1);
    return BigInt(this.readStandardInt(readBuffer, 8));
  }

  toString(bytesLength: number, encoding?: BufferEncoding): string {
    let fragment = this.getCurrent();
    let result = "";
    while (bytesLength > 0) {
      const auxString = fragment.buffer.toString(
        encoding,
        this.readOffset,
        this.readOffset + bytesLength
      );
      result += auxString;
      const readBytes = Buffer.byteLength(auxString);
      bytesLength -= readBytes;
      this.readOffset += readBytes;
      if (bytesLength > 0) {
        fragment = this.getNextFragment(this.currentFragmentIndex);
      }
    }
    return result;
  }

  slice(size: number): Buffer {
    let fragment = this.getCurrent();
    const result: Buffer[] = [];
    while (size > 0) {
      const auxBuffer = fragment.buffer.slice(
        this.readOffset,
        this.readOffset + size
      );
      result.push(auxBuffer);
      size -= auxBuffer.length;
      this.readOffset += auxBuffer.length;
      if (size > 0) {
        fragment = this.getNextFragment(this.currentFragmentIndex);
      }
    }
    return Buffer.concat(result);
  }

  private readStandardInt(
    readInt: ReadInt,
    byteLength: number
  ): number | bigint {
    const fragment = this.getCurrent();
    const availableBytes = fragment.getSize() - this.readOffset;
    if (availableBytes >= byteLength) {
      const result = readInt(fragment.buffer, this.readOffset);
      this.readOffset += byteLength;
      return result;
    } else {
      fragment.buffer.copy(this.auxBuf, 0, this.readOffset, fragment.getSize());
      const nextFragment = this.getNextFragment(this.currentFragmentIndex);
      nextFragment.buffer.copy(
        this.auxBuf,
        availableBytes,
        0,
        byteLength - availableBytes
      );
      this.readOffset += byteLength - availableBytes;
      return readInt(this.auxBuf);
    }
  }

  private getNextFragment(index: number) {
    const nextIndex = index + 1;
    const fragment = this.fragments[nextIndex];
    this.currentFragmentIndex = nextIndex;
    this.readOffset = 0;
    return fragment;
  }
}

class Fragment {
  buffer: Buffer;
  used = 0;

  constructor(private size: number) {
    this.buffer = Buffer.alloc(size);
  }

  static fromBuffer(buffer: Buffer): Fragment {
    const fragment = new Fragment(buffer.length);
    fragment.buffer = buffer;
    return fragment;
  }

  getSize() {
    return this.size;
  }

  private incrementUsedBytes(value: number) {
    this.used += value;
  }

  append(buffer: Buffer) {
    const wroteBytes = buffer.copy(this.buffer, this.used);
    this.incrementUsedBytes(wroteBytes);
    return wroteBytes;
  }
}
