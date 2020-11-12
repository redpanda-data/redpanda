/**
 * Copyright 2020 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

// Code generated
// import Buffer Functions
import BF from "./functions";

export class Class2 {
  public numberSigned8: number;
  public numberSigned16: number;
  public numberSigned32: number;

  /**
   * transform bytes into a buffer to Class2
   * @param buffer is the place where the binary data is stored
   * @param offset is the position where the function will start to
   *        read into buffer
   * @return a tuple, where first element is a Class2 and second
   *        one is the read last position in the buffer
   */
  static fromBytes(buffer: Buffer, offset = 0): [Class2, number] {
    return [
      {
        numberSigned8: (() => {
          const [value, newOffset] = BF.readInt8LE(buffer, offset);
          offset = newOffset;
          return value;
        })(),
        numberSigned16: (() => {
          const [value, newOffset] = BF.readInt16LE(buffer, offset);
          offset = newOffset;
          return value;
        })(),
        numberSigned32: (() => {
          const [value, newOffset] = BF.readInt32LE(buffer, offset);
          offset = newOffset;
          return value;
        })(),
      },
      offset,
    ];
  }
  /**
   * transform from Class2 to binary version with Redpanda
   * standard
   * @param value is a Class2 instance
   * @param buffer is the binary array where the Class2 binary
   *        will save
   * @param offset is the position where the toBytes function starts to write
   *        in the buffer
   * @return the last position of the offset
   */
  static toBytes(value: Class2, buffer: Buffer, offset = 0): number {
    offset = BF.writeInt8LE(value.numberSigned8, buffer, offset);
    offset = BF.writeInt16LE(value.numberSigned16, buffer, offset);
    offset = BF.writeInt32LE(value.numberSigned32, buffer, offset);
    return offset;
  }
}
export class Class3 {
  public numberUSigned8: number;
  public numberUSigned16: number;
  public numberUSigned32: number;

  /**
   * transform bytes into a buffer to Class3
   * @param buffer is the place where the binary data is stored
   * @param offset is the position where the function will start to
   *        read into buffer
   * @return a tuple, where first element is a Class3 and second
   *        one is the read last position in the buffer
   */
  static fromBytes(buffer: Buffer, offset = 0): [Class3, number] {
    return [
      {
        numberUSigned8: (() => {
          const [value, newOffset] = BF.readUInt8LE(buffer, offset);
          offset = newOffset;
          return value;
        })(),
        numberUSigned16: (() => {
          const [value, newOffset] = BF.readUInt16LE(buffer, offset);
          offset = newOffset;
          return value;
        })(),
        numberUSigned32: (() => {
          const [value, newOffset] = BF.readUInt32LE(buffer, offset);
          offset = newOffset;
          return value;
        })(),
      },
      offset,
    ];
  }
  /**
   * transform from Class3 to binary version with Redpanda
   * standard
   * @param value is a Class3 instance
   * @param buffer is the binary array where the Class3 binary
   *        will save
   * @param offset is the position where the toBytes function starts to write
   *        in the buffer
   * @return the last position of the offset
   */
  static toBytes(value: Class3, buffer: Buffer, offset = 0): number {
    offset = BF.writeUInt8LE(value.numberUSigned8, buffer, offset);
    offset = BF.writeUInt16LE(value.numberUSigned16, buffer, offset);
    offset = BF.writeUInt32LE(value.numberUSigned32, buffer, offset);
    return offset;
  }
}
export class Class1 {
  public stringValue: string;
  public booleanValue: boolean;
  public bigint: bigint;
  public varintValue: bigint;
  public bufferValue: Buffer;
  public arrayValue: Array<string>;
  public classSigned: Class2;
  public classUSigned: Class3;

  /**
   * transform bytes into a buffer to Class1
   * @param buffer is the place where the binary data is stored
   * @param offset is the position where the function will start to
   *        read into buffer
   * @return a tuple, where first element is a Class1 and second
   *        one is the read last position in the buffer
   */
  static fromBytes(buffer: Buffer, offset = 0): [Class1, number] {
    return [
      {
        stringValue: (() => {
          const [value, newOffset] = BF.readString(buffer, offset);
          offset = newOffset;
          return value;
        })(),
        booleanValue: (() => {
          const [value, newOffset] = BF.readBoolean(buffer, offset);
          offset = newOffset;
          return value;
        })(),
        bigint: (() => {
          const [value, newOffset] = BF.readInt64LE(buffer, offset);
          offset = newOffset;
          return value;
        })(),
        varintValue: (() => {
          const [value, newOffset] = BF.readVarint(buffer, offset);
          offset = newOffset;
          return value;
        })(),
        bufferValue: (() => {
          const [value, newOffset] = BF.readBuffer(buffer, offset);
          offset = newOffset;
          return value;
        })(),
        arrayValue: (() => {
          const [array, newOffset] = BF.readArray(
            buffer,
            offset,
            (auxBuffer, auxOffset) => BF.readString(auxBuffer, auxOffset)
          );
          offset = newOffset;
          return array;
        })(),
        classSigned: (() => {
          const [value, newOffset] = BF.readObject(buffer, offset, Class2);
          offset = newOffset;
          return value;
        })(),
        classUSigned: (() => {
          const [value, newOffset] = BF.readObject(buffer, offset, Class3);
          offset = newOffset;
          return value;
        })(),
      },
      offset,
    ];
  }
  /**
   * transform from Class1 to binary version with Redpanda
   * standard
   * @param value is a Class1 instance
   * @param buffer is the binary array where the Class1 binary
   *        will save
   * @param offset is the position where the toBytes function starts to write
   *        in the buffer
   * @return the last position of the offset
   */
  static toBytes(value: Class1, buffer: Buffer, offset = 0): number {
    offset = BF.writeString(value.stringValue, buffer, offset);
    offset = BF.writeBoolean(value.booleanValue, buffer, offset);
    offset = BF.writeInt64LE(value.bigint, buffer, offset);
    offset = BF.writeVarint(value.varintValue, buffer, offset);
    offset = BF.writeBuffer(value.bufferValue, buffer, offset);

    offset = BF.writeArray(
      value.arrayValue,
      buffer,
      offset,
      (item, auxBuffer, auxOffset) => BF.writeString(item, auxBuffer, auxOffset)
    );
    offset = BF.writeObject(buffer, offset, Class2, value.classSigned);
    offset = BF.writeObject(buffer, offset, Class3, value.classUSigned);
    return offset;
  }
}
