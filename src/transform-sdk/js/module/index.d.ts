// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/**
 * RecordData is a wrapper around the underlying raw data
 * in a record.
 *
 * Similar to a JavaScript Response object.
 */
export interface RecordData {
  /**
   * Parse the data as JSON.
   *
   * This is a more efficent version of JSON.parse(text());
   *
   * @throws if the payload is not valid JSON
   * @returns the parsed JSON
   */
  json(): any;
  /**
   * Parse the data as a UTF-8 string.
   *
   * @throws if the payload is not valid UTF-8
   * @returns the parsed string
   */
  text(): string;
  /** Return the data as a raw byte array. */
  array(): Uint8Array;
}

/**
 * Records may have a collection of headers attached to them.
 *
 * Headers are opaque to the broker and are purely a mechanism for the
 * producer and consumers to pass information.
 */
export interface RecordHeader {
  /** The key of this header. */
  readonly key?: string | ArrayBuffer | Uint8Array | RecordData | null;
  /** The value of this header. */
  readonly value?: string | ArrayBuffer | Uint8Array | RecordData | null;
}

/**
 * A record within Redpanda.
 *
 * Records are generated as a result of any transforms that act upon
 * a WrittenRecord.
 */
export interface Record {
  /** The key for this record. */
  readonly key?: string | ArrayBuffer | Uint8Array | RecordData | null;
  /** The value for this record. */
  readonly value?: string | ArrayBuffer | Uint8Array | RecordData | null;
  /** The headers attached to this record. */
  readonly headers?: RecordHeader[];
}

/**
 * Records may have a collection of headers attached to them.
 *
 * Headers are opaque to the broker and are purely a mechanism for the
 * producer and consumers to pass information.
 *
 * It is similar to a RecordHeader, expect that it only contains
 * RecordData or null.
 */
export interface WrittenRecordHeader {
  /** The key for this header. */
  readonly key?: RecordData | null;
  /** The value for this header. */
  readonly value?: RecordData | null;
}

/**
 * A persisted record written to a topic within Redpanda.
 *
 * It is similar to a Record, expect that it only contains RecordData
 * or null.
 */
export interface WrittenRecord {
  /** The key for this record. */
  readonly key: RecordData | null;
  /** The value for this record. */
  readonly value: RecordData | null;
  /** The headers for this record. */
  readonly headers: RecordHeader[];
}

/** An event generated after a write event within the broker. */
export interface OnRecordWrittenEvent {
  /** The record that was written as part of this event. */
  readonly record: WrittenRecord;
}

/**
 * A writer for transformed records that are output to the destination
 * topic.
 */
export interface RecordWriter {
  /**
   * Write a record to the output topic.
   *
   * @throws if there are errors writing the record.
   */
  write(record: Record): void;
}

/** The callback type for OnRecordWritten. */
export type OnRecordWrittenCallback = (
  event: OnRecordWrittenEvent,
  writer: RecordWriter,
) => void;

/**
 * Register a callback to be fired when a record is written to the
 * input topic.
 *
 * This callback is triggered after the record has been written,
 * fsynced to disk and the producer acknowledged.
 *
 * This method should be called in your script's entrypoint.
 *
 * @example
 * import {onRecordWritten} from "@redpanda-data/transform-sdk";
 *
 * // Copy the input data to the output topic.
 * onRecordWritten((event, writer) => {
 *   writer.write(event.record);
 * });
 */
export function onRecordWritten(cb: OnRecordWrittenCallback): void;
