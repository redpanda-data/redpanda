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
  * The ID of some schema in Redpanda Schema Registry
  */
export type SchemaID = number;

/**
 * The version of some schema in Redpanda Schema Registry
 */
export type SchemaVersion = number;

/**
 * The format of a schema
 */
export enum SchemaFormat {
    Avro = 0,
    Protobuf = 1,
    JSON = 2,
}

/**
 * Reference to a schema defined elsewhere.
 * The details are format dependent; e.g. Avro schemas will use a different
 * reference syntax from protobuf or JSON schemas. For more information, see:
 *
 *   https://docs.redpanda.com/current/manage/schema-reg/schema-reg-api/#reference-a-schema
 */
export interface Reference {
    /** The name for the referenced schema */
    name: string;
    /** The subject associated with the referenced schema */
    subject: string;
    /** The desired version of the referenced schema */
    version: SchemaVersion;
}

export interface Schema {
    /** The textual representation of the schema */
    readonly schema: string;
    /** The format of the schema */
    readonly format: SchemaFormat;
    /** (Possibly empty) list of references for the schema */
    readonly references: Reference[];
}

/**
 * Structured representation for a Schema along with its subject, version, and ID
 */
export interface SubjectSchema {
    /** The underlying schema object */
    readonly schema: Schema;
    /** The schema's subject */
    readonly subject: string;
    /** The schema's version */
    readonly version: SchemaVersion;
    /** The schema's ID */
    readonly id: SchemaID;
}

/**
 * Client Interface for interacting with Redpanda Schema Registry.
 *
 * Note that these only work within a Data Transform. Interaction with the
 * Redpanda broker is mediated by the Transforms SDK JavaScript runtime.
 *
 * @example
 * import { newClient, SchemaFormat } from "@redpanda-data/transform-sdk-sr";
 *
 * var srClient = newClient();
 * const schema = {
 *     type: "record",
 *     name: "Example",
 *     fields: [
 *         { "name": "a", "type": "long", "default": 0 },
 *         { "name": "b", "type": "string", "default": "" }
 *     ]
 * };
 * 
 * const subjSchema = srClient.createSchema(
 *     "avro-value",
 *     {
 *         schema: JSON.stringify(schema),
 *         format: SchemaFormat.Avro,
 *         references: [],
 *     }
 * );
 */
export interface SchemaRegistryClient {
    /** Look up a schema by its registry-global SchemaID */
    lookupSchemaById(id: SchemaID): Schema;
    /** Look up a schema by subject and specific version */
    lookupSchemaByVersion(subject: string, version: SchemaVersion): SubjectSchema;
    /** Look up the latest version of a schema (by subject) */
    lookupLatestSchema(subject: string): SubjectSchema;
    /** Create a schema under the given subject, returning the version and ID
     * If an equivalent schema already exists globally, that schema ID is
     * reused. If an equivalent schema already exists within that subject, this
     * has no effect and returns the existing schema.
     */
    createSchema(subject: string, schema: Schema): SubjectSchema;
}

/**
 * Construct a new SchemaRegistryClient instance.
 */
export function newClient(): SchemaRegistryClient;

/**
 * The result of a decodeSchemaID operation
 *
 * 'id' holds the decoded SchemaID
 * 'rest' holds the remainder of the input buffer after stripping the encoded ID off the front.
 */
export interface DecodeResult<BufferType> {
    readonly id: SchemaID;
    readonly rest: BufferType;
}

/**
 * Decode the schema ID from the header of some buffer.
 * Throws if 'buf' is of unrecognized type or if the decoding fails for
 * some reason. Note that `buf` must be at least 5 bytes long (the length of
 * the header format).
 *
 * @example
 * import { newClient, decodeSchemaID } from "@redpanda-data/transform-sdk-sr";
 *
 * srClient = newClient();
 * onRecordWritten((event: OnRecordWrittenEvent, writer: RecordWriter) => {
 *   const decoded = decodeSchemaID(event.record.value.array());
 *   console.warn(decoded.rest);
 *   var schema = srClient.lookupSchemaById(decoded.id);
 *   var buf = new Buffer(decoded.rest);
 *   // Application logic goes here!
 *   buf = encodeSchemaID(decoded.id, buf);
 * });
 * 
 */
export function decodeSchemaID(buf: string): DecodeResult<string>;
export function decodeSchemaID(buf: ArrayBuffer): DecodeResult<ArrayBuffer>;
export function decodeSchemaID(buf: Uint8Array): DecodeResult<Uint8Array>;

export function encodeSchemaID(id: SchemaID, buf: string): Uint8Array;
export function encodeSchemaID(id: SchemaID, buf: ArrayBuffer): Uint8Array;
export function encodeSchemaID(id: SchemaID, buf: Uint8Array): Uint8Array;
