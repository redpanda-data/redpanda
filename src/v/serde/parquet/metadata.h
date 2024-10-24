/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

// Parquet metadata is serialized in the Apache thrift compact wire format.
//
// The definition of these structs can be found here:
// https://github.com/apache/parquet-format/blob/db687874e/src/main/thrift/parquet.thrift
//
// And the compare wire format encoding spec is here:
// https://github.com/apache/thrift/blob/8922f17d59/doc/specs/thrift-compact-protocol.md
//
// Both are good reading materials to understand this code better.
//
// Note that our translated file metadata only supports a subset of the fields
// we actually use. It should easily extended as we need add functionality and
// need to write new metadata.

#include "base/seastarx.h"
#include "bytes/iobuf.h"
#include "container/fragmented_vector.h"
#include "serde/parquet/flattened_schema.h"
#include "serde/parquet/schema.h"

#include <seastar/core/sstring.hh>

#include <absl/container/flat_hash_map.h>

#include <cmath>
#include <cstdint>
#include <variant>

namespace serde::parquet {

/**
 * Encodings supported by Parquet.  Not all encodings are valid for all types.
 * These enums are also used to specify the encoding of definition and
 * repetition levels. See the accompanying doc for the details of the more
 * complicated encodings.
 */
enum class encoding : uint8_t {
    /** Default encoding.
     * BOOLEAN - 1 bit per value. 0 is false; 1 is true.
     * INT32 - 4 bytes per value.  Stored as little-endian.
     * INT64 - 8 bytes per value.  Stored as little-endian.
     * FLOAT - 4 bytes per value.  IEEE. Stored as little-endian.
     * DOUBLE - 8 bytes per value.  IEEE. Stored as little-endian.
     * BYTE_ARRAY - 4 byte length stored as little endian, followed by bytes.
     * FIXED_LEN_BYTE_ARRAY - Just the bytes.
     */
    plain = 0,

    /** Group VarInt encoding for INT32/INT64.
     * This encoding is deprecated. It was never used
     */
    //  GROUP_VAR_INT = 1,

    /**
     * Deprecated: Dictionary encoding. The values in the dictionary are encoded
     * in the plain type. in a data page use RLE_DICTIONARY instead. in a
     * Dictionary page use PLAIN instead
     */
    plain_dictionary = 2,

    /** Group packed run length encoding. Usable for definition/repetition
     * levels encoding and Booleans (on one bit: 0 is false, 1 is true.)
     */
    rle = 3,

    /** Bit packed encoding.  This can only be used if the data has a known max
     * width.  Usable for definition/repetition levels encoding.
     */
    bit_packed = 4,

    /** Delta encoding for integers. This can be used for int columns and works
     * best on sorted data
     */
    delta_binary_packed = 5,

    /** Encoding for byte arrays to separate the length values and the data. The
     * lengths are encoded using DELTA_BINARY_PACKED
     */
    delta_length_byte_array = 6,

    /** Incremental-encoded byte array. Prefix lengths are encoded using
     * DELTA_BINARY_PACKED. Suffixes are stored as delta length byte arrays.
     */
    delta_byte_array = 7,

    /** Dictionary encoding: the ids are encoded using the RLE encoding
     */
    rle_dictionary = 8,

    /** Encoding for fixed-width data (FLOAT, DOUBLE, INT32, INT64,
       FIXED_LEN_BYTE_ARRAY). K byte-streams are created where K is the size in
       bytes of the data type. The individual bytes of a value are scattered to
       the corresponding stream and the streams are concatenated. This itself
       does not reduce the size of the data but can lead to better compression
        afterwards.

        Added in 2.8 for FLOAT and DOUBLE.
        Support for INT32, INT64 and FIXED_LEN_BYTE_ARRAY added in 2.11.
     */
    byte_stream_split = 9,
};

/**
 * Supported compression algorithms.
 *
 * Codecs added in format version X.Y can be read by readers based on X.Y and
 * later. Codec support may vary between readers based on the format version and
 * libraries available at runtime.
 *
 * See Compression.md for a detailed specification of these algorithms.
 */
enum class compression_codec : uint8_t {
    UNCOMPRESSED = 0,
    SNAPPY = 1,
    GZIP = 2,
    LZO = 3,
    BROTLI = 4,  // Added in 2.4
    LZ4 = 5,     // DEPRECATED (Added in 2.4)
    ZSTD = 6,    // Added in 2.4
    LZ4_RAW = 7, // Added in 2.9
};

/**
 * Enum to annotate whether lists of min/max elements inside ColumnIndex
 * are ordered and if so, in which direction.
 */
enum class boundary_order {
    unordered = 0,
    ascending = 1,
    descending = 2,
};

struct index_page_header {};

/**
 * The dictionary page must be placed at the first position of the column chunk
 * if it is partly or completely dictionary encoded. At most one dictionary page
 * can be placed in a column chunk.
 **/
struct dictionary_page_header {
    /** Number of values in the dictionary **/
    int32_t num_values;

    /** Encoding using this dictionary page **/
    encoding data_encoding;

    /** If true, the entries in the dictionary are sorted in ascending order **/
    bool is_sorted;
};

/**
 * New page format allowing reading levels without decompressing the data
 * Repetition and definition levels are uncompressed
 * The remaining section containing the data is compressed if is_compressed is
 * true
 */
struct data_page_header {
    /** Number of values, including NULLs, in this data page. **/
    int32_t num_values;
    /** Number of NULL values, in this data page.
        Number of non-null = num_values - num_nulls which is also the number
       of values in the data section **/
    int32_t num_nulls;
    /**
     * Number of rows in this data page. Every page must begin at a
     * row boundary (repetition_level = 0): rows must **not** be
     * split across page boundaries when using V2 data pages.
     **/
    int32_t num_rows;
    /** Encoding used for data in this page **/
    encoding data_encoding;

    // repetition levels and definition levels are always using RLE (without
    // size in it)

    /** Length of the definition levels */
    int32_t definition_levels_byte_length;
    /** Length of the repetition levels */
    int32_t repetition_levels_byte_length;

    /**  Whether the values are compressed.
    Which means the section of the page between
    definition_levels_byte_length + repetition_levels_byte_length + 1 and
    compressed_page_size (included) is compressed with the compression_codec. If
    missing it is considered compressed */
    bool is_compressed;
};

struct page_header {
    /** Uncompressed page size in bytes (not including this header) **/
    int32_t uncompressed_page_size;

    /** Compressed (and potentially encrypted) page size in bytes, not including
     * this header **/
    int32_t compressed_page_size;

    /** The 32-bit CRC checksum for the page, to be be calculated as follows:
     *
     * - The standard CRC32 algorithm is used (with polynomial 0x04C11DB7,
     *   the same as in e.g. GZip).
     * - All page types can have a CRC (v1 and v2 data pages, dictionary pages,
     *   etc.).
     * - The CRC is computed on the serialization binary representation of the
     * page (as written to disk), excluding the page header. For example, for v1
     *   data pages, the CRC is computed on the concatenation of repetition
     * levels, definition levels and column values (optionally compressed,
     * optionally encrypted).
     * - The CRC computation therefore takes place after any compression
     *   and encryption steps, if any.
     *
     * If enabled, this allows for disabling checksumming in HDFS if only a few
     * pages need to be read.
     */
    int32_t crc;

    // Headers for page specific data.  One only will be set.
    std::variant<index_page_header, dictionary_page_header, data_page_header>
      type;
};

/**
 * Encode the page header into binary form using the Apache Thift compact wire
 * format.
 */
iobuf encode(const page_header& header);

/**
 * Description for column metadata
 */
struct column_meta_data {
    /** Type of this column **/
    physical_type type;

    /** Set of all encodings used for this column. The purpose is to validate
     * whether we can decode those pages. **/
    std::vector<encoding> encodings;

    /** Path in schema **/
    std::vector<ss::sstring> path_in_schema;

    /** Compression codec **/
    compression_codec codec;

    /** Number of values in this column **/
    int64_t num_values;

    /** total byte size of all uncompressed pages in this column chunk
     * (including the headers) **/
    int64_t total_uncompressed_size;

    /** total byte size of all compressed, and potentially encrypted, pages
     *  in this column chunk (including the headers) **/
    int64_t total_compressed_size;

    /** Optional key/value metadata **/
    std::vector<std::pair<ss::sstring, ss::sstring>> key_value_metadata;

    /** Byte offset from beginning of file to first data page **/
    int64_t data_page_offset;

    /** Byte offset from beginning of file to root index page **/
    int64_t index_page_offset;

    /** Byte offset from the beginning of file to first (only) dictionary
       page **/
    int64_t dictionary_page_offset;
};

struct column_chunk {
    /** File where column data is stored.  If not set, assumed to be same file
     *as metadata.  This path is relative to the current file.
     **/
    std::optional<ss::sstring> file_path;

    /** Column metadata for this chunk. Some writers may also replicate this at
     *the location pointed to by file_path/file_offset. Note: while marked as
     *optional, this field is in fact required by most major Parquet
     *implementations. As such, writers MUST populate this field.
     **/
    column_meta_data meta_data;

    /** File offset of ColumnChunk's OffsetIndex **/
    int64_t offset_index_offset = 0;

    /** Size of ColumnChunk's OffsetIndex, in bytes **/
    int32_t offset_index_length = 0;

    /** File offset of ColumnChunk's ColumnIndex **/
    int64_t column_index_offset = 0;

    /** Size of ColumnChunk's ColumnIndex, in bytes **/
    int32_t column_index_length = 0;
};

/**
 * Sort order within a RowGroup of a leaf column
 */
struct sorting_column {
    /** The ordinal position of the column (in this row group) **/
    int32_t column_idx;

    /** If true, indicates this column is sorted in descending order. **/
    bool descending;

    /** If true, nulls will come before non-null values, otherwise,
     * nulls go at the end. */
    bool nulls_first;
};

struct row_group {
    /** Metadata for each column chunk in this row group.
     * This list must have the same order as the SchemaElement list in
     * FileMetaData.
     **/
    chunked_vector<column_chunk> columns;

    /** Total byte size of all the uncompressed column data in this row group
     * **/
    int64_t total_byte_size;

    /** Number of rows in this row group **/
    int64_t num_rows;

    /** If set, specifies a sort ordering of the rows in this RowGroup.
     * The sorting columns can be a subset of all the columns.
     */
    std::vector<sorting_column> sorting_columns;

    /** Byte offset from beginning of file to first page (data or dictionary)
     * in this row group **/
    int64_t file_offset;
};

/**
 * Description for file metadata
 */
struct file_metadata {
    /** Version of this file */
    int32_t version;
    /**
     * Parquet schema for this file.
     */
    chunked_vector<flattened_schema> schema;

    /** Number of rows in this file **/
    int64_t num_rows;

    /** Row groups in this file */
    chunked_vector<row_group> row_groups;

    /** Optional key/value metadata **/
    std::vector<std::pair<ss::sstring, ss::sstring>> key_value_metadata;

    /** String for application that wrote this file.  This should be in the
     * format <Application> version <App Version> (build <App Build Hash>). e.g.
     * impala version 1.0 (build 6cf94d29b2b7115df4de2c06e2ab4326d721eb55)
     */
    ss::sstring created_by;
};

/**
 * Encode the file metadata into binary form using the Apache Thift compact wire
 * format.
 */
iobuf encode(const file_metadata& metadata);

} // namespace serde::parquet
