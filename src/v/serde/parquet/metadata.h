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
#include "hashing/crc32.h"
#include "serde/parquet/flattened_schema.h"
#include "serde/parquet/schema.h"

#include <seastar/core/sstring.hh>

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
    uncompressed = 0,
    snappy = 1,
    gzip = 2,
    lzo = 3,
    brotli = 4,  // added in 2.4
    lz4 = 5,     // deprecated (added in 2.4)
    zstd = 6,    // added in 2.4
    lz4_raw = 7, // added in 2.9
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

/**
 * Union to specify the order used for the min_value and max_value fields for a
 * column. This union takes the role of an enhanced enum that allows rich
 * elements (which will be needed for a collation-based ordering in the future).
 *
 * Possible values are:
 * * TypeDefinedOrder - the column uses the order defined by its logical or
 *                      physical type (if there is no logical type).
 *
 * If the reader does not support the value of this union, min and max stats
 * for this column should be ignored.
 */
enum class column_order {
    /**
     * The sort orders for logical types are:
     *   UTF8 - unsigned byte-wise comparison
     *   INT8 - signed comparison
     *   INT16 - signed comparison
     *   INT32 - signed comparison
     *   INT64 - signed comparison
     *   UINT8 - unsigned comparison
     *   UINT16 - unsigned comparison
     *   UINT32 - unsigned comparison
     *   UINT64 - unsigned comparison
     *   DECIMAL - signed comparison of the represented value
     *   DATE - signed comparison
     *   TIME_MILLIS - signed comparison
     *   TIME_MICROS - signed comparison
     *   TIMESTAMP_MILLIS - signed comparison
     *   TIMESTAMP_MICROS - signed comparison
     *   INTERVAL - undefined
     *   JSON - unsigned byte-wise comparison
     *   BSON - unsigned byte-wise comparison
     *   ENUM - unsigned byte-wise comparison
     *   LIST - undefined
     *   MAP - undefined
     *   VARIANT - undefined
     *
     * In the absence of logical types, the sort order is determined by the
     * physical type:
     *   BOOLEAN - false, true
     *   INT32 - signed comparison
     *   INT64 - signed comparison
     *   INT96 (only used for legacy timestamps) - undefined
     *   FLOAT - signed comparison of the represented value (*)
     *   DOUBLE - signed comparison of the represented value (*)
     *   BYTE_ARRAY - unsigned byte-wise comparison
     *   FIXED_LEN_BYTE_ARRAY - unsigned byte-wise comparison
     *
     * (*) Because the sorting order is not specified properly for floating
     *     point values (relations vs. total ordering) the following
     *     compatibility rules should be applied when reading statistics:
     *     - If the min is a NaN, it should be ignored.
     *     - If the max is a NaN, it should be ignored.
     *     - If the min is +0, the row group may contain -0 values as well.
     *     - If the max is -0, the row group may contain +0 values as well.
     *     - When looking for NaN values, min and max should be ignored.
     *
     *     When writing statistics the following rules should be followed:
     *     - NaNs should not be written to min or max statistics fields.
     *     - If the computed max value is zero (whether negative or positive),
     *       `+0.0` should be written into the max statistics field.
     *     - If the computed min value is zero (whether negative or positive),
     *       `-0.0` should be written into the min statistics field.
     */
    type_defined = 1,
};

/**
 * Statistics per row group and per page
 * All fields are optional.
 */
struct statistics {
    // A bound on the range of values stored in this column_chunk/page.
    struct bound {
        // The value of the column determined by ColumnOrder.
        //
        // These may be the actual minimum and maximum values found on a page or
        // column chunk, but can also be (more compact) values that do not exist
        // on a page or column chunk. For example, instead of storing "Blart
        // Versenwald III", a writer may set min_value="B", max_value="C". Such
        // more compact values must still be valid values within the column's
        // logical type.
        //
        // Values are encoded using PLAIN encoding, except that variable-length
        // byte arrays do not include a length prefix.
        iobuf value;
        // If the value referenced above is the actual value
        bool is_exact = false;
    };

    /**
     * Count of null values in the column.
     *
     * Writers SHOULD always write this field even if it is zero (i.e. no null
     * value) or the column is not nullable. Readers MUST distinguish between
     * null_count not being present and null_count == 0. If null_count is not
     * present, readers MUST NOT assume null_count == 0.
     */
    std::optional<int64_t> null_count = 0;
    std::optional<bound> max;
    std::optional<bound> min;
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

    /** Optional statistics for the data in this page **/
    std::optional<statistics> stats;
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
    crc::crc32 crc;

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
    chunked_vector<ss::sstring> path_in_schema;

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
    chunked_vector<std::pair<ss::sstring, ss::sstring>> key_value_metadata;

    /** Byte offset from beginning of file to first data page **/
    int64_t data_page_offset;

    /** Byte offset from beginning of file to root index page **/
    std::optional<int64_t> index_page_offset;

    /** Byte offset from the beginning of file to first (only) dictionary
       page **/
    std::optional<int64_t> dictionary_page_offset;

    /** optional statistics for this column chunk */
    std::optional<statistics> stats;
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
    chunked_vector<sorting_column> sorting_columns;

    /** Byte offset from beginning of file to first page (data or dictionary)
     * in this row group **/
    int64_t file_offset;

    /** Total byte size of all compressed (and potentially encrypted) column
     * data in this row group **/
    int64_t total_compressed_size;

    /** Row group ordinal in the file **/
    int16_t ordinal;
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
    chunked_vector<std::pair<ss::sstring, ss::sstring>> key_value_metadata;

    /** String for application that wrote this file.  This should be in the
     * format <Application> version <App Version> (build <App Build Hash>). e.g.
     * impala version 1.0 (build 6cf94d29b2b7115df4de2c06e2ab4326d721eb55)
     */
    ss::sstring created_by;
    /**
     * Sort order used for the min_value and max_value fields in the Statistics
     * objects and the min_values and max_values fields in the ColumnIndex
     * objects of each column in this file. Sort orders are listed in the order
     * matching the columns in the schema. The indexes are not necessary the
     * same though, because only leaf nodes of the schema are represented in the
     * list of sort orders.
     *
     * Without column_orders, the meaning of the min_value and max_value fields
     * in the Statistics object and the ColumnIndex object is undefined. To
     * ensure well-defined behaviour, if these fields are written to a Parquet
     * file, column_orders must be written as well.
     *
     * The obsolete min and max fields in the Statistics object are always
     * sorted by signed comparison regardless of column_orders.
     */
    chunked_vector<column_order> column_orders;
};

/**
 * Encode the file metadata into binary form using the Apache Thift compact wire
 * format.
 */
iobuf encode(const file_metadata& metadata);

} // namespace serde::parquet
