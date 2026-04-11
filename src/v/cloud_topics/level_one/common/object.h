/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "absl/container/btree_map.h"
#include "base/format_to.h"
#include "base/seastarx.h"
#include "base/units.h"
#include "container/chunked_vector.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/timestamp.h"
#include "serde/envelope.h"

#include <seastar/core/file.hh>
#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>

#include <fmt/format.h>

#include <limits>

namespace cloud_topics::l1 {

// clang-format off
// L1 Object File Format:
// =====================
//
// An L1 object consists of multiple partitions written sequentially, each
// containing a series of record batches, followed by a footer with index data.
//
// Structure:
// [Partition 1 Marker][Partition 1 Data][Partition 2 Marker][Partition 2 Data]...[Footer][Footer Size]
//
// Components:
// 1. Partition Marker: A data type delimiter (1 byte) + size (4 bytes) + serialized model::topic_id_partition
//    - Delimiter: 0x01 (data_type::partition_marker)
//    - Size: uint32_t size of the serialized topid id partition data
//    - Data: Serialized model::topic_id_partition identifying the partition
//
// 2. Partition Data: Sequence of kafka record batches for the partition,
//    with offsets strictly increasing within each partition
//    - Each batch prefixed with delimiter 0x00 (data_type::kafka_batch)
//    - Followed by fixed-width batch header (all header fields in order)
//    - Followed by batch data (variable size recorded in the header)
//
// 3. Footer: A data type delimiter (1 byte) + size (4 bytes) + serialized footer
//    - Delimiter: 0x02 (data_type::footer)
//    - Size: uint32_t size of the serialized footer data
//    - Data: Serialized footer with all partition metadata
//
// 4. Footer Size: Final 4 bytes containing uint32_t size of the footer data
//    (including the delimiter byte and size field)
//
// Data type delimiters:
// - 0x00: kafka_batch - Standard kafka record batch
// - 0x01: partition_marker - Marks start of new partition
// - 0x02: footer - Contains object index metadata
//
// Index entries are created at a configurable byte interval (default 4MiB, see
// cloud_topics_l1_indexing_interval) to enable efficient seeking by offset or
// timestamp within a partition. The index entries are recorded within the footer.
//
// clang-format on

// This file defines the metadata/indexing structure for level one objects. It's
// written as a footer in the object in cloud storage.
struct footer
  : serde::checksum_envelope<footer, serde::version<0>, serde::version<0>> {
    // Information about each partition in the object.
    struct partition
      : serde::envelope<partition, serde::version<0>, serde::version<0>> {
        // The offset in the file where this partition's data starts (after the
        // partition marker).
        size_t file_position = 0;
        // The size of the partition data in bytes, starting from
        // `file_position`.
        size_t length = 0;

        struct index_entry
          : serde::envelope<index_entry, serde::version<0>, serde::version<0>> {
            // This file_position is the offset in the file where the data batch
            // that corresponds to this index entry is located.
            size_t file_position = 0;
            // The kafka base_offset of the kafka batch that is located at
            // `file_position`.
            kafka::offset kafka_offset;
            // The maximum timestamp seen so far (inclusive of the batch that
            // generated this index entry) of the kafka batches within the
            // partition.
            model::timestamp max_timestamp;

            auto serde_fields() {
                return std::tie(file_position, kafka_offset, max_timestamp);
            }
            bool operator==(const index_entry&) const = default;
            fmt::iterator format_to(fmt::iterator) const;
        };
        // Index information for l1 data, this is a snapshot of the state at a
        // periodic interval within the partition data. The interval is
        // configurable via cloud_topics_l1_indexing_interval (default ~4MiB).
        //
        // NOTE: we're working with variably sized batches, it may not be
        // precisely every 4MiB.
        //
        // NOTE: The index entries here are sorted by `file_position` and
        // `kafka_offset`.
        chunked_vector<index_entry> indexes;

        // The offset range of this partition.
        kafka::offset first_offset;
        kafka::offset last_offset;

        // The maximum timestamp in this entire partition.
        model::timestamp max_timestamp;

        ss::future<> serde_async_read(iobuf_parser&, serde::header);
        ss::future<> serde_async_write(iobuf&) const;
        bool operator==(const partition&) const = default;
        fmt::iterator format_to(fmt::iterator) const;

        partition copy() const;
    };

    // All of partitions in the object, along with their metadata.
    //
    // If there are multiple partitions written to an object, then they will
    // appear multiple times.
    //
    // However in terms of offsets, there *must* not be overlapping ranges
    // within the same file.
    absl::btree_multimap<model::topic_id_partition, partition> partitions;

    footer copy() const;

    ss::future<> serde_async_read(iobuf_parser&, serde::header);
    ss::future<> serde_async_write(iobuf&) const;

    bool operator==(const footer&) const = default;
    fmt::iterator format_to(fmt::iterator) const;

    // seek result is the result of asking the index where to start reading some
    // data based on an offset or time query. Returned is the position or offset
    // within the file to start reading from, as well as the remaining length of
    // the partition data within that chunk.
    struct seek_result {
        size_t file_position = 0;
        size_t length = 0;

        bool operator==(const seek_result&) const = default;
        fmt::iterator format_to(fmt::iterator) const;
    };
    // The value returned when an index search doesn't have contain matching
    // data.
    constexpr static seek_result npos = {
      .file_position = std::numeric_limits<size_t>::max(),
      .length = 0,
    };

    // Return the file position of the latest record batch that has the offset
    // at or before the given offset. If the offset is not in this file then
    // `npos` is returned.
    //
    // Example:
    //
    // If the footer has the following offset ranges indexed for the given
    // topic_id_partition:
    //
    // [[1, 10], [11, 20], [30, 40]]
    //
    // Searching for offset 5 would yield the position of the batch[0],
    // while a search for offsets 25 or 40 would yield batch[2]. Searching for
    // offset 50 would yield `npos`.
    seek_result file_position_before_kafka_offset(
      const model::topic_id_partition&, kafka::offset) const;

    // Return the file position of the latest record batch that has a
    // max_timestamp at or before the given timestamp. If the timestamp is
    // greater than all timestamps in this file, then `npos` is returned.
    //
    // Example:
    //
    // If the footer has the following max timestamps indexed for the given
    // topic_id_partition:
    //
    // 3, 10, 10, 10, 40
    //
    // Searching for timestamp 5 or 10 would yield the position of the batch[1],
    // the timestamp 1 would yield the position of batch[0].
    // While a search for timestamp 25 or 40 would yield batch[4] and the
    // timestamp 50 would yield `npos`.
    seek_result file_position_before_max_timestamp(
      const model::topic_id_partition&, model::timestamp);

    // Read the footer using the suffix of an L1 object.
    //
    // Returns either the footer, or the *additional* bytes needed to be
    // prepended to the iobuf in order to complete reading the footer.
    //
    // REQUIRES: that the iobuf is at least the last 4 bytes of the file, but
    // likely you want to optimistically read more data (say 512KiB) if you
    // don't know the the exact footer location.
    //
    // Here's an example of how to use this function:
    //
    // ```c++
    // size_t object_size = ...;
    // auto iobuf = co_await read_object(
    //   handle,
    //   {.offset = object_size - 1_KiB, .size = 1_KiB},
    // );
    // auto result = co_await l1::footer::read(iobuf.share());
    // if (std::holds_alternative<l1::footer>(result)) {
    //   return std::get<l1::footer>(result);
    // }
    // size_t extra = std::get<size_t>(result);
    // auto missing = co_await read_object(
    //   handle,
    //   {.offset = object_size - 1_KiB - extra, .size = extra},
    // );
    // missing.append(std::move(iobuf));
    // result = co_await l1::footer::read(std::move(missing));
    // return std::get<l1::footer>(result);
    // ```
    static ss::future<std::variant<footer, size_t>> read(iobuf);
};

// A builder of an l1 object, which is a collection of partitions.
//
// An L1 object is a sequence of partition segments, each segment is a sequence
// of batches within a partition. There is a special marker that is used to
// seperate partitions and contain the footer/index information.
//
// NOTE: If there is an error at any point when using the builder, it must be
// discarded. It's possible there is partially flushed data that would result
// in an invalid file if resumed.
//
// NOTE: It's valid to call start_partition() with the same topic_id_partition
// multiple times but the data *must* contain disjoint offset ranges. There is
// currently no restriction that segments for the same topic_id_partition must
// be written in order.
class object_builder {
public:
    object_builder() = default;
    object_builder(const object_builder&) = default;
    object_builder(object_builder&&) = delete;
    object_builder& operator=(const object_builder&) = default;
    object_builder& operator=(object_builder&&) = delete;
    virtual ~object_builder() = default;

    // Options for the object_builder
    struct options {
        constexpr static size_t default_indexing_interval = 4_MiB;
        // The byte interval at which to index the object.
        size_t indexing_interval = default_indexing_interval;
    };

    // Create a new object_builder that writes to the given output stream.
    //
    // The returned object_builder must be closed before destructing.
    static std::unique_ptr<object_builder>
      create(ss::output_stream<char>, options);

    // Start writing batches for this partition.
    //
    // This must be called before any add_batch() calls, and calling this
    // after calling start_partition() implicitly ends the current partition
    // and starts a new one.
    virtual ss::future<> start_partition(model::topic_id_partition tidp) = 0;

    // Append a kafka batch to the object. The batch here is expected to be:
    //
    //  - A raft data batch, (meaning the header's type is set to raft_data).
    //  - The offsets in this batch are > than the previous batch in this
    //    partition.
    virtual ss::future<> add_batch(model::record_batch) = 0;

    // Return the size of file in bytes that has been built so far.
    //
    // After `finish` has been called, this will be the size of the fully
    // constructed file.
    virtual size_t file_size() const = 0;

    // Information about the finished object.
    struct object_info {
        footer index;
        // The start offset of the footer in the written object.
        //
        // The footer can be read using `footer::read`
        size_t footer_offset = 0;
        // The size of the final object written to the output stream in bytes.
        size_t size_bytes = 0;
    };

    // Finish the object, writing the footer/index.
    virtual ss::future<object_info> finish() = 0;

    // Closes the underlying output stream and releases any resources held.
    //
    // Must be called before destructing the object_builder.
    virtual ss::future<> close() = 0;
};

// A reader for an L1 object, which reads partitions and batches constructed
// from object_builder.
//
// This represents an L1 object, which is just a stream of segments,where a
// segment is a series of batches for a topic_id_partition. If used with a
// object_seeker, then it can also represent the tail of an L1 object (see
// object_seeker for more).
class object_reader {
public:
    // A marker struct that indicates we've reached the end of the file.
    struct eof {};

    /// Tag returned by peek() when the next item is a partition marker.
    struct partition_tag {};

    /// Tag returned by peek() when the next item is a footer.
    struct footer_tag {};

    object_reader() = default;
    object_reader(const object_reader&) = delete;
    object_reader(object_reader&&) = delete;
    object_reader& operator=(const object_reader&) = delete;
    object_reader& operator=(object_reader&&) = delete;
    virtual ~object_reader() = default;

    // Create an object_reader from a file.
    //
    // Offset can be used to start at a particular batch, using the file
    // position recorded in a footer
    static std::unique_ptr<object_reader> create(
      ss::file,
      size_t offset = 0,
      size_t length = std::numeric_limits<size_t>::max());

    // Create an object_reader from a file path.
    //
    // Offset can be used to start at a particular batch, using the file
    // position recorded in a footer.
    static ss::future<std::unique_ptr<object_reader>> create(
      std::filesystem::path,
      size_t offset = 0,
      size_t length = std::numeric_limits<size_t>::max());

    // Create an object_reader from a stream.
    static std::unique_ptr<object_reader> create(ss::input_stream<char>);

    // When reading the next entry it maybe a raft data batch OR
    // it could be a partition marker, meaning the data for that partition ended
    // and we are about to start reading the next partition. If an footer
    // is returned, then we've reached the end of the file and the footer is
    // returned.
    using result = std::
      variant<model::topic_id_partition, model::record_batch, footer, eof>;

    /// Discriminator returned by peek(). For batches, contains the
    /// header. For all other item types, contains a lightweight tag.
    using peek_result = std::
      variant<model::record_batch_header, partition_tag, footer_tag, eof>;

    // Read the "next" item from the L1 object.
    //
    // The next item can be either a partition marker
    // (model::topic_id_partition) or a data batch (model::record_batch).
    virtual ss::future<result> read_next() = 0;

    /// Peek at the next item's discriminator. For batches, reads the
    /// data_type byte and fixed-width header from the stream. For all
    /// other item types, reads only the data_type byte; the remaining
    /// data is left in the stream for read_next() to consume.
    ///
    /// Idempotent: calling peek() multiple times without an intervening
    /// read_next() returns the same result without further I/O.
    virtual ss::future<peek_result> peek() = 0;

    // Close the reader, releasing any resources it holds.
    //
    // Must be called before destructing the object_reader.
    virtual ss::future<> close() = 0;
};

// The parameters to combine multiple L1 objects into a single L1 object.
struct combine_objects_parameters {
    // The input object that will be combined.
    struct input_object {
        // A stream for the full object that was built using an
        // `object_builder`.
        //
        // Since this takes ownership of the stream it will also close the
        // stream.
        ss::input_stream<char> stream;
        // The output from `object_builder::finish` containing metadata from the
        // constructed object.
        object_builder::object_info info;
    };
    // Mechanically, the objects to combine into a single object. The resulting
    // object will contain data in the order of these input objects.
    chunked_vector<input_object> inputs;
    // The stream to write the output to.
    //
    // Since this takes ownership of the output stream it will also be
    // responsible for closing it.
    ss::output_stream<char> output;
};

// Combine multiple L1 objects into a single object, and write it to the output
// stream.
//
// The new object metadata is returned after the merging of files is
// successful.
ss::future<object_builder::object_info>
  combine_objects(combine_objects_parameters);

} // namespace cloud_topics::l1
