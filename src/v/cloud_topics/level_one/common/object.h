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
#include "base/seastarx.h"
#include "base/units.h"
#include "container/chunked_vector.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/timestamp.h"
#include "serde/envelope.h"

#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>

#include <fmt/format.h>

#include <limits>

namespace experimental::cloud_topics::l1 {

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
// 1. Partition Marker: A data type delimiter (1 byte) + size (4 bytes) + serialized model::ntp
//    - Delimiter: 0x01 (data_type::partition_marker)
//    - Size: uint32_t size of the serialized ntp data
//    - Data: Serialized model::ntp identifying the partition
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
// Index entries are created approximately every 4MiB to enable efficient seeking by
// offset or timestamp within a partition. The index entries are recorded within the
// footer.
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
        };
        // Index information for l1 data, this is a snapshot of the state at a
        // periodic interval within the partition data. For example, we can
        // generate an index entry every ~4MiB within the partition, and store
        // it here in the footer.
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

        partition copy() const;
    };

    // All of partitions in the object, along with their metadata.
    //
    // If there are multiple partitions written to an object, then they will
    // appear multiple times.
    //
    // However in terms of offsets, there *must* not be overlapping ranges
    // within the same file.
    absl::btree_multimap<model::ntp, partition> partitions;

    footer copy() const;

    ss::future<> serde_async_read(iobuf_parser&, serde::header);
    ss::future<> serde_async_write(iobuf&) const;

    bool operator==(const footer&) const = default;

    // The value returned when an index search doesn't have contain matching
    // data.
    constexpr static size_t npos = std::numeric_limits<size_t>::max();

    // Return the file position of the latest record batch that has the offset
    // at or before the given offset. If the offset is not in this file then
    // `npos` is returned.
    //
    // Example:
    //
    // If the footer has the following offset ranges indexed for the given
    // ntp:
    //
    // [[1, 10], [11, 20], [30, 40]]
    //
    // Searching for offset 5 would yield the position of the batch[0],
    // while a search for offsets 25 or 40 would yield batch[2]. Searching for
    // offset 50 would yield `npos`.
    size_t file_position_before_kafka_offset(const model::ntp&, kafka::offset);

    // Return the file position of the latest record batch that has a
    // max_timestamp at or before the given timestamp. If the timestamp is
    // greater than all timestamps in this file, then `npos` is returned.
    //
    // Example:
    //
    // If the footer has the following max timestamps indexed for the given
    // ntp:
    //
    // 3, 10, 10, 10, 40
    //
    // Searching for timestamp 5 or 10 would yield the position of the batch[1],
    // the timestamp 1 would yield the position of batch[0].
    // While a search for timestamp 25 or 40 would yield batch[4] and the
    // timestamp 50 would yield `npos`.
    size_t
    file_position_before_max_timestamp(const model::ntp&, model::timestamp);

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
// NOTE: It's valid to call start_partition() with the same NTP multiple times
// but the data *must* contain disjoint offset ranges. There is currently no
// restriction that segments for the same NTP must be written in order.
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
        constexpr static size_t default_indexing_frequency = 4_MiB;
        // The frequency at which to index the object, in bytes.
        size_t indexing_frequency = default_indexing_frequency;
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
    virtual ss::future<> start_partition(model::ntp ntp) = 0;

    // Append a kafka batch to the object. The batch here is expected to be:
    //
    //  - A raft data batch, (meaning the header's type is set to raft_data).
    //  - The offsets in this batch are > than the previous batch in this
    //    partition.
    virtual ss::future<> add_batch(model::record_batch) = 0;

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
// segment is a series of batches for a NTP. If used with a object_seeker, then
// it can also represent the tail of an L1 object (see object_seeker for more).
class object_reader {
public:
    // A marker struct that indicates we've reached the end of the file.
    struct eof {};

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
    using result = std::variant<model::ntp, model::record_batch, footer, eof>;

    // Read the "next" item from the L1 object.
    //
    // The next item can be either a partition marker (model::ntp) or a
    // data batch (model::record_batch).
    virtual ss::future<result> read_next() = 0;

    // Close the reader, releasing any resources it holds.
    //
    // Must be called before destructing the object_reader.
    virtual ss::future<> close() = 0;
};

} // namespace experimental::cloud_topics::l1

template<>
struct fmt::formatter<
  experimental::cloud_topics::l1::footer::partition::index_entry> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

    template<typename FormatContext>
    typename FormatContext::iterator format(
      const experimental::cloud_topics::l1::footer::partition::index_entry&
        entry,
      FormatContext& ctx) const {
        return fmt::format_to(
          ctx.out(),
          "{{file_position: {}, kafka_offset: {}, max_timestamp: {}}}",
          entry.file_position,
          entry.kafka_offset,
          entry.max_timestamp);
    }
};

template<>
struct fmt::formatter<experimental::cloud_topics::l1::footer::partition> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

    template<typename FormatContext>
    typename FormatContext::iterator format(
      const experimental::cloud_topics::l1::footer::partition& partition,
      FormatContext& ctx) const {
        return fmt::format_to(
          ctx.out(),
          "{{file_position: {}, length: {}, first_offset: {}, last_offset: {}, "
          "max_timestamp: {}, indexes: [{}]}}",
          partition.file_position,
          partition.length,
          partition.first_offset,
          partition.last_offset,
          partition.max_timestamp,
          fmt::join(partition.indexes, ", "));
    }
};

template<>
struct fmt::formatter<experimental::cloud_topics::l1::footer> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

    template<typename FormatContext>
    typename FormatContext::iterator format(
      const experimental::cloud_topics::l1::footer& index,
      FormatContext& ctx) const {
        auto out = fmt::format_to(ctx.out(), "{{partitions: [");
        for (const auto& [ntp, partition] : index.partitions) {
            out = fmt::format_to(
              out, "{{ntp: {}, partition: {}}}, ", ntp, partition);
        }
        return fmt::format_to(out, "]}}");
    }
};
