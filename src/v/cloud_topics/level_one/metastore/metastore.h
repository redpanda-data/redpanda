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

#include "base/seastarx.h"
#include "cloud_topics/level_one/common/object_id.h"
#include "cloud_topics/level_one/metastore/metastore_manifest.h"
#include "cloud_topics/level_one/metastore/offset_interval_set.h"
#include "container/chunked_hash_map.h"
#include "container/chunked_vector.h"
#include "model/fundamental.h"
#include "model/timestamp.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>

#include <expected>
#include <optional>

namespace cloud_storage {
struct remote_label;
} // namespace cloud_storage

namespace cloud_topics::l1 {

// Interface to interact with the L1 metastore. Meant to be totally agnostic to
// the underlying implementation, whether it's an in-memory store, replicated
// store, or even an external service.
//
// The metastore contains metadata about cloud topic partitions and the L1
// objects in which they store their data. It is designed to be a simple state
// store, performing atomic updates to state and answering simple queries for
// callers. The metastore itself isn't meant to perform complex partition
// storage management tasks (e.g. garbage collection or compaction), but rather
// it exposes primitives to enable robustly performing such tasks.
//
// At a high level, the metastore contains the following metadata:
// - a fixed set of information about each Kafka partition (e.g. start offset,
//   next offset),
// - the set of extents (pointers into L1 objects) associated with each Kafka
//   partition, with enough metadata to find an L1 object by offset or
//   timestamp,
// - the set of L1 objects and some information about them to enable efficient
//   object downloads (e.g. footer location).
//
// While callers should generally avoid sending invalid requests to the
// metastore (e.g. creating gaps in offset space), it is up to the metastore
// implementation to ensure such requests are rejected and don't have harmful
// side effects. As such, callers can think of this interface as thread safe.
class metastore {
protected:
    metastore() = default;

public:
    virtual ~metastore() = default;
    metastore(const metastore&) = delete;
    metastore& operator=(const metastore&) = delete;
    metastore(const metastore&&) = delete;
    metastore& operator=(const metastore&&) = delete;

    enum class errc {
        missing_ntp,
        invalid_request,
        out_of_range,

        // Indicates an issue sending or receiving the request rather than a
        // logical error. Expected that a retry may succeed.
        transport_error,
    };
    struct object_metadata {
        struct ntp_metadata {
            model::topic_id_partition tidp;
            kafka::offset base_offset;
            kafka::offset last_offset;
            model::timestamp max_timestamp;
            size_t pos;
            size_t size;
        };
        using ntp_metas_list_t = chunked_vector<ntp_metadata>;

        object_id oid;
        size_t footer_pos;
        size_t object_size;
        ntp_metas_list_t ntp_metas;
    };
    struct object_response {
        object_id oid;
        size_t footer_pos;
        size_t object_size;
        // The first offset available in the object (inclusive).
        kafka::offset first_offset;
        // The last offset available in the object (inclusive).
        // This can be used to skip to the next offset.
        kafka::offset last_offset;
    };

    // Interface to build object metadata for the L1 metastore. Meant to be
    // totally agnostic to the underlying implementation, whether it's
    // logically partitioned or not.
    class object_metadata_builder {
    public:
        using error = named_type<ss::sstring, struct builder_error_tag>;
        virtual ~object_metadata_builder() = default;

        // Returns an object ID that hasn't yet been finished that is
        // appropriate for the given partition. Potentially shares the object
        // with another partition, if the object is allowed by the metastore to
        // be shared by the other partition.
        virtual ss::future<std::expected<object_id, error>>
        get_or_create_object_for(const model::topic_id_partition&) = 0;

        // Creates a new object for the given partition. It is guaranteed that
        // this object is brand new/unused. It is not guaranteed, however, that
        // it cannot be accessed concurrently by other users of this
        // `object_metadata_builder` who call `get_or_create_object_for()`. If
        // complete isolation of objects is required, ensure that all users of
        // this particular `object_metadata_builder` only invoke
        // `create_object_for()` and `finish()` for the provided `object_id`
        // within a tightly bounded scope.
        virtual ss::future<std::expected<object_id, error>>
        create_object_for(const model::topic_id_partition&) = 0;

        // Removes a pending object from the builder. The object must be in the
        // pending state. Further calls to get_or_create_object_for() will not
        // return the object id. Any other call that references the object id
        // will return an error.
        virtual std::expected<void, error> remove_pending_object(object_id) = 0;

        // Adds the given partition metadata for the given object. Expected
        // that finish() has not yet been called on the object.
        virtual std::expected<void, error>
          add(object_id, object_metadata::ntp_metadata) = 0;

        // Tracks the given object as finished. Further calls to
        // get_or_create_object_for() will not return the finished object ID.
        virtual std::expected<void, error>
        finish(object_id, size_t footer_pos, size_t object_size) = 0;

        // Returns `true` if this builder has no finalized objects in it, and
        // `false` if it does. Intended to be called after all pending objects
        // have been either removed or finished, but before this builder is
        // passed to a metastore interface.
        virtual bool is_empty() const = 0;
    };

    struct offsets_response {
        kafka::offset start_offset;
        kafka::offset next_offset;
    };
    virtual ss::future<
      std::expected<std::unique_ptr<object_metadata_builder>, errc>>
    object_builder() = 0;

    struct term_offset {
        model::term_id term;

        // The first offset that the given term was seen in a given offset
        // range. Note, this doesn't necessarily indicate the start offset for
        // the term in the entire log, just within a specific range (e.g. the
        // range covered by a newly added object)
        kafka::offset first_offset;
    };
    // Mapping per partition of the first offset seen for each term for a given
    // set of extents. Both the terms and the offsets must be strictly
    // monotonically increasing.
    using term_offset_map_t = chunked_hash_map<
      model::topic_id_partition,
      chunked_vector<term_offset>>;

    // Returns offsets (e.g. start, next) for the given partition.
    virtual ss::future<std::expected<offsets_response, errc>>
    get_offsets(const model::topic_id_partition&) = 0;

    struct size_response {
        // The total size of the partition in bytes.
        size_t size{0};
        // The number of extents in the partition.
        size_t num_extents{0};
    };
    // Returns the size of the partition in bytes.
    virtual ss::future<std::expected<size_response, errc>>
    get_size(const model::topic_id_partition&) = 0;

    struct add_response {
        // The actual next offsets for any topic partitions whose input objects
        // were not properly aligned in a given add request.
        //
        // Callers should treat this as the new source of truth for subsequent
        // attempts to add objects for these partitions.
        chunked_hash_map<model::topic_id_partition, kafka::offset>
          corrected_next_offsets;
    };
    // If the input is invalid (e.g. unordered, empty, contains an object that
    // already exists) an error is returned.
    //
    // If no error is returned, this means that the request was valid, though
    // it does not imply that all extents were accepted by the metastore. The
    // response should be examined to determine if subsequent add_objects()
    // requests need to be re-aligned to a different offset.
    virtual ss::future<std::expected<add_response, errc>>
    add_objects(const object_metadata_builder&, const term_offset_map_t&) = 0;

    // Adds the given objects to the metastore, expecting that the new extents
    // replace an extent or set of extents covering the same range.
    // It is expected that the set of new extents per partition covers a
    // contiguous range of that partition's offset space.
    //
    // While these constraints aren't the only way we could ensure
    // correctness, these simplify accounting and makes it easier to validate
    // that we haven't lost data.
    virtual ss::future<std::expected<void, errc>>
    replace_objects(const object_metadata_builder&) = 0;

    // Moves the start offset of the given partition's log to the given offset.
    virtual ss::future<std::expected<void, errc>>
    set_start_offset(const model::topic_id_partition&, kafka::offset) = 0;

    struct topic_removal_response {
        // Topic IDs that were not removed from the metastore and still have
        // state to be removed in a subsequent attempt.
        chunked_hash_set<model::topic_id> not_removed;
    };
    // Removes the given topics from the metastore, returning any that need to
    // be retried. If an error is returned, callers should assume that all
    // topics need to be retried.
    virtual ss::future<std::expected<topic_removal_response, errc>>
    remove_topics(const chunked_vector<model::topic_id>&) = 0;

    // Finds the first object of a given partition with data greater than or
    // equal to the given offset. If no such offset exists, returns
    // `out_of_range`.
    virtual ss::future<std::expected<object_response, errc>>
    get_first_ge(const model::topic_id_partition&, kafka::offset) = 0;

    // Finds the first object of a given partition with data greater than or
    // equal to the given timestamp, that starts at or after the provided
    // offset. If no such timestamp exists, returns `out_of_range`.
    virtual ss::future<std::expected<object_response, errc>> get_first_ge(
      const model::topic_id_partition&, kafka::offset, model::timestamp) = 0;

    // Finds the kafka offset such that if data was truncated before this offset
    // where the total amount of data left would be ~size (within the
    // granularity of a single object's size). This is intended to be used for
    // bytes based retention of the metastore.
    //
    // If no such offset exists, returns `out_of_range`.
    virtual ss::future<std::expected<kafka::offset, errc>>
    get_first_offset_for_bytes(const model::topic_id_partition&, uint64_t size)
      = 0;

    // Returns the end (i.e. one past the last) offset at which data was added
    // for the given partition term.
    virtual ss::future<std::expected<kafka::offset, errc>>
    get_end_offset_for_term(const model::topic_id_partition&, model::term_id)
      = 0;

    // Returns the partition term in which the given offset was added.
    virtual ss::future<std::expected<model::term_id, errc>>
    get_term_for_offset(const model::topic_id_partition&, kafka::offset) = 0;

    using compaction_epoch
      = named_type<int64_t, struct metastore_compaction_epoch>;

    // Compaction metadata updates per partition
    //
    // Kafka compaction works by taking "dirty" ranges of data, collecting the
    // keys and offsets within that range, and then removing all older
    // instances of those keys from the beginning of the log. At that point,
    // that range of data is considered "cleaned". For all keys in a cleaned
    // range, only the latest version of that key exists in the log up to the
    // cleaned range.
    //
    // Kafka also has the concept of a "tombstone" (empty-value record) that
    // indicates the logical deletion of the record's key. Once a given
    // tombstone record has been cleaned, a timer begins and after
    // delete.retention.ms elapses, the cleaned tombstone record may be
    // removed.
    //
    // To support these ideas, the metastore tracks cleaned offset ranges and
    // whether they contain tombstones. This allows it to expose dirty ranges
    // and ranges with removable tombstones to callers.
    struct compaction_update {
        bool operator==(const compaction_update&) const = default;
        struct cleaned_range {
            bool operator==(const cleaned_range&) const = default;
            kafka::offset base_offset;
            kafka::offset last_offset;

            // Whether or not the cleaned range included any tombstones.
            bool has_tombstones{false};

            fmt::iterator format_to(fmt::iterator it) const {
                return fmt::format_to(
                  it,
                  "{{offsets:({}~{}), has_tombstones:{}}}",
                  base_offset,
                  last_offset,
                  has_tombstones);
            }
        };
        // Ranges indicating that the data's keys have been fully deduplicated
        // from the start of the log.
        chunked_vector<cleaned_range> new_cleaned_ranges;

        // Ranges of cleaned offsets that previously had tombstones, that have
        // been removed.
        offset_interval_set removed_tombstones_ranges;

        // Timestamp at which the compaction operation happened.
        model::timestamp cleaned_at;

        // The expected compaction epoch of the log at time of update
        // application.
        compaction_epoch expected_compaction_epoch;

        fmt::iterator format_to(fmt::iterator it) const {
            return fmt::format_to(
              it,
              "{{new_cleaned_ranges:{}, removed_tombstone_ranges:{}, "
              "cleaned_at:{}, expected_compaction_epoch: {}}}",
              new_cleaned_ranges,
              removed_tombstones_ranges,
              cleaned_at,
              expected_compaction_epoch);
        }
    };
    using compaction_map_t
      = chunked_hash_map<model::topic_id_partition, compaction_update>;

    struct compaction_offsets_response {
        // Offset ranges whose keys have not been fully deduplicated from the
        // start of the log.
        offset_interval_set dirty_ranges;

        // The set of offset ranges that contain tombstones whose keys have
        // been cleaned long enough to be eligible for tombstone removal.
        //
        // A compaction method, when iterating over a tombstone record, may
        // consult this to determine if the tombstone should be removed.
        offset_interval_set removable_tombstone_ranges;

        fmt::iterator format_to(fmt::iterator it) const {
            return fmt::format_to(
              it,
              "{{dirty_ranges:{}, removable_tombstone_ranges:{}}}",
              dirty_ranges,
              removable_tombstone_ranges);
        }
    };
    // Similar to replace_objects(), but with additional constraints based on
    // compaction metadata. See get_compaction_info() for more details on
    // expected usage.
    virtual ss::future<std::expected<void, errc>> compact_objects(
      const object_metadata_builder&, const compaction_map_t&) = 0;

    // All the information required to query a `compaction_info_response` from
    // the metastore. Parameters are used for call to `get_compaction_info()`.
    struct compaction_info_spec {
        model::topic_id_partition tidp;
        model::timestamp tombstone_removal_upper_bound_ts;
    };

    struct compaction_info_response {
        // The dirty ratio of the log/partition.
        double dirty_ratio;
        // The earliest dirty timestamp in the log. `std::nullopt` if there is
        // no such timestamp.
        std::optional<model::timestamp> earliest_dirty_ts;
        // Dirty ranges & removable tombstone ranges.
        compaction_offsets_response offsets_response;
        // The log's current compaction epoch.
        compaction_epoch compaction_epoch;
        // The log's current start_offset. Can be expected to be == 0 for
        // `compact` only topics, might be > 0 for `compact,delete` topics.
        kafka::offset start_offset;

        fmt::iterator format_to(fmt::iterator it) const {
            return fmt::format_to(
              it,
              "{{dirty_ratio:{}, earliest_dirty_ts:{}, offsets_response:{}, "
              "compaction_epoch:{}, start_offset:{}}}",
              dirty_ratio,
              earliest_dirty_ts,
              offsets_response,
              compaction_epoch,
              start_offset);
        }
    };

    // Returns metadata required to determine what to compact for the given
    // partition - e.g dirty ratio and earliest dirty timestamp, as well as
    // compaction offsets.
    //
    // Cleaned ranges with tombstones that were cleaned at or below
    // tombstone_removal_upper_bound_ts are eligible to have tombstones
    // entirely removed. These ranges will be returned in the response.
    //
    // Below is pseudocode for sample usage:
    //
    // info = co_await metastore.get_compaction_info( \
    //   partition, tombstone_removal_upper_bound_ts);
    //
    // key_offset_map m;
    // cleaned_ranges new_cleaned_ranges;
    // offset_interval_set removed_tombstones_ranges;
    //
    // # Build an offset map based on the dirty ranges.
    // for dirty_range in info.dirty_ranges:
    //     reader = log.reader(dirty_range.base, dirty_range.last)
    //     co_await m.add_latest_offset_per_key(reader)
    //
    //     cleaned_range r(...offset range that was actually read...);
    //     if ...reader witnessed tombstones...:
    //         cleaned_range.cleaned_with_tombstones_at = now()
    //
    //     new_cleaned_ranges.insert(cleaned_range)
    //
    // # Determine what ranges to remove tombstones from.
    // removed_tombstones_ranges = \
    //   ...info.removable_tombstone_ranges that fall below m.max_offset()...
    //
    // # This operation deduplicates based on the offset map and removes
    // # tombstones in the given ranges, up to the max indexed by the map.
    // objects = co_await log.compact( \
    //   m.max_offset(), m, removed_tombstones_ranges)
    //
    // co_await metastore.compact_objects( \
    //   objects, {{tp, {new_cleaned_ranges, removed_tombstones_ranges}}})
    virtual ss::future<std::expected<compaction_info_response, errc>>
    get_compaction_info(const compaction_info_spec&) = 0;

    using compaction_info_map = chunked_hash_map<
      model::topic_id_partition,
      std::expected<compaction_info_response, errc>>;

    // Vectorized RPC for obtaining compaction state for a number of partitions.
    virtual ss::future<std::expected<compaction_info_map, errc>>
    get_compaction_infos(const chunked_vector<compaction_info_spec>&) = 0;

    struct extent_object_info {
        object_id oid;
        size_t footer_pos{0};
        size_t object_size{0};
    };

    struct extent_metadata {
        kafka::offset base_offset;
        kafka::offset last_offset;
        model::timestamp max_timestamp;
        // Only populated when include_object_metadata is set.
        std::optional<extent_object_info> object_info;

        fmt::iterator format_to(fmt::iterator it) const {
            if (object_info.has_value()) {
                return fmt::format_to(
                  it,
                  "{{offsets:({}~{}), max_timestamp:{}, oid:{}, "
                  "footer_pos:{}, object_size:{}}}",
                  base_offset,
                  last_offset,
                  max_timestamp,
                  object_info->oid,
                  object_info->footer_pos,
                  object_info->object_size);
            }
            return fmt::format_to(
              it,
              "{{offsets:({}~{}), max_timestamp:{}}}",
              base_offset,
              last_offset,
              max_timestamp);
        }
    };

    using extent_metadata_vec = chunked_vector<extent_metadata>;

    struct extent_metadata_response {
        extent_metadata_vec extents{};
        // True when no more extents exist beyond this response (end of
        // iteration). False when more extents may exist (hit max_num_extents
        // limit).
        bool end_of_stream{true};
    };

    using include_object_metadata
      = ss::bool_class<struct include_object_metadata_tag>;

    // Returns a number of extents in the offset range `[start, end]`
    // inclusively, and in ascending offset order. Useful for forward
    // iteration over an extent-aligned offset range- that is, for an extent
    // metastore state of `[[0, 9],[10,19],[20,29]]`, and a request like
    // `get_extent_metadata_ge([0, 15])`, the returned extents will be `[[0, 9],
    // [10, 19]]`.
    //
    // When include_object_metadata is yes, each extent_metadata in the
    // response will also have oid, footer_pos, and object_size populated.
    virtual ss::future<std::expected<extent_metadata_response, errc>>
    get_extent_metadata_forwards(
      const model::topic_id_partition&,
      kafka::offset,
      kafka::offset,
      size_t,
      include_object_metadata) = 0;

    // Returns a number of extents in the offset range `[start, end]`
    // inclusively, and in descending offset order. Useful for backward
    // iteration over an extent-aligned offset range- that is, for an extent
    // metastore state of `[[0, 9],[10,19],[20,29]]`, and a request like
    // `get_extent_metadata_le([0, 15])`, the returned extents will be `[[10,
    // 19], [0, 9]]`.
    //
    // NOTE: unlike get_extent_metadata_forwards, this method does not
    // support include_object_metadata. Add it here if needed.
    virtual ss::future<std::expected<extent_metadata_response, errc>>
    get_extent_metadata_backwards(
      const model::topic_id_partition&, kafka::offset, kafka::offset, size_t)
      = 0;

    // Flushes all metastore partitions to cloud storage.
    virtual ss::future<std::expected<std::nullopt_t, errc>> flush() = 0;

    // Restores metastore state from a previously flushed manifest in the given
    // cluster's cloud storage. This downloads the metastore topic manifest,
    // ensures the metastore topic exists with the correct number of partitions,
    // and restores each partition's domain to its corresponding domain_uuid.
    virtual ss::future<std::expected<std::nullopt_t, errc>>
    restore(const cloud_storage::remote_label&) = 0;
};

} // namespace cloud_topics::l1

template<>
struct fmt::formatter<cloud_topics::l1::metastore::errc> final
  : fmt::formatter<std::string_view> {
    template<typename FormatContext>
    auto format(
      const cloud_topics::l1::metastore::errc& k, FormatContext& ctx) const {
        using enum cloud_topics::l1::metastore::errc;
        switch (k) {
        case invalid_request:
            return formatter<string_view>::format(
              "metastore::errc::invalid_request", ctx);
        case missing_ntp:
            return formatter<string_view>::format(
              "metastore::errc::missing_ntp", ctx);
        case out_of_range:
            return formatter<string_view>::format(
              "metastore::errc::out_of_range", ctx);
        case transport_error:
            return formatter<string_view>::format(
              "metastore::errc::transport_error", ctx);
        }
    }
};
