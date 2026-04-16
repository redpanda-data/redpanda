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

#include "cloud_topics/level_one/common/object_id.h"
#include "cloud_topics/level_one/metastore/lsm/values.h"
#include "lsm/lsm.h"
#include "model/fundamental.h"
#include "utils/detailed_error.h"

namespace cloud_topics::l1 {

// Encapsulate queries that operate on state in a database.
class state_reader {
public:
    enum class errc {
        // There was an issue reading state when building the update. This may
        // be transient (e.g. timeout when reading from object storage).
        io_error,
        // There was an issue reading state that is unexpected. This indicates
        // non-transient errors like broken invariants and should be logged at
        // ERROR level.
        corruption,
        // The underlying state is being shutdown.
        shutting_down,
    };
    using error = detailed_error<errc>;

    struct extent_row {
        ss::sstring key;
        extent_row_value val;
    };

    struct object_row {
        ss::sstring key;
        object_row_value val;
        lsm::sequence_number seqno;
    };

    enum class direction { forward, backward };

    class extent_key_range {
    public:
        extent_key_range(
          ss::sstring base, ss::sstring last, lsm::iterator it, direction dir)
          : _base_key(std::move(base))
          , _last_key(std::move(last))
          , _iter(std::move(it))
          , _direction(dir) {}

        // Returns extent_rows matching exactly between _base_key and
        // _last_key, or generates an error if it can't.
        //
        // Stops generating after the first error.
        ss::coroutine::experimental::generator<std::expected<extent_row, error>>
        get_rows();

        ss::future<chunked_vector<std::expected<extent_row, error>>>
        materialize_rows();

    private:
        ss::sstring to_string();

        ss::sstring _base_key;
        ss::sstring _last_key;

        // Snapshot of the database.
        lsm::iterator _iter;
        direction _direction;
    };

    class object_key_range {
    public:
        object_key_range(ss::sstring base_key, lsm::iterator it)
          : _inclusive_base_key(std::move(base_key))
          , _iter(std::move(it)) {}

        // Returns object_rows starting from _base_key (if provided), or
        // generates an error if it can't.
        //
        // Stops generating after the first error.
        ss::coroutine::experimental::generator<std::expected<object_row, error>>
        get_rows();

    private:
        ss::sstring to_string();

        // Optional lower bound for iteration. If nullopt, starts from the first
        // object key.
        ss::sstring _inclusive_base_key;

        // Snapshot of the database.
        lsm::iterator _iter;
    };

    explicit state_reader(lsm::snapshot snap)
      : snap_(std::move(snap)) {}

    ss::future<std::expected<std::optional<metadata_row_value>, error>>
    get_metadata(const model::topic_id_partition&);

    ss::future<std::expected<std::optional<compaction_state>, error>>
    get_compaction_metadata(const model::topic_id_partition&);

    ss::future<std::expected<std::optional<object_entry>, error>>
      get_object(object_id);

    // Returns the highest term start for the given partition, or nullopt if
    // the partition does not exist.
    ss::future<std::expected<std::optional<term_start>, error>>
    get_max_term(const model::topic_id_partition&);

    // Returns the first extent that contains an offset at or equal to the
    // given offset. If no such extent exists (e.g. all extents are below the
    // offset) returns nullopt.
    ss::future<std::expected<std::optional<extent>, error>>
    get_extent_ge(const model::topic_id_partition&, kafka::offset);

    // Returns the key ranges whose first extent's base offset matches `base`
    // and whose last extent's last offset matches `last`. If no such range
    // exists, return nullopt.
    ss::future<std::expected<std::optional<extent_key_range>, error>>
    get_extent_range(
      const model::topic_id_partition&, kafka::offset base, kafka::offset last);

    // Returns an iterator for all extents that overlap with the inclusive
    // range [min_offset, max_offset]. Returns nullopt if there are no extents
    // in the given bounds.
    ss::future<std::expected<std::optional<extent_key_range>, error>>
    get_inclusive_extents(
      const model::topic_id_partition&,
      std::optional<kafka::offset> min_offset,
      std::optional<kafka::offset> max_offset);
    ss::future<std::expected<std::optional<extent_key_range>, error>>
    get_inclusive_extents_backward(
      const model::topic_id_partition&,
      std::optional<kafka::offset> min_offset,
      std::optional<kafka::offset> max_offset);

    // Returns the term containing the given offset, i.e. the term with the
    // start_offset <= the given offset, or  nullopt if no such term exists.
    ss::future<std::expected<std::optional<term_start>, error>>
    get_term_le(const model::topic_id_partition&, kafka::offset);

    // Returns the end offset (exclusive) for the given term:
    // - If a higher term exists, returns the start_offset of the next term
    // - If this is the highest term, the partition's next_offset
    // Returns nullopt if all terms are below the given term or the term
    // doesn't exist.
    ss::future<std::expected<std::optional<kafka::offset>, error>>
    get_term_end(const model::topic_id_partition&, model::term_id);

    // Returns term row keys for the given partition. If `upper_bound` is
    // provided, returns keys for all terms up to and including the highest
    // term with start_offset <= upper_bound.
    ss::future<std::expected<chunked_vector<ss::sstring>, error>> get_term_keys(
      const model::topic_id_partition&,
      std::optional<kafka::offset> upper_bound);

    // Returns all terms for the given partition, ordered by term_id.
    ss::future<std::expected<chunked_vector<term_start>, error>>
    get_all_terms(const model::topic_id_partition&);

    // Returns all partition IDs that have metadata for the given topic.
    ss::future<std::expected<chunked_vector<model::partition_id>, error>>
    get_partitions_for_topic(const model::topic_id&);

    // Returns an object_key_range starting from the given object_id. If
    // start_oid is nullopt, starts from the first object in the database.
    ss::future<std::expected<object_key_range, error>>
    get_object_range(std::optional<object_id> start_oid);

private:
    ss::future<
      std::expected<std::optional<std::pair<ss::sstring, ss::sstring>>, error>>
    find_inclusive_extent_keys(
      const model::topic_id_partition&,
      std::optional<kafka::offset> min_offset,
      std::optional<kafka::offset> max_offset);

    template<typename KeyT, typename ValT, typename... KeyEncodeArgs>
    ss::future<std::expected<std::optional<ValT>, error>>
    get_val(KeyEncodeArgs...);

    lsm::snapshot snap_;
};

// Returns an object_key_range starting from the given object_id. If start_oid
// is nullopt, starts from the first object in the database.
std::expected<state_reader::object_key_range, state_reader::error>
make_object_range(lsm::iterator iter, std::optional<object_id> start_oid);

} // namespace cloud_topics::l1

template<>
struct fmt::formatter<cloud_topics::l1::state_reader::errc>
  : fmt::formatter<std::string_view> {
    auto
    format(cloud_topics::l1::state_reader::errc e, format_context& ctx) const {
        using errc = cloud_topics::l1::state_reader::errc;
        std::string_view name;
        switch (e) {
        case errc::io_error:
            name = "state_reader::errc::io_error";
            break;
        case errc::corruption:
            name = "state_reader::errc::corruption";
            break;
        case errc::shutting_down:
            name = "state_reader::errc::shutting_down";
            break;
        }
        return fmt::format_to(ctx.out(), "{}", name);
    }
};
