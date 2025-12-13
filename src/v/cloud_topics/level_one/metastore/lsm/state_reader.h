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

namespace cloud_topics::l1 {

// Encapsulate queries that operate on state in a database.
class state_reader {
public:
    enum class errc {
        io_error,
        corruption,
        shutting_down,
    };

    struct extent_row {
        ss::sstring key;
        extent_row_value val;
    };
    class extent_key_range {
    public:
        extent_key_range(ss::sstring base, ss::sstring last, lsm::iterator it)
          : _base_key(std::move(base))
          , _last_key(std::move(last))
          , _iter(std::move(it)) {}

        // Returns extent_rows matching exactly between _base_key and
        // _last_key, or generates an error if it can't.
        //
        // Stops generating after the first error.
        ss::coroutine::experimental::generator<std::expected<extent_row, errc>>
        get_rows();

        ss::future<chunked_vector<std::expected<extent_row, errc>>>
        materialize_rows();

    private:
        ss::sstring _base_key;
        ss::sstring _last_key;

        // Snapshot of the database.
        lsm::iterator _iter;
    };

    explicit state_reader(lsm::snapshot snap)
      : snap_(std::move(snap)) {}

    ss::future<std::expected<std::optional<metadata_row_value>, errc>>
    get_metadata(const model::topic_id_partition&);

    ss::future<std::expected<std::optional<compaction_state>, errc>>
    get_compaction_metadata(const model::topic_id_partition&);

    ss::future<std::expected<std::optional<object_entry>, errc>>
      get_object(object_id);

    // Returns the highest term start for the given partition, or nullopt if
    // the partition does not exist.
    ss::future<std::expected<std::optional<term_start>, errc>>
    get_max_term(const model::topic_id_partition&);

    // Returns the first extent that contains an offset at or equal to the
    // given offset. If no such extent exists (e.g. all extents are below the
    // offset) returns nullopt.
    ss::future<std::expected<std::optional<extent>, errc>>
    get_extent_ge(const model::topic_id_partition&, kafka::offset);

    // Returns the key ranges whose first extent's base offset matches `base`
    // and whose last extent's last offset matches `last`. If no such range
    // exists, return nullopt.
    ss::future<std::expected<std::optional<extent_key_range>, errc>>
    get_extent_range(
      const model::topic_id_partition&, kafka::offset base, kafka::offset last);

private:
    template<typename KeyT, typename ValT, typename... KeyEncodeArgs>
    ss::future<std::expected<std::optional<ValT>, errc>>
    get_val(KeyEncodeArgs...);

    lsm::snapshot snap_;
};

} // namespace cloud_topics::l1
