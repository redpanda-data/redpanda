/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "bytes/bytes.h"
#include "hashing/xx.h"
#include "model/record_batch_reader.h"
#include "storage/compacted_index.h"
#include "storage/compacted_index_writer.h"
#include "storage/compacted_offset_list.h"
#include "storage/index_state.h"
#include "storage/logger.h"
#include "storage/segment_appender.h"
#include "units.h"

#include <absl/container/btree_map.h>
#include <absl/container/node_hash_map.h>
#include <fmt/core.h>
#include <roaring/roaring.hh>

namespace storage::internal {

struct compaction_reducer {};

class compaction_key_reducer : public compaction_reducer {
public:
    static constexpr const size_t default_max_memory_usage = 5_MiB;
    struct value_type {
        value_type(model::offset o, uint32_t i)
          : offset(o)
          , natural_index(i) {}
        model::offset offset;
        uint32_t natural_index;
    };
    using underlying_t = absl::node_hash_map<
      bytes,
      value_type,
      bytes_hasher<uint64_t, xxhash_64>,
      bytes_type_eq>;

    explicit compaction_key_reducer(size_t max_mem = default_max_memory_usage)
      : _max_mem(max_mem) {}

    ss::future<ss::stop_iteration> operator()(compacted_index::entry&&);
    Roaring end_of_stream();

private:
    size_t idx_mem_usage() {
        using debug = absl::container_internal::hashtable_debug_internal::
          HashtableDebugAccess<underlying_t>;
        return debug::AllocatedByteSize(_indices);
    }
    Roaring _inverted;
    underlying_t _indices;
    size_t _keys_mem_usage{0};
    size_t _max_mem{0};
    uint32_t _natural_index{0};
};

/// This class copies the input reader into the writer consulting the bitmap of
/// wether ot keep the entry or not
class index_filtered_copy_reducer : public compaction_reducer {
public:
    index_filtered_copy_reducer(Roaring b, compacted_index_writer& w)
      : _bm(std::move(b))
      , _writer(&w) {}

    ss::future<ss::stop_iteration> operator()(compacted_index::entry&&);
    void end_of_stream() {}

private:
    uint32_t _natural_index = 0;
    Roaring _bm;
    compacted_index_writer* _writer;
};

class index_copy_reducer : public compaction_reducer {
public:
    explicit index_copy_reducer(compacted_index_writer& w)
      : _writer(&w) {}

    ss::future<ss::stop_iteration> operator()(compacted_index::entry&&);
    void end_of_stream() {}

private:
    compacted_index_writer* _writer;
};

class compacted_offset_list_reducer : public compaction_reducer {
public:
    explicit compacted_offset_list_reducer(model::offset base)
      : _list(base, Roaring{}) {}

    ss::future<ss::stop_iteration> operator()(compacted_index::entry&&);
    compacted_offset_list end_of_stream() { return std::move(_list); }

private:
    compacted_offset_list _list;
};

class copy_data_segment_reducer : public compaction_reducer {
public:
    copy_data_segment_reducer(compacted_offset_list l, segment_appender* a)
      : _list(std::move(l))
      , _appender(a) {}

    ss::future<ss::stop_iteration> operator()(model::record_batch&&);
    storage::index_state end_of_stream() { return std::move(_idx); }

private:
    ss::future<ss::stop_iteration>
    do_compaction(model::compression, model::record_batch&&);

    bool should_keep(model::offset base, int32_t delta) const {
        const auto o = base + model::offset(delta);
        return _list.contains(o);
    }
    std::optional<model::record_batch> filter(model::record_batch&&);

    compacted_offset_list _list;
    segment_appender* _appender;
    index_state _idx;
    size_t _acc{0};
};

class index_rebuilder_reducer : public compaction_reducer {
public:
    explicit index_rebuilder_reducer(compacted_index_writer* w) noexcept
      : _w(w) {}
    ss::future<ss::stop_iteration> operator()(model::record_batch&&);
    void end_of_stream() {}

private:
    ss::future<> do_index(model::record_batch&&);

    compacted_index_writer* _w;
};

} // namespace storage::internal
