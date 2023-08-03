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
#include "storage/fwd.h"
#include "storage/index_state.h"
#include "storage/logger.h"
#include "units.h"
#include "utils/fragmented_vector.h"

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
    roaring::Roaring end_of_stream();

private:
    size_t idx_mem_usage() {
        using debug = absl::container_internal::hashtable_debug_internal::
          HashtableDebugAccess<underlying_t>;
        return debug::AllocatedByteSize(_indices);
    }
    roaring::Roaring _inverted;
    underlying_t _indices;
    size_t _keys_mem_usage{0};
    size_t _max_mem{0};
    uint32_t _natural_index{0};
};

/// This class copies the input reader into the writer consulting the bitmap of
/// wether ot keep the entry or not
class index_filtered_copy_reducer : public compaction_reducer {
public:
    index_filtered_copy_reducer(roaring::Roaring b, compacted_index_writer& w)
      : _bm(std::move(b))
      , _writer(&w) {}

    ss::future<ss::stop_iteration> operator()(compacted_index::entry&&);
    void end_of_stream() {}

private:
    uint32_t _natural_index = 0;
    roaring::Roaring _bm;
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
      : _list(base, roaring::Roaring{}) {}

    ss::future<ss::stop_iteration> operator()(compacted_index::entry&&);
    compacted_offset_list end_of_stream() { return std::move(_list); }

private:
    compacted_offset_list _list;
};

class copy_data_segment_reducer : public compaction_reducer {
public:
    copy_data_segment_reducer(
      compacted_offset_list l,
      segment_appender* a,
      bool internal_topic,
      offset_delta_time apply_offset)
      : _list(std::move(l))
      , _appender(a)
      , _idx(index_state::make_empty_index(apply_offset))
      , _internal_topic(internal_topic) {}

    ss::future<ss::stop_iteration> operator()(model::record_batch);
    storage::index_state end_of_stream() { return std::move(_idx); }

private:
    ss::future<ss::stop_iteration>
      do_compaction(model::compression, model::record_batch);

    bool should_keep(model::offset base, int32_t delta) const {
        const auto o = base + model::offset(delta);
        return _list.contains(o);
    }
    std::optional<model::record_batch> filter(model::record_batch&&);

    compacted_offset_list _list;
    segment_appender* _appender;
    index_state _idx;
    size_t _acc{0};

    /// We need to know if this is an internal topic to inform whether to
    /// index on non-raft-data batches
    bool _internal_topic;
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

/**
 * Filters out the following record batches from compaction.
 * - Aborted transaction raft data bathes
 * - Transactional control metadata batches (commit/abort etc)
 *
 * Resulting compacted segment includes only committed transaction's
 * data without the transactional markers.
 *
 * Note: fence batches are retained to preserve the epochs from pids.
 * The state machine uses this information to preserve the monotonicity
 * of epoch and to fence older pids.

 * The implementation wraps an index_rebuilder_reducer and filters out
 * the aforementioned batches before delegating them to compact.
 * Bookkeeps an ongoing list of aborted transactions up until the
 * batch we consumed. We keep adding/removing from this list as we consume
 * the control metadata batches that signal begin/end of transactions.
 *
 * Few higher level invariants this implementation assumes to be true.
 *  - We only compact a segment if its offset range is within LSO boundary. This
 *    guarantees that any batch we see is either committed/aborted (eventually,
 may
 *    not be within this segment boundary).
 *  - Aborted tx ranges from the stm are the source of truth. Particularly for
 *    transactions spanning multiple segments (where begin/end or both may not
 be in
 *    the current segment).
 */
class tx_reducer : public compaction_reducer {
public:
    explicit tx_reducer(
      ss::lw_shared_ptr<storage::stm_manager> stm_mgr,
      fragmented_vector<model::tx_range>&& txs,
      compacted_index_writer* w) noexcept
      : _delegate(index_rebuilder_reducer(w))
      , _aborted_txs(model::tx_range_cmp(), std::move(txs))
      , _stm_mgr(stm_mgr)
      , _non_transactional(!stm_mgr->has_tx_stm()) {
        _stats._num_aborted_txes = _aborted_txs.size();
    }
    ss::future<ss::stop_iteration> operator()(model::record_batch&&);

    struct stats {
        size_t _tx_data_batches_discarded{0};
        size_t _tx_control_batches_discarded{0};
        size_t _non_tx_control_batches_discarded{0};
        size_t _all_batches_discarded{0};
        size_t _num_aborted_txes{0};
        size_t _all_batches{0};

        friend std::ostream& operator<<(std::ostream& os, const stats& s) {
            fmt::print(
              os,
              "{{ all_batches: {}, aborted_txs: {}, all "
              "discarded batches: {}, tx data batches discarded: {}, tx "
              "control batches discarded {}, non tx control batches discarded: "
              "{}}}",
              s._all_batches,
              s._num_aborted_txes,
              s._all_batches_discarded,
              s._tx_data_batches_discarded,
              s._tx_control_batches_discarded,
              s._non_tx_control_batches_discarded);
            return os;
        }
    };

    stats end_of_stream() { return _stats; }

private:
    bool handle_tx_control_batch(const model::record_batch&);
    bool handle_tx_data_batch(const model::record_batch&);
    bool handle_non_tx_control_batch(const model::record_batch&);
    void consume_aborted_txs(model::offset);

    index_rebuilder_reducer _delegate;
    // A min heap of aborted transactions based on begin offset.
    using underlying_t = std::priority_queue<
      model::tx_range,
      fragmented_vector<model::tx_range>,
      model::tx_range_cmp>;
    underlying_t _aborted_txs;
    // Current list of aborted transactions maintained up to the
    // end offset of the batch we consumed.
    absl::flat_hash_map<model::producer_identity, model::tx_range>
      _ongoing_aborted_txs;
    ss::lw_shared_ptr<storage::stm_manager> _stm_mgr;
    stats _stats;
    // Set if no transactional stm is attached to the partition of this
    // segment. This means there are no batches of interest in this segment
    // for this reducer and we short circuit the logic to directly delegate to
    // the underlying reducer. This is true for internal topics like
    // __consumer_offsets where transactional guarantees are enforced by
    // stm implementations other than the one used for data partitions.
    // Also true for partitions without any transactional stms attached.
    bool _non_transactional;
};

} // namespace storage::internal
