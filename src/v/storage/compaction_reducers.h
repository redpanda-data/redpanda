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

#include "base/units.h"
#include "bytes/bytes.h"
#include "container/fragmented_vector.h"
#include "hashing/xx.h"
#include "model/record_batch_reader.h"
#include "storage/compacted_index.h"
#include "storage/compacted_index_writer.h"
#include "storage/compacted_offset_list.h"
#include "storage/fwd.h"
#include "storage/index_state.h"
#include "storage/logger.h"
#include "utils/tracking_allocator.h"

#include <seastar/util/noncopyable_function.hh>

#include <absl/container/btree_map.h>
#include <fmt/core.h>
#include <roaring/roaring.hh>

#include <queue>

namespace storage::internal {

struct compaction_reducer {};

class compaction_key_reducer : public compaction_reducer {
public:
    static constexpr const size_t default_max_memory_usage = 5_MiB;
    struct value_type {
        value_type(bytes k, model::offset o, uint32_t i)
          : key(std::move(k))
          , offset(o)
          , natural_index(i) {}
        bytes key;
        model::offset offset;
        uint32_t natural_index;
    };
    using underlying_t = absl::btree_multimap<
      uint64_t,
      value_type,
      std::less<>,
      util::tracking_allocator<value_type>>;

    explicit compaction_key_reducer(size_t max_mem = default_max_memory_usage)
      : _max_mem(max_mem)
      , _memory_tracker(
          ss::make_shared<util::mem_tracker>("compaction_key_reducer_index"))
      , _indices{util::tracking_allocator<value_type>{_memory_tracker}} {}

    ss::future<ss::stop_iteration> operator()(compacted_index::entry&&);
    roaring::Roaring end_of_stream();
    size_t idx_mem_usage() { return _memory_tracker->consumption(); }

private:
    size_t _keys_mem_usage{0};
    size_t _max_mem{0};
    uint32_t _natural_index{0};

    roaring::Roaring _inverted;

    ss::shared_ptr<util::mem_tracker> _memory_tracker;
    underlying_t _indices;
    bytes_hasher<uint64_t, xxhash_64> _hasher;
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
    using filter_t = ss::noncopyable_function<ss::future<bool>(
      const model::record_batch&, const model::record&, bool)>;
    copy_data_segment_reducer(
      filter_t f,
      segment_appender* a,
      bool internal_topic,
      offset_delta_time apply_offset,
      model::offset segment_last_offset,
      compacted_index_writer* cidx = nullptr,
      bool inject_failure = false,
      ss::abort_source* as = nullptr)
      : _should_keep_fn(std::move(f))
      , _segment_last_offset(segment_last_offset)
      , _appender(a)
      , _compacted_idx(cidx)
      , _idx(index_state::make_empty_index(apply_offset))
      , _internal_topic(internal_topic)
      , _inject_failure(inject_failure)
      , _as(as) {}

    ss::future<ss::stop_iteration> operator()(model::record_batch);
    storage::index_state end_of_stream() { return std::move(_idx); }

private:
    ss::future<ss::stop_iteration>
      filter_and_append(model::compression, model::record_batch);

    ss::future<> maybe_keep_offset(
      const model::record_batch&,
      const model::record&,
      bool,
      std::vector<int32_t>&);

    ss::future<std::optional<model::record_batch>> filter(model::record_batch);

    // Creates a placeholder batch with same offset range as the input header.
    model::record_batch make_placeholder_batch(model::record_batch_header&);

    filter_t _should_keep_fn;

    // Offset to keep in case the index is empty as of getting to this offset.
    model::offset _segment_last_offset;
    segment_appender* _appender;

    // Compacted index writer for the newly written segment. May not be
    // supplied if the compacted index isn't expected to change, e.g. when
    // rewriting a single segment filtering with its own compacted index.
    compacted_index_writer* _compacted_idx;
    index_state _idx;
    size_t _acc{0};

    /// We need to know if this is an internal topic to inform whether to
    /// index on non-raft-data batches
    bool _internal_topic;

    /// If set to true, will throw an exception on operator().
    bool _inject_failure;

    /// Allows the reducer to stop early, e.g. in case the partition is being
    /// shut down.
    ss::abort_source* _as;
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
 * Filters out the aborted transaction data batches from the segment.
 * Retains the fence and control batches (commit/abort) thus preserving the
 * transaction boundaries.
 *
 * The implementation wraps an index_rebuilder_reducer and filters out
 * the aforementioned batches before delegating them to compact.
 * Bookkeeps an ongoing list of aborted transactions up until the
 * batch we consumed. We keep adding/removing from this list as we consume
 * the control metadata batches that signal begin/end of transactions.
 *
 * Few higher level invariants this implementation assumes to be true.
 *  - We only compact a segment if its offset range is within LSO boundary. This
 *    guarantees that any batch we see is either committed/aborted (eventually,
 *    may not be within this segment boundary).
 *  - Aborted tx ranges from the stm are the source of truth. Particularly for
 *    transactions spanning multiple segments (where begin/end or both may not
 *    be in the current segment).
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
      , _transactional_stm_type(stm_mgr->transactional_stm_type()) {
        _stats.num_aborted_txes = _aborted_txs.size();
    }
    ss::future<ss::stop_iteration> operator()(model::record_batch&&);

    struct stats {
        size_t batches_discarded{0};
        size_t num_aborted_txes{0};
        size_t batches_processed{0};

        friend std::ostream& operator<<(std::ostream& os, const stats& s) {
            fmt::print(
              os,
              "{{ batches processed: {}, aborted_txs: {}, "
              "discarded batches: {} }}",
              s.batches_processed,
              s.num_aborted_txes,
              s.batches_discarded);
            return os;
        }
    };

    stats end_of_stream() { return _stats; }

private:
    bool can_discard_tx_data_batch(const model::record_batch&);
    bool can_discard_consumer_offsets_batch(const model::record_batch&);
    // Refreshes the running list of aborted transactions
    // based on the encountered input batch. New ongoing aborted
    // transaction (if any) may be added and finished aborted
    // transactions are removed if an abort batch is encountered.
    void refresh_ongoing_aborted_txs(const model::record_batch&);

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
    // Set if a transactional stm is attached to this partition.
    std::optional<storage::stm_type> _transactional_stm_type;
};

} // namespace storage::internal
