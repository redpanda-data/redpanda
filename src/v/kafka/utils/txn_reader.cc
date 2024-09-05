/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "txn_reader.h"

#include "kafka/server/partition_proxy.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/timeout_clock.h"

#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/future.hh>
#include <seastar/core/loop.hh>
#include <seastar/util/variant_utils.hh>

#include <absl/container/btree_set.h>

#include <functional>
#include <memory>
#include <optional>
#include <ostream>
#include <vector>

namespace kafka {

namespace {

class aborted_transaction_tracker_impl : public aborted_transaction_tracker {
public:
    aborted_transaction_tracker_impl(
      kafka::partition_proxy* partition,
      ss::lw_shared_ptr<const storage::offset_translator_state> translator)
      : _partition(partition)
      , _translator(std::move(translator)) {}

    ss::future<std::vector<model::tx_range>>
    compute_aborted_transactions(model::offset base, model::offset max) final {
        return _partition->aborted_transactions(base, max, _translator);
    }

private:
    kafka::partition_proxy* _partition;
    ss::lw_shared_ptr<const storage::offset_translator_state> _translator;
};

} // namespace

std::unique_ptr<aborted_transaction_tracker>
aborted_transaction_tracker::create_default(
  kafka::partition_proxy* proxy,
  ss::lw_shared_ptr<const storage::offset_translator_state> translator) {
    return std::make_unique<aborted_transaction_tracker_impl>(
      proxy, std::move(translator));
}

namespace {

const model::record_batch_reader::data_t&
readonly_data_view(const model::record_batch_reader::storage_t& batches) {
    using data_t = model::record_batch_reader::data_t;
    using foreign_data_t = model::record_batch_reader::foreign_data_t;
    return ss::visit(
      batches,
      [](const data_t& data) { return std::ref(data); },
      [](const foreign_data_t& data) -> std::reference_wrapper<const data_t> {
          return std::ref(*data.buffer);
      });
}

bool contains_control_or_txn_batch(
  const model::record_batch_reader::storage_t& batches) {
    for (const model::record_batch& batch : readonly_data_view(batches)) {
        model::record_batch_attributes attrs = batch.header().attrs;
        if (attrs.is_control() || attrs.is_transactional()) {
            return true;
        }
    }
    return false;
}

struct drain_result {
    std::optional<std::pair<model::offset, model::offset>> txn_range;
    ss::chunked_fifo<model::record_batch> batches;
};

class drainer {
public:
    explicit drainer(model::record_batch_reader::storage_t initial) {
        // TODO(perf): This initial slice is iterated over twice, once to look
        // for control/transactional batches, and another time here.
        //
        // It's possible to combine the passes at the cost of more complex code.
        // If we do tackle this optimization one should ensure that the pass
        // through of the originally loaded slices if there are no control/txn
        // batches does not copy the batches over to a new buffer if it's not
        // needed.
        ss::visit(
          initial,
          [this](model::record_batch_reader::data_t& d) {
              for (auto& batch : d) {
                  process(std::move(batch));
              }
          },
          [this](model::record_batch_reader::foreign_data_t& d) {
              for (const auto& batch : *d.buffer) {
                  process(batch.copy());
              }
          });
    }

    ss::future<ss::stop_iteration> operator()(model::record_batch batch) {
        process(std::move(batch));
        return ss::make_ready_future<ss::stop_iteration>(
          ss::stop_iteration::no);
    }

    drain_result end_of_stream() { return std::move(_result); }

private:
    void process(model::record_batch batch) {
        model::record_batch_attributes attrs = batch.header().attrs;
        // It's possible that there are only control batches, which then we
        // don't have to filter out any aborts.
        if (attrs.is_transactional()) {
            if (!_result.txn_range) {
                _result.txn_range.emplace(
                  batch.base_offset(), batch.last_offset());
            }
            _result.txn_range->second = batch.last_offset();
        }
        _result.batches.push_back(std::move(batch));
    }

    drain_result _result;
};

bool is_aborted(
  const model::record_batch& batch,
  const absl::btree_set<model::tx_range, std::greater<>>& aborted_txns) {
    model::producer_identity pid = {
      batch.header().producer_id, batch.header().producer_epoch};
    auto it = aborted_txns.lower_bound(
      {pid, batch.base_offset(), model::offset::max()});

    if (it == aborted_txns.end()) {
        return false;
    }
    // If the pids match, and the batch lies within the abort range, then this
    // batch is aborted.
    return it->pid == pid && batch.base_offset() >= it->first
           && batch.last_offset() <= it->last;
}

std::unique_ptr<model::record_batch_reader::impl> make_txn_filtered_reader(
  ss::chunked_fifo<model::record_batch> batches,
  const absl::btree_set<model::tx_range, std::greater<>>& aborted_txns) {
    ss::chunked_fifo<model::record_batch> filtered;
    for (auto& batch : batches) {
        auto attrs = batch.header().attrs;
        if (attrs.is_transactional() && is_aborted(batch, aborted_txns)) {
            continue;
        }
        if (attrs.is_control()) {
            continue;
        }
        filtered.push_back(std::move(batch));
    }
    return model::make_fragmented_memory_record_batch_reader(
             std::move(filtered))
      .release();
}

} // namespace

read_committed_reader::read_committed_reader(
  std::unique_ptr<aborted_transaction_tracker> tracker,
  model::record_batch_reader reader)
  : _tracker(std::move(tracker))
  , _underlying(std::move(reader).release()) {}

void read_committed_reader::print(std::ostream& os) {
    os << "transform::txn_reader{}";
}

bool read_committed_reader::is_end_of_stream() const {
    return _underlying->is_end_of_stream();
}

ss::future<model::record_batch_reader::storage_t>
read_committed_reader::do_load_slice(
  model::timeout_clock::time_point deadline) {
    model::record_batch_reader::storage_t loaded
      = co_await _underlying->do_load_slice(deadline);
    // We delete the tracker when we've filtered the stream already.
    if (!_tracker || !contains_control_or_txn_batch(loaded)) {
        co_return loaded;
    }
    auto [txn_offset_range, batches] = co_await _underlying->consume(
      drainer(std::move(loaded)), deadline);
    absl::btree_set<model::tx_range, std::greater<>> aborted_txn_markers;
    if (txn_offset_range) {
        auto [base_offset, last_offset] = txn_offset_range.value();
        auto txn_ranges = co_await _tracker->compute_aborted_transactions(
          base_offset, last_offset);
        aborted_txn_markers.insert(txn_ranges.begin(), txn_ranges.end());
    }
    // Mark stream as filtered by deleting the tracker.
    _tracker = nullptr;
    _underlying = make_txn_filtered_reader(
      std::move(batches), aborted_txn_markers);
    co_return co_await _underlying->do_load_slice(deadline);
}

} // namespace kafka
