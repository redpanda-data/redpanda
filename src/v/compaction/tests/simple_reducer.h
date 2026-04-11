// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "bytes/bytes.h"
#include "compaction/filter.h"
#include "compaction/key.h"
#include "compaction/key_offset_map.h"
#include "compaction/reducer.h"
#include "compaction/utils.h"
#include "container/chunked_circular_buffer.h"
#include "model/fundamental.h"

#include <seastar/core/coroutine.hh>

namespace compaction {

// A simple filter which persists the latest key-value pair from the built
// key_offset_map.
class simple_map_filter : public filter {
public:
    simple_map_filter(
      sliding_window_reducer::sink& sink,
      const key_offset_map& map,
      model::ntp ntp)
      : filter(sink, std::move(ntp))
      , _map(map) {}

private:
    ss::future<> maybe_index_offset_delta(
      const model::record_batch& b,
      const model::record& r,
      std::vector<int32_t>& offset_deltas) const {
        if (co_await is_latest_record_for_key(_map, b, r)) {
            offset_deltas.push_back(r.offset_delta());
        }
    }

    ss::future<std::vector<int32_t>>
    compute_offset_deltas_to_keep(const model::record_batch& b) const final {
        std::vector<int32_t> offset_deltas;
        offset_deltas.reserve(b.record_count());

        co_await b.for_each_record_async(
          [this, &b, &offset_deltas](const model::record& r) {
              return maybe_index_offset_delta(b, r, offset_deltas);
          });

        co_return offset_deltas;
    }

    ss::future<std::optional<model::record_batch>>
    filter_batch_with_offset_deltas(
      model::record_batch b, std::vector<int32_t> offset_deltas) const final {
        co_return co_await do_filter_batch(
          std::move(b), std::move(offset_deltas));
    }

    const key_offset_map& _map;
};

// A simple sink which pushes filtered batches to a container.
class simple_sink : public sliding_window_reducer::sink {
public:
    simple_sink(chunked_circular_buffer<model::record_batch>& output_batches)
      : _output_batches(output_batches) {}

    ss::future<bool> initialize(sliding_window_reducer::source&) final {
        co_return true;
    }

    ss::future<ss::stop_iteration>
    operator()(model::record_batch b, model::compression) final {
        _output_batches.push_back(std::move(b));
        co_return ss::stop_iteration::no;
    }

    ss::future<> finalize(bool /*success*/) final { co_return; }

    ss::future<> prepare_iteration(kafka::offset) final { co_return; }
    ss::future<> finish_iteration(kafka::offset, kafka::offset) final {
        co_return;
    }

    chunked_circular_buffer<model::record_batch>& _output_batches;
};

// A simple source which iterates over a container of input batches, indexing
// key-value pairs in the offset map and then uses the built key-offset map and
// simple_map_filter in the forward pass.
class simple_source : public sliding_window_reducer::source {
public:
    using container_t = chunked_circular_buffer<model::record_batch>;
    simple_source(container_t batches, model::ntp ntp)
      : _batches(std::move(batches))
      , _ntp(std::move(ntp)) {}

    ss::future<> initialize() final {
        _b_it = _batches.rbegin();
        _f_it = _batches.begin();
        co_return;
    }

    ss::future<ss::stop_iteration> map_building_iteration() final {
        if (_b_it == _batches.rend()) {
            co_return ss::stop_iteration::yes;
        }

        auto& b = *_b_it;
        co_await b.for_each_record_async([this, &b](const model::record& r) {
            auto key = compaction_key{iobuf_to_bytes(r.key())};
            auto o = b.base_offset() + model::offset_delta(r.offset_delta());
            return _map.put(key, o);
        });

        ++_b_it;

        co_return ss::stop_iteration::no;
    }

    ss::future<ss::stop_iteration>
    deduplication_iteration(sliding_window_reducer::sink& s) final {
        if (_f_it == _batches.end()) {
            co_return ss::stop_iteration::yes;
        }

        auto& b = *_f_it;
        auto filter = simple_map_filter(s, _map, _ntp);
        auto ret = co_await filter(std::move(b));
        ++_f_it;
        co_return ret;
    }

private:
    container_t::reverse_iterator _b_it;
    container_t::iterator _f_it;
    container_t _batches;
    simple_key_offset_map _map;
    model::ntp _ntp;
};

} // namespace compaction
