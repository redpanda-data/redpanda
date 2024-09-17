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
#include "transform_manager.h"

#include "base/vassert.h"
#include "base/vlog.h"
#include "logger.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/transform.h"
#include "rpc/backoff_policy.h"
#include "transform_processor.h"
#include "utils/human.h"

#include <seastar/core/future.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/when_all.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/util/log.hh>

#include <absl/algorithm/container.h>
#include <absl/container/btree_map.h>
#include <absl/container/btree_set.h>

#include <algorithm>
#include <chrono>
#include <memory>
#include <utility>

namespace transform {

using state = model::transform_report::processor::state;
using namespace std::chrono_literals;

namespace {
template<typename ClockType>
class processor_backoff {
public:
    static constexpr std::chrono::seconds base_duration = 1s;
    static constexpr std::chrono::seconds max_duration = 30s;
    // If a processor has been running for this long we mark it as good.
    static constexpr std::chrono::seconds reset_duration = 10s;

    // Mark that we attempted to start a processor, this is used to ensure that
    // we properly reset backoff if the processor has been running for long
    // enough.
    void mark_start_attempt() { _start_time = ClockType::now(); }

    ClockType::duration next_backoff_duration() {
        auto now = ClockType::now();
        if (now > (_start_time + reset_duration)) {
            _backoff.reset();
        }
        _backoff.next_backoff();
        return _backoff.current_backoff_duration();
    }

private:
    // the last time we started this processor
    ClockType::time_point _start_time = ClockType::now();
    // the backoff policy for this processor for when we attempt to restart
    // the processor. If it's been enough time since our last restart of the
    // processor we will reset this.
    ::rpc::backoff_policy _backoff
      = ::rpc::make_exponential_backoff_policy<ClockType>(
        base_duration, max_duration);
};

// The lexicographically smallest ntp
// NOLINTNEXTLINE(cert-err58-cpp)
static const model::ntp min_ntp = model::ntp(
  model::ns(""), model::topic(""), model::partition_id::min());

} // namespace

// The underlying container for holding processors and indexing them correctly.
//
// It's possible for there to be the same ntp with multiple transforms and it's
// also possible for a transform to be attached to multiple ntps, so the true
// "key" for a processor is the tuple of (id, ntp), hence the need for the
// complicated table of indexes.
template<typename ClockType>
class processor_table {
public:
    class entry_t {
    public:
        entry_t(
          std::unique_ptr<transform::processor> processor,
          ss::lw_shared_ptr<transform::probe> probe)
          : _processor(std::move(processor))
          , _probe(std::move(probe)) {
            _probe->state_change({.to = _current_state});
        }
        entry_t(const entry_t&) = delete;
        entry_t& operator=(const entry_t&) = delete;
        entry_t(entry_t&& e) noexcept = default;
        entry_t& operator=(entry_t&& e) noexcept = default;
        ~entry_t() {
            // _probe could be moved out
            if (_probe) {
                _probe->state_change({.from = _current_state});
            }
        }

        transform::processor* processor() const { return _processor.get(); }
        const ss::lw_shared_ptr<transform::probe>& probe() const {
            return _probe;
        }

        ClockType::duration next_backoff_duration() {
            _probe->state_change(
              {.from = _current_state, .to = state::errored});
            _current_state = state::errored;
            return _backoff.next_backoff_duration();
        }
        void mark_start_attempt() {
            _probe->state_change(
              {.from = _current_state, .to = state::inactive});
            _current_state = state::inactive;
            _backoff.mark_start_attempt();
        }
        void mark_running() {
            _probe->state_change(
              {.from = _current_state, .to = state::running});
            _current_state = state::running;
        }
        state current_state() const { return _current_state; }

    private:
        std::unique_ptr<transform::processor> _processor;
        ss::lw_shared_ptr<transform::probe> _probe;
        processor_backoff<ClockType> _backoff;
        state _current_state = state::inactive;
    };

    auto range() const { return std::make_pair(_table.begin(), _table.end()); }

    entry_t&
    insert(std::unique_ptr<processor> processor, ss::lw_shared_ptr<probe> p) {
        auto id = processor->id();
        auto ntp = processor->ntp();
        auto [table_it, table_inserted] = _table.emplace(
          std::make_pair(id, ntp), entry_t(std::move(processor), std::move(p)));
        vassert(table_inserted, "invalid transform processor management");
        auto [index_it, index_inserted] = _ntp_index.emplace(
          std::make_pair(ntp, id));
        vassert(index_inserted, "invalid transform processor management");
        return table_it->second;
    }

    ss::lw_shared_ptr<probe> get_or_create_probe(
      model::transform_id id, const model::transform_metadata& meta) {
        auto it = _table.lower_bound(std::make_pair(id, min_ntp));
        if (it != _table.end() && it->first.first == id) {
            return it->second.probe();
        }
        auto probe = ss::make_lw_shared<transform::probe>();
        probe->setup_metrics(meta);
        return probe;
    }

    bool contains(model::transform_id id, const model::ntp& ntp) {
        return _table.contains(std::make_pair(id, ntp));
    }

    entry_t* get_or_null(model::transform_id id, const model::ntp& ntp) {
        auto it = _table.find(std::make_pair(id, ntp));
        if (it == _table.end()) {
            return nullptr;
        }
        return std::addressof(it->second);
    }

    ss::future<> clear() {
        co_await ss::parallel_for_each(
          _table.begin(),
          _table.end(),
          // NOLINTNEXTLINE(cppcoreguidelines-avoid-reference-coroutine-parameters)
          [](auto& e) { return e.second.processor()->stop(); });
        _table.clear();
        _ntp_index.clear();
    }

    ss::future<> erase_by_id(model::transform_id target_id) {
        // Take advantage that the _table is sorted by id, then ntp.
        // we're looking for a range of ids in _table.
        auto it = _table.lower_bound(std::make_pair(target_id, min_ntp));
        // copy the iterator so that we can erase the whole ID range.
        auto begin = it;
        ss::chunked_fifo<ss::future<>> stop_futures;
        while (it != _table.end()) {
            auto [id, ntp] = it->first;
            if (id != target_id) {
                break;
            }
            stop_futures.push_back(it->second.processor()->stop());
            vassert(
              _ntp_index.erase(std::make_pair(ntp, id)) > 0,
              "inconsistent index");
            ++it;
            co_await ss::coroutine::maybe_yield();
        }
        co_await ss::when_all(stop_futures.begin(), stop_futures.end());
        _table.erase(begin, it);
    }

    // Clear our all the transforms with a given ntp and return all the IDs that
    // no longer exist.
    ss::future<> erase_by_ntp(model::ntp target_ntp) {
        auto it = _ntp_index.lower_bound(
          std::pair(target_ntp, model::transform_id::min()));
        ss::chunked_fifo<entry_t> entries;
        ss::chunked_fifo<ss::future<>> stop_futures;
        while (it != _ntp_index.end()) {
            auto [ntp, id] = *it;
            if (ntp != target_ntp) {
                break;
            }
            auto node = _table.extract(std::make_pair(id, ntp));
            stop_futures.push_back(node.mapped().processor()->stop());
            // delay destroying the processor until after it's stopped.
            entries.push_back(std::move(node.mapped()));
            it = _ntp_index.erase(it);
            co_await ss::coroutine::maybe_yield();
        }
        co_await ss::when_all(stop_futures.begin(), stop_futures.end());
    }

private:
    // We take advantage of these containers being sorted to perform the correct
    // queries.
    absl::btree_map<std::pair<model::transform_id, model::ntp>, entry_t> _table;
    absl::btree_set<std::pair<model::ntp, model::transform_id>> _ntp_index;
};

template<typename ClockType>
manager<ClockType>::manager(
  model::node_id self,
  std::unique_ptr<registry> r,
  std::unique_ptr<processor_factory> f,
  ss::scheduling_group sg,
  std::unique_ptr<memory_limits> memory_limits)
  : _self(self)
  , _queue(
      sg,
      [](const std::exception_ptr& ex) {
          vlog(tlog.error, "unexpected transform manager error: {}", ex);
      })
  , _memory_limits(std::move(memory_limits))
  , _registry(std::move(r))
  , _processors(std::make_unique<processor_table<ClockType>>())
  , _processor_factory(std::move(f)) {}

template<typename ClockType>
manager<ClockType>::~manager() = default;

template<typename ClockType>
ss::future<> manager<ClockType>::start() {
    return ss::now();
}

template<typename ClockType>
ss::future<> manager<ClockType>::stop() {
    vlog(tlog.info, "Stopping transform manager...");
    co_await _queue.shutdown();
    co_await _processors->clear();
    vlog(tlog.info, "Stopped transform manager.");
}

template<typename ClockType>
void manager<ClockType>::on_leadership_change(
  model::ntp ntp, ntp_leader leader_status) {
    _queue.submit([this, ntp = std::move(ntp), leader_status]() mutable {
        return handle_leadership_change(std::move(ntp), leader_status);
    });
}

template<typename ClockType>
void manager<ClockType>::on_plugin_change(model::transform_id id) {
    _queue.submit([this, id] { return handle_plugin_change(id); });
}

template<typename ClockType>
void manager<ClockType>::on_transform_state_change(
  model::transform_id id, model::ntp ntp, processor::state state) {
    _queue.submit([this, id, ntp = std::move(ntp), state]() mutable {
        switch (state) {
        case processor::state::running:
            return handle_transform_running(id, ntp);
        case processor::state::errored:
            return handle_transform_error(id, ntp);
        case processor::state::inactive:
            // We don't expect inactive changes, as processors should never
            // become inactive on their own, but only as apart of the startup
            // sequence.
        case processor::state::unknown:
            vassert(false, "unexpected transform state change: {}", state);
        }
    });
}

template<typename ClockType>
ss::future<> manager<ClockType>::handle_leadership_change(
  model::ntp ntp, ntp_leader leader_status) {
    vlog(
      tlog.debug,
      "handling leadership status change to leader={} for: {}",
      leader_status,
      ntp);

    if (leader_status == ntp_leader::no) {
        // We're not the leader anymore, time to shutdown all the processors
        co_await _processors->erase_by_ntp(ntp);
        co_return;
    }
    // We're the leader - start all the processor that aren't already running
    auto transforms = _registry->lookup_by_input_topic(
      model::topic_namespace_view(ntp));
    for (model::transform_id id : transforms) {
        co_await start_processor(ntp, id);
    }
}

template<typename ClockType>
ss::future<> manager<ClockType>::handle_plugin_change(model::transform_id id) {
    vlog(tlog.debug, "handling update to plugin: {}", id);
    // If we have an existing processor we need to restart it with the updates
    // applied.
    co_await _processors->erase_by_id(id);

    auto transform = _registry->lookup_by_id(id);
    // If there is no transform OR the transform is paused, we're good to go,
    // everything is shutdown if needed.
    //
    // Note that this has implications for how we model processor state in the
    // cluster-wide transform report.
    // see `transform::service::compute_default_report` for detail.
    if (!transform || transform->paused) {
        co_return;
    }

    // Otherwise, start a processor for every partition we're a leader of.
    auto partitions = _registry->get_leader_partitions(transform->input_topic);
    for (model::partition_id partition : partitions) {
        auto ntp = model::ntp(
          transform->input_topic.ns, transform->input_topic.tp, partition);
        // It's safe to directly create processors, because we deleted them
        // for a full restart from this deploy
        co_await create_processor(std::move(ntp), id, *transform);
    }
}

template<typename ClockType>
ss::future<> manager<ClockType>::handle_transform_error(
  model::transform_id id, model::ntp ntp) {
    auto* entry = _processors->get_or_null(id, ntp);
    if (!entry) {
        co_return;
    }
    entry->probe()->increment_failure();
    auto delay = entry->next_backoff_duration();
    co_await entry->processor()->stop();
    vlog(
      tlog.info,
      "transform {} errored on partition {}, delaying for {} then restarting",
      entry->processor()->meta().name,
      ntp.tp.partition,
      human::latency(delay));
    _queue.submit_delayed<ClockType>(delay, [this, id, ntp]() mutable {
        return start_processor(std::move(ntp), id);
    });
}

template<typename ClockType>
ss::future<> manager<ClockType>::handle_transform_running(
  model::transform_id id, model::ntp ntp) {
    auto* entry = _processors->get_or_null(id, ntp);
    if (!entry) {
        co_return;
    }
    entry->mark_running();
}

template<typename ClockType>
ss::future<>
manager<ClockType>::start_processor(model::ntp ntp, model::transform_id id) {
    auto* entry = _processors->get_or_null(id, ntp);
    // It's possible something else came along and kicked this processor and
    // that worked.
    if (entry && entry->processor()->is_running()) {
        co_return;
    }
    auto transform = _registry->lookup_by_id(id);
    // This transform was deleted, if there is an entry, it *should* have a
    // pending delete notification enqueued (if there is an existing entry).
    if (!transform) {
        co_return;
    }
    auto leaders = _registry->get_leader_partitions(
      model::topic_namespace_view(ntp));
    // no longer a leader for this partition, if there was an entry, it
    // *should* have a pending no_leader notification enqueued (that will
    // cleanup the existing entry).
    if (!leaders.contains(ntp.tp.partition)) {
        co_return;
    }
    if (entry) {
        entry->mark_start_attempt();
        co_await entry->processor()->start();
    } else {
        co_await create_processor(ntp, id, *std::move(transform));
    }
}

template<typename ClockType>
ss::future<> manager<ClockType>::create_processor(
  model::ntp ntp, model::transform_id id, model::transform_metadata meta) {
    auto p = _processors->get_or_create_probe(id, meta);
    processor::state_callback cb =
      [this](model::transform_id id, model::ntp ntp, processor::state state) {
          on_transform_state_change(id, ntp, state);
      };
    auto fut = co_await ss::coroutine::as_future(
      _processor_factory->create_processor(
        id, ntp, meta, std::move(cb), p.get(), _memory_limits.get()));
    if (fut.failed()) {
        vlog(
          tlog.warn,
          "failed to create transform processor {}: {}, retrying...",
          meta.name,
          fut.get_exception());
        // Delay some time before attempting to recreate a processor
        // TODO: Should we have more sophisticated backoff mechanisms?
        constexpr auto recreate_attempt_delay = 30s;
        _queue.submit_delayed<ClockType>(
          recreate_attempt_delay, [this, ntp = std::move(ntp), id]() mutable {
              return start_processor(std::move(ntp), id);
          });
    } else {
        // Ensure that we insert this transform into our mapping before we
        // start it, so that if start fails and calls the error callback
        // we properly know that it's in flight.
        auto& entry = _processors->insert(std::move(fut).get(), std::move(p));
        vlog(tlog.info, "starting transform {} on {}", meta.name, ntp);
        entry.mark_start_attempt();
        co_await entry.processor()->start();
    }
}

template<typename ClockType>
ss::future<> manager<ClockType>::drain_queue_for_test() {
    ss::promise<> p;
    auto f = p.get_future();
    // Move the promise into the queue so we get
    // broken promise exceptions if the queue is shutdown.
    _queue.submit([p = std::move(p)]() mutable {
        p.set_value();
        return ss::now();
    });
    co_await std::move(f);
}

template<typename ClockType>
model::cluster_transform_report manager<ClockType>::compute_report() const {
    model::cluster_transform_report report;
    for (auto [it, end] = _processors->range(); it != end; ++it) {
        const auto& entry = it->second;
        processor* p = entry.processor();
        auto id = p->ntp().tp.partition;
        report.add(
          p->id(),
          p->meta(),
          {
            .id = id,
            .status = entry.current_state(),
            .node = _self,
            .lag = p->current_lag(),
          });
    }
    return report;
}

template class manager<ss::lowres_clock>;
template class manager<ss::manual_clock>;

} // namespace transform
