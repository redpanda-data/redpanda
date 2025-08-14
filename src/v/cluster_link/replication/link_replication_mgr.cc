/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cluster_link/replication/link_replication_mgr.h"

#include "base/units.h"
#include "cluster_link/logger.h"
#include "utils/to_string.h"

using namespace std::chrono_literals;

namespace cluster_link::replication {

link_replication_manager::link_replication_manager(
  ss::scheduling_group sg,
  std::unique_ptr<data_source_factory> source_factory,
  std::unique_ptr<data_sink_factory> sink_factory)
  : _sg(sg)
  , _source_factory(std::move(source_factory))
  , _sink_factory(std::move(sink_factory))
  , _queue(_sg, [](const std::exception_ptr& ex) {
      vlog(cllog.error, "unexpected error in processing notifications: {}", ex);
  }) {}

ss::future<> link_replication_manager::start() { return ss::now(); }

ss::future<> link_replication_manager::stop() {
    vlog(cllog.trace, "Stopping link replication manager");
    // to avoid further submissions to the queue.
    auto f = _gate.close();
    co_await _queue.shutdown();
    co_await std::move(f);
    // stop any pending replicators
    for (auto& [_, replicator] : _replicators) {
        co_await replicator->stop();
    }
    _replicators.clear();
    vlog(cllog.trace, "Link replication manager stopped");
}

ss::future<> link_replication_manager::do_start_replicator(
  model::ntp ntp, model::term_id term) {
    auto holder = _gate.hold();
    vlog(cllog.debug, "Starting replicator for {}", ntp);
    auto it = _replicators.find(ntp);
    vassert(
      it == _replicators.end(),
      "Replicator for {} already exists, an instance should be stopped before "
      "starting a new one",
      ntp);
    auto source = _source_factory->make_source(ntp);
    auto sink = _sink_factory->make_sink(ntp);
    auto replicator = std::make_unique<partition_replicator>(
      ntp, term, std::move(source), std::move(sink));
    auto [r_it, _] = _replicators.emplace(ntp, std::move(replicator));
    co_await r_it->second->start();
}

void link_replication_manager::start_replicator(
  model::ntp ntp, model::term_id term) {
    if (_gate.is_closed()) {
        return;
    }
    _queue.submit([this, term, ntp = std::move(ntp)]() mutable {
        return do_start_replicator(ntp, term).handle_exception(
          [this, term, ntp = std::move(ntp)](
            const std::exception_ptr& e) mutable {
              vlog(
                cllog.error,
                "Failed to start replicator for {} at term {}: {},",
                ntp,
                term,
                e);
              auto it = _replicators.find(ntp);
              if (it != _replicators.end() && !_gate.is_closed()) {
                  it->second->notify_sink_on_failure(term);
              }
          });
    });
}

ss::future<> link_replication_manager::do_stop_replicator(
  model::ntp ntp, std::optional<model::term_id> term) {
    auto holder = _gate.hold();
    vlog(cllog.debug, "Stopping replicator for {}", ntp);
    auto it = _replicators.find(ntp);
    if (it == _replicators.end() || (term && (*term < it->second->term()))) {
        vlog(
          cllog.warn,
          "No replicator found for {} at term {}, skipping stop",
          ntp,
          term);
        co_return;
    }
    auto replicator = std::move(it->second);
    _replicators.erase(it);
    co_await replicator->stop();
}

void link_replication_manager::stop_replicator(
  model::ntp ntp, std::optional<model::term_id> term) {
    if (_gate.is_closed()) {
        return;
    }
    _queue.submit([this, ntp = std::move(ntp), term]() mutable {
        return do_stop_replicator(ntp, term).handle_exception(
          [ntp = std::move(ntp), term](const std::exception_ptr& e) mutable {
              vlog(
                cllog.error,
                "Failed to stop replicator for {} at term {}: {},",
                ntp,
                term,
                e);
          });
    });
}
} // namespace cluster_link::replication
