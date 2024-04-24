/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "remote_wasm.h"

#include "base/vlog.h"
#include "model/record.h"
#include "ssx/future-util.h"
#include "transform/logger.h"
#include "transform/rpc/client.h"
#include "transform/worker/rpc/client.h"
#include "transform/worker/rpc/errc.h"
#include "utils/log_hist.h"
#include "wasm/api.h"
#include "wasm/transform_probe.h"

#include <seastar/core/loop.hh>
#include <seastar/core/smp.hh>

#include <exception>

namespace transform {

namespace {
class remote_engine : public wasm::engine {
public:
    remote_engine(
      std::unique_ptr<wasm::logger> logger,
      chunked_hash_map<remote_wasm_manager::vm_key, size_t>* ref_counts,
      model::transform_id id,
      model::transform_metadata meta,
      worker::rpc::client* client)
      : _logger(std::move(logger))
      , _ref_counts(ref_counts)
      , _id(id)
      , _meta(std::move(meta))
      , _client(client) {}
    remote_engine(const remote_engine&) = delete;
    remote_engine(remote_engine&&) = delete;
    remote_engine& operator=(const remote_engine&) = delete;
    remote_engine& operator=(remote_engine&&) = delete;
    ~remote_engine() override {
        ssx::background = ss::smp::submit_to(
          remote_wasm_manager::shard,
          [refs = _ref_counts, id = _id, uuid = _meta.uuid]() {
              auto it = refs->find(std::make_pair(id, uuid));
              vassert(it != refs->end(), "invalid reference counting");
              it->second -= 1;
              if (it->second == 0) {
                  refs->erase(it);
              }
          });
    }

    // TODO: consider if these should do anything on the worker node... Right
    // now they do not.
    ss::future<> start() override { co_return; }
    ss::future<> stop() override { co_return; }

    ss::future<> transform(
      model::record_batch batch,
      wasm::transform_probe* probe,
      wasm::transform_callback cb) override {
        chunked_vector<ss::foreign_ptr<std::unique_ptr<model::record_batch>>>
          batches;
        batches.reserve(1);
        batches.emplace_back(
          std::make_unique<model::record_batch>(std::move(batch)));
        auto start = log_hist_public::clock_type::now();
        auto response = co_await _client->transform(
          _id, _meta.uuid, model::partition_id{}, std::move(batches));
        if (response.has_error()) {
            probe->transform_error();
            throw std::runtime_error(ss::format(
              "error transforming records: {}",
              worker::rpc::error_category().message(
                static_cast<int>(response.error()))));
        }
        // TODO: log messages returned in the response using the logger.
        auto end = log_hist_public::clock_type::now();
        size_t count = 0;
        for (auto& data : response.value()) {
            for (auto& record : data.output) {
                // The worker validates the output topic is valid
                if (record.get_owner_shard() == ss::this_shard_id()) {
                    std::ignore = co_await cb(data.topic, std::move(*record));
                } else {
                    std::ignore = co_await cb(data.topic, record->copy());
                }
            }
            count += data.output.size();
        }
        auto avg_duration
          = std::chrono::duration_cast<log_hist_public::duration_type>(
              end - start)
              .count()
            / count;
        for (size_t i = 0; i < count; ++i) {
            probe->record_latency(avg_duration);
        }
    }

private:
    std::unique_ptr<wasm::logger> _logger;
    chunked_hash_map<remote_wasm_manager::vm_key, size_t>* _ref_counts;
    model::transform_id _id;
    model::transform_metadata _meta;
    worker::rpc::client* _client;
};

class ref_counting_factory : public wasm::factory {
public:
    ref_counting_factory(
      chunked_hash_map<remote_wasm_manager::vm_key, size_t>* ref_counts,
      model::transform_id id,
      model::transform_metadata meta,
      ss::sharded<worker::rpc::client>* client)
      : _ref_counts(ref_counts)
      , _id(id)
      , _meta(std::move(meta))
      , _client(client) {}
    ref_counting_factory(const ref_counting_factory&) = delete;
    ref_counting_factory(ref_counting_factory&&) = delete;
    ref_counting_factory& operator=(const ref_counting_factory&) = delete;
    ref_counting_factory& operator=(ref_counting_factory&&) = delete;
    ~ref_counting_factory() override {
        auto it = _ref_counts->find(std::make_pair(_id, _meta.uuid));
        vassert(it != _ref_counts->end(), "invalid reference counting");
        it->second -= 1;
        if (it->second == 0) {
            _ref_counts->erase(it);
        }
    }
    ss::future<ss::shared_ptr<wasm::engine>>
    make_engine(std::unique_ptr<wasm::logger> logger) final {
        co_await ss::smp::submit_to(
          remote_wasm_manager::shard,
          [refs = _ref_counts, id = _id, uuid = _meta.uuid] {
              auto it = refs->find(std::make_pair(id, uuid));
              vassert(it != refs->end(), "invalid reference counting");
              it->second += 1;
          });
        co_return ss::make_shared<remote_engine>(
          std::move(logger), _ref_counts, _id, _meta, &_client->local());
    }

private:
    chunked_hash_map<remote_wasm_manager::vm_key, size_t>* _ref_counts;
    model::transform_id _id;
    model::transform_metadata _meta;
    ss::sharded<worker::rpc::client>* _client;
};

} // namespace

remote_wasm_manager::remote_wasm_manager(
  rpc::client* rpc_client,
  ss::sharded<worker::rpc::client>* worker_client,
  metadata_lookup_fn meta_lookup)
  : _reconciliation_needed_event("remote_wasm_manager")
  , _rpc_client(rpc_client)
  , _worker_client(worker_client)
  , _metadata_lookup(std::move(meta_lookup)) {}

ss::future<> remote_wasm_manager::start() {
    ssx::background
      = ssx::spawn_with_gate_then(_gate, [this]() {
            vlog(tlog.info, "starting remote wasm reconciliation loop");
            return do_reconciliation();
        }).then([] {
            vlog(tlog.info, "stopping remote wasm reconciliation loop");
        });
    co_return;
}

ss::future<> remote_wasm_manager::stop() {
    vassert(
      _ref_counts.empty(),
      "expected all wasm engines to be shutdown before stopping, engines "
      "running: {}",
      _ref_counts.size());
    _reconciliation_needed_event.broken();
    _reconciliation_waiters.broadcast();
    co_await _gate.close();
    if (!_active_vms.empty()) {
        // In the case of network failures, this can happen, so just log that we
        // couldn't cleanly shutdown.
        vlog(
          tlog.error,
          "unable to stop all worker VMs before stopping remote_wasm_manager, "
          "{} still active",
          _active_vms.size());
    }
}

ss::future<ss::shared_ptr<wasm::factory>> remote_wasm_manager::make_factory(
  model::transform_id id, model::transform_metadata meta) {
    vlog(tlog.info, "attempting to make factory for transform {}", meta.name);
    auto h = _gate.hold();
    auto key = std::make_pair(id, meta.uuid);
    _ref_counts[key] += 1;
    auto factory = ss::make_shared<ref_counting_factory>(
      &_ref_counts, id, std::move(meta), _worker_client);
    if (!_active_vms.contains(key)) {
        // Wait for the VM to start remotely
        _reconciliation_needed_event.set();
        co_await _reconciliation_waiters.wait([this, key] {
            return _active_vms.contains(key) || _gate.is_closed();
        });
    }
    vlog(tlog.info, "completed making factory for transform {}", meta.name);
    co_return factory;
}

ss::future<> remote_wasm_manager::reconciliation_loop() {
    while (!_gate.is_closed()) {
        // TODO: Add jitter
        co_await _reconciliation_needed_event.wait(10s);
        try {
            co_await do_reconciliation();
        } catch (...) {
            vlog(
              tlog.error,
              "error during reconciliation loop: {}",
              std::current_exception());
        }
    }
}

ss::future<> remote_wasm_manager::do_reconciliation() {
    // Bound this loop in case of bugz
    for (int i = 0; i < 3; ++i) {
        auto result = co_await _worker_client->local().list_status();
        if (!result) {
            vlog(
              tlog.warn,
              "error listing status of worker: {}",
              worker::rpc::make_error_code(result.error()).message());
            // Backoff by waiting for the next pass of the reconciliation loop.
            co_return;
        }
        auto report = std::move(result.value());
        _active_vms.clear();
        for (const auto& status : report) {
            vlog(
              tlog.info,
              "vm {}/{} in state: {}",
              status.id,
              status.transform_version,
              int(status.state));
            if (status.state == worker::rpc::vm_state::running) {
                _active_vms.emplace(status.id, status.transform_version);
            }
        }
        using needs_started = ss::bool_class<struct needs_started_t>;
        chunked_hash_map<vm_key, needs_started> needs_update;
        // If anything that is actively ref'd is not in the set of running VMs
        // we need to start those VMs
        for (const auto& [key, _] : _ref_counts) {
            if (!_active_vms.contains(key)) {
                needs_update.emplace(key, needs_started::yes);
                vlog(
                  tlog.info, "vm {}/{} needs started", key.first, key.second);
            }
        }
        // Anything running that we don't have an active ref count for we need
        // to stop.
        for (const auto& key : _active_vms) {
            if (!_ref_counts.contains(key)) {
                needs_update.emplace(key, needs_started::no);
                vlog(
                  tlog.info, "vm {}/{} needs stopped", key.first, key.second);
            }
        }
        vlog(tlog.info, "reconciling {} VMs", needs_update.size());
        // If there are no differences, then we can abort the loop and notify
        // any waiters things are as expected.
        if (needs_update.empty()) {
            break;
        }
        // The main consideration here for max_concurrent is if we're starting
        // lots of VMs and need to load the binaries. They are capped out at
        // 10MB by default, so at max we load 30MB of data here, which seems
        // safe. Making this number 10 however, starts to feel uncomfortable to
        // load 100MB of data.
        // TODO(limits): This should factor into the subsystem's memory usage.
        constexpr size_t max_concurrent = 3;
        co_await ss::max_concurrent_for_each(
          needs_update,
          max_concurrent,
          [this](std::pair<vm_key, needs_started> entry) {
              const auto& [key, status] = entry;
              if (status == needs_started::yes) {
                  return start_vm(key);
              } else {
                  return stop_vm(key);
              }
          });
    }
    _reconciliation_waiters.broadcast();
}

ss::future<> remote_wasm_manager::start_vm(vm_key key) {
    auto meta = _metadata_lookup(key.first);
    if (!meta || meta->uuid != key.second) {
        vlog(
          tlog.warn,
          "failed to find metadata for vm {}/{} (outdated={})",
          key.first,
          key.second,
          meta.has_value());
        co_return;
    }
    auto load_result = co_await _rpc_client->load_wasm_binary(
      meta->source_ptr, 3s);
    if (load_result.has_error()) {
        vlog(
          tlog.warn,
          "failed to find load wasm binary for vm {}/{}: {}",
          key.first,
          key.second,
          cluster::make_error_code(load_result.error()).message());
        co_return;
    }
    auto start_result = co_await _worker_client->local().start_vm(
      key.first, *meta, std::move(load_result.value()));
    vlogl(
      tlog,
      start_result == worker::rpc::errc::success ? ss::log_level::info
                                                 : ss::log_level::warn,
      "starting vm {}/{} result: {}",
      key.first,
      key.second,
      worker::rpc::make_error_code(start_result).message());
}

ss::future<> remote_wasm_manager::stop_vm(vm_key key) {
    auto result = co_await _worker_client->local().stop_vm(
      key.first, key.second);
    vlogl(
      tlog,
      result == worker::rpc::errc::success ? ss::log_level::info
                                           : ss::log_level::warn,
      "stopping vm {}/{} result: {}",
      key.first,
      key.second,
      worker::rpc::make_error_code(result).message());
}

remote_wasm_manager::~remote_wasm_manager() {
    vassert(
      _ref_counts.empty(),
      "expected all wasm engines to be shutdown before destructing, engines "
      "running: {}",
      _ref_counts.size());
}
} // namespace transform
