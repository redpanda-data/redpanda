/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "service.h"

#include "model/transform.h"
#include "random/generators.h"
#include "transform/worker/rpc/control_plane.h"
#include "transform/worker/rpc/data_plane.h"
#include "wasm/api.h"
#include "wasm/transform_probe.h"

#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/weak_ptr.hh>
#include <seastar/coroutine/as_future.hh>

#include <absl/algorithm/container.h>

#include <algorithm>
#include <exception>
#include <utility>

namespace transform::worker {
namespace {
// NOLINTNEXTLINE
static ss::logger log{"transform/worker/service"};

class collecting_logger : public wasm::logger {
public:
    void log(ss::log_level lvl, std::string_view message) noexcept override {
        _messages_by_level[lvl].push_back(ss::sstring(message));
    }

private:
    absl::flat_hash_map<ss::log_level, chunked_vector<ss::sstring>>
      _messages_by_level;
};

class dev_null_logger : public wasm::logger {
public:
    void log(ss::log_level, std::string_view) noexcept override {}
};

} // namespace

struct vm {
    model::transform_metadata meta;
    ss::lw_shared_ptr<probe> probe;
    ss::shared_ptr<wasm::engine> engine;

    rpc::vm_state compute_state() {
        return engine ? rpc::vm_state::running : rpc::vm_state::starting;
    }
};

struct probe
  : public ss::weakly_referencable<probe>
  , public ss::enable_lw_shared_from_this<probe> {
    wasm::transform_probe underlying;
};

local_service::local_service(wasm::caching_runtime* wasm_runtime)
  : _runtime(wasm_runtime) {}

local_service::~local_service() = default;

ss::future<rpc::current_state_reply>
local_service::compute_current_state(rpc::current_state_request) {
    using result
      = chunked_hash_map<std::pair<model::transform_id, uuid_t>, rpc::vm_state>;
    // Map reduce the VM states. We always end up with an unknown status if
    // cores don't agree.
    auto results = co_await container().map_reduce0(
      &local_service::do_compute_current_state,
      result{},
      [](const result& a, const result& b) { // NOLINT
          result c;
          // The common case is that all cores have the same set of VMs.
          c.reserve(a.size());
          auto combos = {std::tie(a, b), std::tie(b, a)};
          // Ensure that every entry in the resulting map has the same status on
          // both cores, otherwise the result is unknown.
          for (const auto& [src, other] : combos) {
              for (const auto& [k, v] : src) {
                  auto it = other.find(k);
                  if (it != other.end() && v == it->second) {
                      c.emplace(k, v);
                  } else {
                      c.emplace(k, rpc::vm_state::unknown);
                  }
              }
          }
          return c;
      });
    rpc::current_state_reply reply;
    for (const auto& [pair, state] : results) {
        reply.state.emplace_back(pair.first, pair.second, state);
    }
    co_return reply;
}

chunked_hash_map<std::pair<model::transform_id, uuid_t>, rpc::vm_state>
local_service::do_compute_current_state() {
    // TODO(rockwood): Protect against reactor stalls with yields
    chunked_hash_map<std::pair<model::transform_id, uuid_t>, rpc::vm_state>
      result;
    for (const auto& [k, v] : _vms) {
        result.emplace(k, v->compute_state());
    }
    return result;
}

ss::future<rpc::start_vm_reply>
local_service::start_vm(rpc::start_vm_request req) {
    auto h = co_await _vm_mutex.hold_read_lock();
    auto it = _vms.find(std::make_pair(req.id, req.metadata.uuid));
    if (it != _vms.end()) {
        co_return rpc::start_vm_reply{.error_code = rpc::errc::success};
    }
    h.return_all();
    try {
        auto factory = co_await container().invoke_on(
          0, &local_service::load_factory, &req);
        co_await container().invoke_on_all(
          &local_service::do_start_vm, req.id, req.metadata, factory.get());
        co_return rpc::start_vm_reply{.error_code = rpc::errc::success};
    } catch (const std::exception& ex) {
        vlog(log.warn, "failed to start vm: {}", ex);
        co_return rpc::start_vm_reply{
          .error_code = rpc::errc::failed_to_start_engine};
    }
}

ss::future<ss::foreign_ptr<ss::shared_ptr<wasm::factory>>>
local_service::load_factory(rpc::start_vm_request* req) {
    auto cached = _runtime->get_cached_factory(req->metadata);
    if (cached) {
        co_return ss::make_foreign(*cached);
    }
    auto factory = co_await _runtime->make_factory(
      req->metadata, std::move(req->wasm_binary));
    co_return ss::make_foreign(std::move(factory));
}

ss::future<> local_service::do_start_vm(
  model::transform_id id,
  model::transform_metadata metadata,
  wasm::factory* factory) {
    auto h = co_await _vm_mutex.hold_write_lock();
    auto it = _vms.find(std::make_pair(id, metadata.uuid));
    if (it != _vms.end()) {
        // TODO(rockwood): this case could be improved by checking if it's
        // stopping and reverting that operation, but the simpler thing is to
        // wait for reconsilation loop to retry after the stop finishes.
        co_return;
    }
    _vms.emplace(
      std::make_pair(id, metadata.uuid),
      std::make_unique<vm>(
        /*meta=*/metadata,
        /*probe=*/nullptr,
        /*engine=*/nullptr));
    h.return_all();
    auto engine = co_await factory->make_engine(
      std::make_unique<dev_null_logger>());
    auto fut = co_await ss::coroutine::as_future<>(engine->start());
    if (fut.failed()) {
        h = co_await _vm_mutex.hold_write_lock();
        _vms.erase(std::make_pair(id, metadata.uuid));
        std::rethrow_exception(fut.get_exception());
    }
    h = co_await _vm_mutex.hold_read_lock();
    it = _vms.find(std::make_pair(id, metadata.uuid));
    if (it == _vms.end()) {
        // This means someone stopped the VM, just throw away the engine.
        co_await engine->stop();
        co_return;
    }
    it->second->probe = get_or_make_probe(it->second->meta.name);
    it->second->engine = std::move(engine);
}

ss::lw_shared_ptr<probe>
local_service::get_or_make_probe(const model::transform_name& name) {
    auto it = _probes.find(name);
    if (it != _probes.end()) {
        if (it->second) {
            return it->second->shared_from_this();
        }
    }
    auto p = ss::make_lw_shared<probe>();
    p->underlying.setup_metrics(name());
    _probes[name] = p->weak_from_this();
    return p;
}

ss::future<rpc::stop_vm_reply>
local_service::stop_vm(rpc::stop_vm_request req) {
    co_await container().invoke_on_all(
      &local_service::do_stop_vm, req.id, req.transform_version);
    co_return rpc::stop_vm_reply{.error_code = rpc::errc::success};
}

ss::future<> local_service::do_stop_vm(model::transform_id id, uuid_t version) {
    // TODO(rockwood): All start and stop should be a single event queue so it's
    // simpler to manage this stuff.
    auto h = co_await _vm_mutex.hold_read_lock();
    auto it = _vms.find(std::make_pair(id, version));
    if (it != _vms.end()) {
        co_return;
    }
    h.return_all();
    h = co_await _vm_mutex.hold_write_lock();
    auto node = _vms.extract(std::make_pair(id, version));
    h.return_all();
    if (!node) {
        co_return;
    }
    if (!node->second->engine) {
        // The engine was still starting, the start case handles this.
        co_return;
    }
    co_await node->second->engine->stop();
}

ss::future<rpc::transform_data_reply>
local_service::transform_data(rpc::transform_data_request req) {
    // TODO: We should be able to route to other cores... Maybe we need a
    // foreign_ptr?
    return do_local_transform_data(std::move(req));
}

ss::future<rpc::transform_data_reply>
local_service::do_local_transform_data(rpc::transform_data_request req) {
    auto h = co_await _vm_mutex.hold_read_lock();
    auto it = _vms.find(std::make_pair(req.id, req.transform_version));
    if (it == _vms.end()) {
        co_return rpc::transform_data_reply{
          .error_code = rpc::errc::vm_not_found,
        };
    }
    absl::
      flat_hash_map<model::topic_view, chunked_vector<model::transformed_data>>
        output;
    auto& machine = it->second;
    if (!machine->engine) {
        // Still starting
        co_return rpc::transform_data_reply{
          .error_code = rpc::errc::transform_failed,
        };
    }
    auto resp = co_await do_transform(machine.get(), std::move(req));
    if (resp.error_code != rpc::errc::success) {
        try {
            co_await machine->engine->stop();
            co_await machine->engine->start();
        } catch (const std::exception& ex) {
            std::ignore = ex;
        }
    }
    co_return resp;
}

ss::future<rpc::transform_data_reply>
local_service::do_transform(vm* machine, rpc::transform_data_request req) {
    absl::
      flat_hash_map<model::topic_view, chunked_vector<model::transformed_data>>
        output;
    model::topic_view default_topic = machine->meta.output_topics.front().tp;
    for (const auto& topic : machine->meta.output_topics) {
        output.emplace(topic.tp, chunked_vector<model::transformed_data>{});
    }
    try {
        for (auto& batch : req.batches) {
            co_await machine->engine->transform(
              std::move(batch),
              &machine->probe->underlying,
              [&output, &default_topic](
                std::optional<model::topic_view> topic,
                model::transformed_data data) {
                  auto it = output.find(topic.value_or(default_topic));
                  if (it == output.end()) {
                      return ssx::now(wasm::write_success::no);
                  }
                  it->second.push_back(std::move(data));
                  return ssx::now(wasm::write_success::yes);
              });
        }
    } catch (...) {
        vlog(
          log.warn,
          "failed to transform records: {}",
          std::current_exception());
        co_return rpc::transform_data_reply{
          .error_code = rpc::errc::transform_failed,
        };
    }
    std::vector<rpc::transformed_topic_output> transformed;
    for (auto& [topic, data] : output) {
        if (data.empty()) {
            continue;
        }
        transformed.emplace_back(model::topic(topic), std::move(data));
    }
    // TODO(rockwood): collect and return logs
    co_return rpc::transform_data_reply{
      .error_code = rpc::errc::success,
      .output = std::move(transformed),
    };
}

ss::future<rpc::current_state_reply> network_service::compute_current_state(
  rpc::current_state_request req, ::rpc::streaming_context&) {
    return _service->local().compute_current_state(req);
}

ss::future<rpc::start_vm_reply> network_service::start_vm(
  rpc::start_vm_request req, ::rpc::streaming_context&) {
    return _service->local().start_vm(std::move(req));
}

ss::future<rpc::stop_vm_reply>
network_service::stop_vm(rpc::stop_vm_request req, ::rpc::streaming_context&) {
    return _service->local().stop_vm(req);
}

ss::future<rpc::transform_data_reply> network_service::transform_data(
  rpc::transform_data_request req, ::rpc::streaming_context&) {
    return _service->local().transform_data(std::move(req));
}

} // namespace transform::worker
