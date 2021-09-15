/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "coproc/pacemaker.h"

#include "coproc/exception.h"
#include "coproc/logger.h"
#include "coproc/types.h"
#include "model/validation.h"
#include "rpc/reconnect_transport.h"
#include "rpc/types.h"
#include "ssx/future-util.h"
#include "storage/api.h"
#include "storage/directories.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/reactor.hh>

#include <absl/container/flat_hash_set.h>

#include <algorithm>
#include <exception>

namespace coproc {

rpc::transport_configuration
wasm_transport_cfg(const net::unresolved_address& addr) {
    return rpc::transport_configuration{
      .server_addr = addr,
      .max_queued_bytes = static_cast<uint32_t>(
        config::shard_local_cfg().coproc_max_inflight_bytes.value()),
      .credentials = nullptr,
      .disable_metrics = net::metrics_disabled(
        config::shard_local_cfg().disable_metrics())};
}

rpc::backoff_policy wasm_transport_backoff() {
    using namespace std::chrono_literals;
    return rpc::make_exponential_backoff_policy<rpc::clock_type>(1s, 10s);
}

void pacemaker::save_routes() {
    (void)ss::with_gate(_gate, [this] {
        all_routes routes;
        routes.reserve(_scripts.size());
        for (auto& [id, script] : _scripts) {
            routes.emplace(id, script->get_routes());
        }
        return save_offsets(_offs.snap, std::move(routes)).then([this] {
            if (!_offs.timer.armed()) {
                _offs.timer.arm(_offs.duration);
            }
        });
    });
}

pacemaker::pacemaker(net::unresolved_address addr, sys_refs& rs)
  : _shared_res(
    rpc::reconnect_transport(
      wasm_transport_cfg(addr), wasm_transport_backoff()),
    rs) {
    _offs.timer.set_callback([this] {
        try {
            save_routes();
        } catch (const ss::gate_closed_exception&) {
            vlog(
              coproclog.debug, "Gate closed while attempting to write offsets");
        }
    });
}

ss::future<> pacemaker::start() {
    co_await ss::recursive_touch_directory(offsets_snapshot_path().string());
    _cached_routes = co_await recover_offsets(_offs.snap);
    if (!_offs.timer.armed()) {
        _offs.timer.arm(_offs.duration);
    }
}

ss::future<> pacemaker::reset() {
    _offs.timer.cancel();
    all_routes routes;
    routes.reserve(_scripts.size());
    for (auto& [id, script] : _scripts) {
        routes.emplace(id, script->get_routes());
    }
    auto removed = co_await remove_all_sources();
    _cached_routes = std::move(routes);
    vlog(coproclog.info, "Pacemaker reset {} scripts", removed.size());
    if (!_offs.timer.armed()) {
        _offs.timer.arm(_offs.duration);
    }
}

ss::future<> pacemaker::stop() {
    /// Ensure no more timers are fired
    _offs.timer.cancel();
    /// Waiting on the gate here will cause a deadlock since the signal to stop
    /// all fibers hasn't yet been sent out (remove_all_sources). However we
    /// wish to close the gate before any async actions begin in this method to
    /// prevent add_sources from adding new scripts.
    ss::future<> gate_closed = _gate.close();
    /// Deregister and clean-up resources
    for (auto& [id, ps] : _updates) {
        for (auto& p : ps) {
            p.set_exception(wait_on_script_future_stranded(
              fmt::format("Script {} startup future abandoned", id)));
        }
    }
    const size_t n_active_scripts = _scripts.size();
    const auto results = co_await remove_all_sources();
    const size_t n_removed = std::accumulate(
      results.cbegin(),
      results.cend(),
      size_t(0),
      [](size_t acc, const std::pair<script_id, errc>& pair) {
          return pair.second == errc::success ? acc + 1 : acc;
      });
    if (n_removed != n_active_scripts) {
        vlog(coproclog.error, "Failed to gracefully shutdown all copro fibers");
    }
    /// Finally close the connection to the wasm engine
    vlog(coproclog.info, "Closing connection to coproc wasm engine");
    co_await _shared_res.transport.stop();
    co_await std::move(gate_closed);
}

std::vector<errc> pacemaker::add_source(
  script_id id, std::vector<topic_namespace_policy> topics) {
    routes_t rs;
    std::vector<errc> acks;
    vassert(
      _scripts.find(id) == _scripts.end(),
      "add_source() detects already existing script_id: {}",
      id);
    do_add_source(id, rs, acks, topics);
    if (rs.empty()) {
        /// Failure occurred or no matching topics/ntps found
        /// Reasons are returned in the 'acks' structure
        fire_updates(id, errc::topic_does_not_exist);
        return acks;
    }
    auto script_ctx = std::make_unique<script_context>(
      id, _shared_res, std::move(rs));
    const auto [_, success] = _scripts.emplace(id, std::move(script_ctx));
    vassert(success, "Double coproc insert detected");
    vlog(coproclog.debug, "Adding source with id: {}", id);
    (void)ss::with_gate(_gate, [this, id] {
        fire_updates(id, errc::success);
        auto found = _scripts.find(id);
        if (found == _scripts.end()) {
            return ss::now();
        }
        return found->second->start().handle_exception_type(
          [this, id](const script_exception& e) {
              /// A script must be deregistered due to an internal script error.
              /// The wasm engine determines the case, most likley the apply()
              /// method has thrown or there is a syntax error within the script
              /// itself.
              vlog(coproclog.error, "Script failure handler: {}", e.what());
              vassert(
                id == e.get_id(),
                "script_failed_handler id mismatch detected, observed: {} "
                "expected: {}",
                e.get_id(),
                id);
              /// TODO(rob): Poses interesting issue, now that there is a script
              /// database entry in the db but is not 'running'
              return container().invoke_on_all([id](pacemaker& p) {
                  return p.remove_source(id).discard_result();
              });
          });
    });
    return acks;
}

void pacemaker::do_add_source(
  script_id id,
  routes_t& rs,
  std::vector<errc>& acks,
  const std::vector<topic_namespace_policy>& topics) {
    auto saved = _cached_routes.extract(id);
    if (!saved.empty()) {
        vlog(coproclog.info, "Recovering offsets for id: {}", id);
        rs = std::move(saved.mapped());
    }
    for (const topic_namespace_policy& tnp : topics) {
        auto partitions = _shared_res.rs.partition_manager.local()
                            .get_topic_partition_table(tnp.tn);
        if (partitions.empty()) {
            acks.push_back(errc::topic_does_not_exist);
            continue;
        }

        for (auto& [ntp, partition] : partitions) {
            auto [itr, success] = rs.emplace(ntp, ss::make_lw_shared<source>());
            itr->second->rctx.input = partition;
            if (success || tnp.policy != topic_ingestion_policy::stored) {
                if (tnp.policy == topic_ingestion_policy::latest) {
                    itr->second->rctx.absolute_start
                      = partition->last_stable_offset();
                } else {
                    /// Default mode is to start at offset 0, if stored is
                    /// chosen but there is no stored offset found
                    itr->second->rctx.absolute_start = model::offset{0};
                }
            }
        }
        acks.push_back(errc::success);
    }
    /// Could be the case that input topics were delete before restart of
    /// coprocessor, remove them from working set and log the event
    absl::erase_if(rs, [](routes_t::value_type& p) {
        if (!p.second->rctx.input) {
            vlog(
              coproclog.error,
              "Missing input for ntp {} after recovery",
              p.first);
            return true;
        }
        return false;
    });
}

ss::future<errc> pacemaker::remove_source(script_id id) {
    vlog(coproclog.debug, "Removing source with id: {}", id);
    auto handle = _scripts.extract(id);
    if (handle.empty()) {
        co_return errc::script_id_does_not_exist;
    }
    std::unique_ptr<script_context> ctx = std::move(handle.mapped());
    co_await ctx->shutdown();
    co_return errc::success;
}

ss::future<absl::flat_hash_map<script_id, errc>>
pacemaker::restart_partition(model::ntp ntp, script_inputs_t inputs) {
    absl::flat_hash_map<script_id, errc> results;
    auto partition = _shared_res.rs.partition_manager.local().get(ntp);
    if (!partition) {
        for (const auto& [id, _] : inputs) {
            results.emplace(id, errc::partition_not_exists);
        }
        co_return results;
    }
    std::vector<ss::future<>> fs;
    for (auto& [id, tnps] : inputs) {
        read_context rps{.input = partition};
        auto found = _scripts.find(id);
        if (found != _scripts.end()) {
            fs.emplace_back(
              found->second->start_processing_ntp(ntp, std::move(rps))
                .handle_exception_type([ntp](const wait_future_stranded&) {
                    return errc::internal_error;
                })
                .then([id = id, &results](errc e) { results[id] = e; }));
        } else {
            /// Ignore existing offsets that may exist on disk
            _cached_routes.erase(id);
            auto errs = add_source(id, std::move(tnps));
            bool all_failures = std::all_of(
              errs.begin(), errs.end(), [](errc e) {
                  return e != errc::success;
              });
            results[id] = all_failures ? errc::topic_does_not_exist
                                       : errc::success;
        }
    }
    co_await ss::when_all_succeed(fs.begin(), fs.end());
    co_return results;
}

ss::future<std::vector<script_id>>
pacemaker::shutdown_partition(model::ntp ntp) {
    std::vector<ss::future<script_id>> fs;
    for (const auto& [id, script] : _scripts) {
        if (auto route = script->get_route(ntp)) {
            fs.push_back(
              script->stop_processing_ntp(ntp)
                .then([id = id](errc) { return id; })
                .handle_exception_type(
                  [id = id](const wait_future_stranded&) { return id; }));
        }
    }
    co_return co_await ss::when_all_succeed(fs.begin(), fs.end());
}

ss::future<absl::btree_map<script_id, errc>> pacemaker::remove_all_sources() {
    std::vector<script_id> ids;
    ids.reserve(_scripts.size());
    for (const auto& [id, _] : _scripts) {
        ids.push_back(id);
    }
    absl::btree_map<script_id, errc> results_map;
    for (const script_id& id : ids) {
        errc result = co_await remove_source(id);
        results_map.emplace(id, result);
    }
    co_return results_map;
}

void pacemaker::fire_updates(script_id id, errc e) {
    auto handle = _updates.extract(id);
    if (!handle.empty()) {
        auto update_vec = std::move(handle.mapped());
        for (auto& p : update_vec) {
            p.set_value(e);
        }
    }
}

ss::future<errc> pacemaker::wait_for_script(script_id id) {
    if (_gate.is_closed()) {
        return ss::make_exception_future<errc>(ss::gate_closed_exception());
    }
    if (_scripts.count(id)) {
        return ss::make_ready_future<errc>(errc::success);
    }
    auto [itr, _] = _updates.try_emplace(id);
    itr->second.emplace_back();
    return itr->second.back().get_future();
}

bool pacemaker::local_script_id_exists(script_id id) {
    return _scripts.find(id) != _scripts.end();
}

bool pacemaker::is_up_to_date() const {
    return std::all_of(_scripts.cbegin(), _scripts.cend(), [](const auto& p) {
        return p.second->is_up_to_date();
    });
}

} // namespace coproc
