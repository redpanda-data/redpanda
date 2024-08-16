/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "cluster/self_test_backend.h"

#include "base/seastarx.h"
#include "base/vlog.h"
#include "cluster/logger.h"
#include "json/document.h"
#include "ssx/future-util.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/loop.hh>
#include <seastar/util/log.hh>

namespace cluster {

self_test_backend::self_test_backend(
  model::node_id self,
  ss::sharded<node::local_monitor>& nlm,
  ss::sharded<rpc::connection_cache>& connections,
  ss::sharded<cloud_storage::remote>& cloud_storage_api,
  ss::scheduling_group sg)
  : _self(self)
  , _st_sg(sg)
  , _disk_test(nlm)
  , _network_test(self, connections)
  , _cloud_test(cloud_storage_api) {}

ss::future<> self_test_backend::start() {
    co_await _disk_test.start();
    co_await _network_test.start();
    co_await _cloud_test.start();
}

ss::future<> self_test_backend::stop() {
    auto f = _gate.close();
    co_await _disk_test.stop();
    co_await _network_test.stop();
    co_await _cloud_test.stop();
    co_await _lock.get_units(); /// Ensure outstanding work is completed
    co_await std::move(f);
}

ss::future<std::vector<self_test_result>>
self_test_backend::do_start_test(start_test_request r) {
    auto gate_holder = _gate.hold();
    std::vector<self_test_result> results;

    parse_self_test_checks(r);

    auto dtos = std::move(r.dtos);
    auto ntos = std::move(r.ntos);
    auto ctos = std::move(r.ctos);
    auto unparsed_checks = std::move(r.unparsed_checks);

    _stage = self_test_stage::disk;
    for (auto& dto : dtos) {
        try {
            dto.sg = _st_sg;
            if (!_cancelling) {
                auto dtr = co_await _disk_test.run(dto);
                results.insert(
                  results.end(),
                  std::make_move_iterator(dtr.begin()),
                  std::make_move_iterator(dtr.end()));
            } else {
                results.push_back(self_test_result{
                  .name = dto.name,
                  .test_type = "disk",
                  .warning = "Disk self test prevented from starting due to "
                             "cancel signal"});
            }
        } catch (const std::exception& ex) {
            vlog(
              clusterlog.error,
              "Disk self test finished with error: {} - options: {}",
              ex.what(),
              dto);
            results.push_back(self_test_result{
              .name = dto.name, .test_type = "disk", .error = ex.what()});
        }
    }

    _stage = self_test_stage::net;
    for (auto& nto : ntos) {
        try {
            if (!nto.peers.empty()) {
                nto.sg = _st_sg;
                if (!_cancelling) {
                    auto ntr = co_await _network_test.run(nto);
                    results.insert(
                      results.end(),
                      std::make_move_iterator(ntr.begin()),
                      std::make_move_iterator(ntr.end()));
                } else {
                    results.push_back(self_test_result{
                      .name = nto.name,
                      .test_type = "network",
                      .warning
                      = "Network self test prevented from starting due to "
                        "cancel signal"});
                }
            } else {
                results.push_back(self_test_result{
                  .name = nto.name,
                  .test_type = "network",
                  .warning = "No peers to start network test against"});
            }
        } catch (const std::exception& ex) {
            vlog(
              clusterlog.error,
              "Network self test finished with error: {} - options: {}",
              ex.what(),
              nto);
            results.push_back(self_test_result{
              .name = nto.name, .test_type = "network", .error = ex.what()});
        }
    }

    _stage = self_test_stage::cloud;
    for (auto& cto : ctos) {
        try {
            cto.sg = _st_sg;
            if (!_cancelling) {
                auto ctr = co_await _cloud_test.run(cto);
                results.insert(
                  results.end(),
                  std::make_move_iterator(ctr.begin()),
                  std::make_move_iterator(ctr.end()));
            } else {
                results.push_back(self_test_result{
                  .name = cto.name,
                  .test_type = "cloud",
                  .warning = "Cloud self test prevented from starting due to "
                             "cancel signal"});
            }
        } catch (const std::exception& ex) {
            vlog(
              clusterlog.error,
              "Cloud self test finished with error: {} - options: {}",
              ex.what(),
              cto);
            results.push_back(self_test_result{
              .name = cto.name, .test_type = "cloud", .error = ex.what()});
        }
    }

    for (const auto& unparsed_check : unparsed_checks) {
        results.push_back(self_test_result{
          .name = "Unknown",
          .test_type = unparsed_check.test_type,
          .error = fmt::format(
            "Unknown test type {} requested on node {}",
            unparsed_check.test_type,
            _self)});
    }

    co_return results;
}

get_status_response self_test_backend::start_test(start_test_request req) {
    auto units = _lock.try_get_units();
    if (units) {
        _cancelling = false;
        _id = req.id;
        vlog(
          clusterlog.debug, "Request to start self-tests with id: {}", req.id);
        ssx::background
          = ssx::spawn_with_gate_then(_gate, [this, req = std::move(req)]() {
                return do_start_test(std::move(req)).then([this](auto results) {
                    for (auto& r : results) {
                        r.test_id = _id;
                    }
                    _prev_run = get_status_response{
                      .id = _id,
                      .status = self_test_status::idle,
                      .results = std::move(results),
                      .stage = _stage};
                });
            }).finally([units = std::move(units)] {});
    } else {
        vlog(
          clusterlog.info,
          "Request to start already in-progress test receieved, updating test "
          "UUID from: {} to: {}",
          _id,
          req.id);
        _id = req.id;
    }
    return get_status();
}

ss::future<get_status_response> self_test_backend::stop_test() {
    auto gate_holder = _gate.hold();
    _cancelling = true;
    _disk_test.cancel();
    _network_test.cancel();
    _cloud_test.cancel();
    try {
        /// When lock is released, the 'then' block above will set the _prev_run
        /// var with the finalized test results from the cancelled run.
        static const auto stop_test_timeout = 5s;
        co_return co_await _lock.with(
          stop_test_timeout, [this] { return _prev_run; });
    } catch (const ss::semaphore_timed_out&) {
        vlog(clusterlog.warn, "Failed to stop self tests within 5s timeout");
    }
    co_return get_status_response{
      .id = _id, .status = self_test_status::running, .stage = _stage};
}

get_status_response self_test_backend::get_status() const {
    if (!_lock.ready()) {
        return get_status_response{
          .id = _id, .status = self_test_status::running, .stage = _stage};
    }
    return _prev_run;
}

ss::future<netcheck_response>
self_test_backend::netcheck(model::node_id source, iobuf&& iob) {
    static const auto reset_threshold = 200ms;
    auto now = ss::lowres_clock::now();
    if (likely(
          _prev_nc.source == source
          || _prev_nc.source == previous_netcheck_entity::unassigned
          || ((_prev_nc.last_request + reset_threshold) < now))) {
        _prev_nc = previous_netcheck_entity{
          .source = source, .last_request = now};
        co_return netcheck_response{.bytes_read = iob.size_bytes()};
    }
    // Clients will respect this empty response and respond by sleeping before
    // attempting to call this endpoint.
    co_return netcheck_response{.bytes_read = 0};
}

} // namespace cluster
