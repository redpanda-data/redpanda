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

#include "coproc/script_context.h"

#include "coproc/exception.h"
#include "coproc/logger.h"
#include "coproc/reference_window_consumer.hpp"
#include "coproc/script_context_backend.h"
#include "coproc/script_context_frontend.h"
#include "coproc/types.h"
#include "model/record_batch_reader.h"
#include "model/timeout_clock.h"
#include "storage/api.h"
#include "storage/parser_utils.h"
#include "storage/types.h"
#include "vlog.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>

#include <chrono>
#include <exception>

namespace coproc {

script_context::script_context(
  script_id id,
  shared_script_resources& resources,
  ntp_context_cache&& contexts)
  : _resources(resources)
  , _ntp_ctxs(std::move(contexts))
  , _id(id) {
    vassert(
      !_ntp_ctxs.empty(),
      "Unallowed to create an instance of script_context without having a "
      "valid subscription list");
}

ss::future<> script_context::start() {
    vassert(_gate.get_count() == 0, "Cannot call start() twice");
    return ss::with_gate(_gate, [this] {
        return ss::do_until(
          [this] { return _abort_source.abort_requested(); },
          [this] {
              /// do_execute is by design expected to throw one type of
              /// exception, \ref script_failed_exception for which there is a
              /// handler setup by the invoker of this start() method
              return do_execute().then([this] {
                  process_idle_callbacks();
                  return ss::sleep_abortable(
                           _resources.jitter.next_jitter_duration(),
                           _abort_source)
                    .handle_exception_type([](const ss::sleep_aborted&) {});
              });
          });
    });
}

ss::future<> script_context::do_execute() {
    /// This loop executes while there is data to read from the input logs and
    /// while there is a current successful connection to the wasm engine.
    /// If both of those conditions aren't met, the loop breaks, hitting the
    /// sleep_abortable() call in the fiber started by 'start()'
    return ss::repeat([this] {
        if (_abort_source.abort_requested()) {
            return ss::make_ready_future<ss::stop_iteration>(
              ss::stop_iteration::yes);
        }
        return _resources.transport.get_connected(model::no_timeout)
          .then([this](result<rpc::transport*> transport) {
              if (!transport) {
                  /// Failed to connected to the wasm engine for whatever
                  /// reason, exit to yield
                  return ss::make_ready_future<ss::stop_iteration>(
                    ss::stop_iteration::yes);
              }
              return process_send_write(transport.value())
                .handle_exception_type(
                  [this](const partition_shutdown_exception& ex) {
                      vlog(
                        coproclog.warn,
                        "Removing partition due to shutdown: {}",
                        ex.ntp());
                      _ntp_ctxs.erase(ex.ntp());
                      return ss::make_ready_future<ss::stop_iteration>(
                        ss::stop_iteration::yes);
                  });
          });
    });
}

void script_context::process_idle_callbacks() {
    auto ps = std::exchange(_idle, std::vector<ss::promise<>>());
    for (auto& p : ps) {
        p.set_value();
    }
}

ss::future<> script_context::wait_idle_state() {
    if (_gate.is_closed()) {
        return ss::make_exception_future<>(ss::gate_closed_exception());
    }
    _idle.emplace_back();
    return _idle.back().get_future();
}

ss::future<ss::stop_iteration>
script_context::process_send_write(rpc::transport* t) {
    /// Read batch of data
    input_read_args args{
      .id = _id,
      .read_sem = _resources.read_sem,
      .abort_src = _abort_source,
      .inputs = _ntp_ctxs};
    auto requests = co_await read_from_inputs(args);
    if (requests.empty()) {
        co_return ss::stop_iteration::yes;
    }
    /// Send request to wasm engine
    process_batch_request req{.reqs = std::move(requests)};
    supervisor_client_protocol client(*t);
    auto response = co_await client.process_batch(
      std::move(req), rpc::client_opts(rpc::clock_type::now() + 5s));
    if (!response) {
        vlog(
          coproclog.warn,
          "Error upon attempting to perform RPC to wasm engine, code: {}",
          response.error());
        co_return ss::stop_iteration::yes;
    }
    /// Finally write response
    output_write_args oargs{
      .id = _id,
      .rs = _resources.rs,
      .inputs = _ntp_ctxs,
      .locks = _resources.log_mtx};
    co_await write_materialized(std::move(response.value().data.resps), oargs);
    co_return ss::stop_iteration::no;
}

ss::future<> script_context::shutdown() {
    auto ps = std::exchange(_idle, std::vector<ss::promise<>>());
    for (auto& p : ps) {
        p.set_exception(wait_idle_state_future_stranded(fmt::format(
          "Idle callback futures abandoned due to shutdown, id: {}", _id)));
    }
    _abort_source.request_abort();
    return _gate.close().then([this] { _ntp_ctxs.clear(); });
}

} // namespace coproc
