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

#include "coproc/logger.h"
#include "coproc/reference_window_consumer.hpp"
#include "coproc/script_context_frontend.h"
#include "coproc/types.h"
#include "model/record_batch_reader.h"
#include "model/timeout_clock.h"
#include "storage/api.h"
#include "storage/parser_utils.h"
#include "storage/types.h"
#include "vlog.h"

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
              supervisor_client_protocol client(*transport.value());
              input_read_args args{
                .id = _id,
                .read_sem = _resources.read_sem,
                .abort_src = _abort_source,
                .inputs = _ntp_ctxs};
              return read_from_inputs(args).then(
                [this, client = std::move(client)](
                  std::vector<process_batch_request::data> requests) mutable {
                    if (requests.empty()) {
                        /// No data to read from all inputs, no need to
                        /// incessently loop, can exit to yield
                        return ss::make_ready_future<ss::stop_iteration>(
                          ss::stop_iteration::yes);
                    }
                    /// Send request to wasm engine
                    process_batch_request req{.reqs = std::move(requests)};
                    return send_request(std::move(client), std::move(req))
                      .then([] { return ss::stop_iteration::no; });
                });
          });
    });
}

ss::future<> script_context::shutdown() {
    _abort_source.request_abort();
    return _gate.close().then([this] { _ntp_ctxs.clear(); });
}

ss::future<> script_context::send_request(
  supervisor_client_protocol client, process_batch_request r) {
    using reply_t = result<rpc::client_context<process_batch_reply>>;
    return client
      .process_batch(
        std::move(r), rpc::client_opts(rpc::clock_type::now() + 5s))
      .then([this](reply_t reply) {
          if (reply) {
              return process_reply(std::move(reply.value().data));
          }
          vlog(
            coproclog.warn,
            "Error upon attempting to perform RPC to wasm engine, code: {}",
            reply.error());
          return ss::now();
      });
}

ss::future<> script_context::process_reply(process_batch_reply reply) {
    if (reply.resps.empty()) {
        vlog(
          coproclog.error, "Wasm engine interpreted the request as erraneous");
        return ss::now();
    }
    return ss::do_with(std::move(reply), [this](process_batch_reply& reply) {
        return ss::do_for_each(
          reply.resps, [this](process_batch_reply::data& e) {
              return process_one_reply(std::move(e));
          });
    });
}

ss::future<> script_context::process_one_reply(process_batch_reply::data e) {
    /// Ensure this 'script_context' instance is handling the correct reply
    if (e.id != _id) {
        /// TODO: Maybe in the future errors of these type should mean redpanda
        /// kill -9's the wasm engine.
        vlog(
          coproclog.error,
          "erranous reply from wasm engine, mismatched id observed, expected: "
          "{} and observed {}",
          _id,
          e.id);
        return ss::now();
    }
    if (!e.reader) {
        return ss::make_exception_future<>(script_failed_exception(
          e.id,
          fmt::format(
            "script id {} will auto deregister due to an internal syntax "
            "error",
            e.id)));
    }
    /// Use the source topic portion of the materialized topic to perform a
    /// lookup for the relevent 'ntp_context'
    auto materialized_ntp = model::materialized_ntp(e.ntp);
    auto found = _ntp_ctxs.find(materialized_ntp.source_ntp());
    if (found == _ntp_ctxs.end()) {
        vlog(
          coproclog.warn,
          "script {} unknown source ntp: {}",
          _id,
          materialized_ntp.source_ntp());
        return ss::now();
    }
    auto ntp_ctx = found->second;
    return write_materialized(materialized_ntp, std::move(*e.reader))
      .then([this, ntp_ctx](write_response wr) {
          if (wr == write_response::crc_failure) {
              vlog(coproclog.warn, "record_batch failed to pass crc checks");
              return;
          } else if (wr == write_response::term_too_old) {
              vlog(coproclog.debug, "older term record detected, retrying");
              return;
          }
          auto ofound = ntp_ctx->offsets.find(_id);
          vassert(
            ofound != ntp_ctx->offsets.end(),
            "Offset not found for script id {} for ntp owning context: {}",
            _id,
            ntp_ctx->ntp());
          /// Reset the acked offset so that progress can be made
          ofound->second.last_acked = ofound->second.last_read;
      });
}

ss::future<storage::log> get_log(storage::api& api, const model::ntp& ntp) {
    auto found = api.log_mgr().get(ntp);
    if (found) {
        return ss::make_ready_future<storage::log>(*found);
    }
    vlog(coproclog.info, "Making new log: {}", ntp);
    return api.log_mgr().manage(
      storage::ntp_config(ntp, api.log_mgr().config().base_dir));
}

/// Solution to case where scripts writing to the same materialized topic may
/// attempt to write a record with a lower term_id then the logs base.
class term_id_barrier {
public:
    explicit term_id_barrier(model::term_id last)
      : _last(last) {}

    ss::future<ss::stop_iteration> operator()(const model::record_batch& rb) {
        /// If the situation is encountered, the consumer will be alerted,
        /// and in the case below, the reference_window_consumer will not
        /// attempt to further process the batch, i.e. aborting the write
        if (rb.term() < _last) {
            _exited_early = true;
        }
        return ss::make_ready_future<ss::stop_iteration>(
          _exited_early ? ss::stop_iteration::yes : ss::stop_iteration::no);
    }

    std::optional<model::term_id> end_of_stream() {
        return _exited_early ? std::nullopt : std::optional(_last);
    }

private:
    bool _exited_early{false};
    model::term_id _last;
};

ss::future<std::variant<script_context::write_response, model::term_id>>
script_context::write_checked(
  storage::log log,
  model::term_id last_term,
  model::record_batch_reader reader) {
    using rt_val = std::variant<write_response, model::term_id>;
    using consumption_result = std::
      tuple<bool, std::optional<model::term_id>, model::record_batch_reader>;
    return std::move(reader)
      .for_each_ref(
        coproc::reference_window_consumer(
          model::record_batch_crc_checker(),
          term_id_barrier(last_term),
          storage::internal::compress_batch_consumer(
            model::compression::zstd, 512)),
        model::no_timeout)
      .then([log](consumption_result rs) mutable {
          bool crc_success = std::get<bool>(rs);
          if (!crc_success) {
              return ss::make_ready_future<rt_val>(write_response::crc_failure);
          }
          auto newest_term = std::get<std::optional<model::term_id>>(rs);
          if (!newest_term) {
              return ss::make_ready_future<rt_val>(
                write_response::term_too_old);
          }
          const storage::log_append_config write_cfg{
            .should_fsync = storage::log_append_config::fsync::no,
            .io_priority = ss::default_priority_class(),
            .timeout = model::no_timeout};
          return std::move(std::get<model::record_batch_reader>(rs))
            .for_each_ref(log.make_appender(write_cfg), model::no_timeout)
            .then(
              [](storage::append_result ar) { return rt_val(ar.last_term); });
      });
}

ss::future<script_context::write_response> script_context::write_materialized(
  const model::materialized_ntp& m_ntp, model::record_batch_reader reader) {
    auto found = _resources.log_mtx.find(m_ntp.input_ntp());
    if (found == _resources.log_mtx.end()) {
        found = _resources.log_mtx
                  .emplace(
                    m_ntp.input_ntp(),
                    std::make_pair(mutex(), model::term_id{}))
                  .first;
    }
    return found->second.first.with([this,
                                     m_ntp,
                                     reader = std::move(reader)]() mutable {
        model::term_id last_term = _resources.log_mtx[m_ntp.input_ntp()].second;
        return get_log(_resources.api, m_ntp.input_ntp())
          .then([this, last_term, reader = std::move(reader)](
                  storage::log log) mutable {
              return write_checked(
                std::move(log), last_term, std::move(reader));
          })
          .then([this,
                 m_ntp](std::variant<write_response, model::term_id> written) {
              if (std::holds_alternative<model::term_id>(written)) {
                  model::term_id next_term = std::get<model::term_id>(written);
                  _resources.log_mtx[m_ntp.input_ntp()].second = next_term;
                  return write_response::success;
              }
              return std::get<write_response>(written);
          });
    });
}

} // namespace coproc
