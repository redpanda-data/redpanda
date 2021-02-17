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
#include "coproc/types.h"
#include "likely.h"
#include "model/record_batch_reader.h"
#include "model/timeout_clock.h"
#include "raft/types.h"
#include "storage/api.h"
#include "storage/types.h"
#include "vlog.h"

#include <chrono>
namespace coproc {

struct batch_info {
    model::offset last;
    std::size_t total_size_bytes;
    model::record_batch_reader rbr;
};

std::optional<batch_info>
extract_batch_info(model::record_batch_reader::data_t data) {
    if (data.empty()) {
        /// TODO(rob) Its possible to recieve an empty batch when a
        /// batch type filter is enabled on the reader which for copro
        return std::nullopt;
    }
    const model::offset last_offset = data.back().last_offset();
    const std::size_t total_size = std::accumulate(
      data.cbegin(),
      data.cend(),
      std::size_t(0),
      [](std::size_t acc, const model::record_batch& x) {
          return acc + x.size_bytes();
      });
    return batch_info{
      .last = last_offset,
      .total_size_bytes = total_size,
      .rbr = model::make_memory_record_batch_reader(std::move(data))};
}

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
    vassert(
      !_gate.is_closed() && _gate.get_count() == 0,
      "Cannot call start() twice");
    (void)ss::with_gate(_gate, [this] {
        return ss::do_until(
          [this] { return _abort_source.abort_requested(); },
          [this] {
              using namespace std::chrono_literals;
              return do_execute().then([this] {
                  return ss::sleep_abortable(
                           _resources.jitter.next_jitter_duration(),
                           _abort_source)
                    .handle_exception([](std::exception_ptr ep) {
                        vlog(
                          coproclog.debug,
                          "script_context loop exception: {}",
                          ep);
                    });
              });
          });
    });
    return ss::now();
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
              return read().then(
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
                      .then([] { return ss::stop_iteration::no; })
                      .handle_exception([](std::exception_ptr eptr) {
                          vlog(
                            coproclog.error,
                            "Exception encountered during send: {}",
                            eptr);
                          return ss::stop_iteration::yes;
                      });
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
          return ss::make_exception_future<>(std::runtime_error(
            fmt::format("Error on copro request: {}", reply.error())));
      })
      .handle_exception([](std::exception_ptr eptr) {
          return ss::make_exception_future<>(std::runtime_error(
            fmt::format("Copro request future threw: {}", eptr)));
      });
}

ss::future<std::vector<process_batch_request::data>> script_context::read() {
    std::vector<process_batch_request::data> requests;
    requests.reserve(_ntp_ctxs.size());
    return ss::do_with(
      std::move(requests),
      [this](std::vector<process_batch_request::data>& requests) {
          return ss::parallel_for_each(
                   _ntp_ctxs,
                   [this, &requests](const ntp_context_cache::value_type& p) {
                       return read_ntp(p.second).then(
                         [&requests](
                           std::optional<process_batch_request::data> r) {
                             if (r) {
                                 requests.push_back(std::move(*r));
                             }
                         });
                   })
            .then([&requests] { return std::move(requests); });
      });
}

storage::log_reader_config
script_context::get_reader(const ss::lw_shared_ptr<ntp_context>& ntp_ctx) {
    auto found = ntp_ctx->offsets.find(_id);
    vassert(
      found != ntp_ctx->offsets.end(),
      "script_id must exist: {} for ntp: {}",
      _id,
      ntp_ctx->ntp());
    const ntp_context::offset_pair& cp_offsets = found->second;
    const model::offset next_read
      = (unlikely(cp_offsets.last_acked == model::offset{}))
          ? model::offset(0)
          : cp_offsets.last_acked + model::offset(1);
    if (next_read <= cp_offsets.last_acked) {
        vlog(
          coproclog.info,
          "Replaying read on ntp: {} at offset: {}",
          ntp_ctx->ntp(),
          cp_offsets.last_read);
    }
    const storage::offset_stats os = ntp_ctx->log.offsets();
    return storage::log_reader_config(
      next_read,
      os.dirty_offset,
      1,
      max_batch_size(),
      ss::default_priority_class(),
      raft::data_batch_type,
      std::nullopt,
      _abort_source);
}

ss::future<std::optional<process_batch_request::data>>
script_context::read_ntp(ss::lw_shared_ptr<ntp_context> ntp_ctx) {
    return ss::with_semaphore(
      _resources.read_sem, max_batch_size(), [this, ntp_ctx]() {
          storage::log_reader_config cfg = get_reader(ntp_ctx);
          return ntp_ctx->log.make_reader(cfg)
            .then([](model::record_batch_reader rbr) {
                return model::consume_reader_to_memory(
                  std::move(rbr), model::no_timeout);
            })
            .then([](model::record_batch_reader::data_t data) {
                return extract_batch_info(std::move(data));
            })
            .then(
              [this, ntp_ctx](std::optional<batch_info> obatch_info)
                -> std::optional<process_batch_request::data> {
                  if (!obatch_info) {
                      return std::nullopt;
                  }
                  ntp_ctx->offsets[_id].last_read = obatch_info->last;
                  return process_batch_request::data{
                    .ids = std::vector<script_id>{_id},
                    .ntp = ntp_ctx->ntp(),
                    .reader = std::move(obatch_info->rbr)};
              });
      });
}

ss::future<> script_context::process_reply(process_batch_reply reply) {
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
        return ss::make_exception_future<>(std::logic_error(fmt::format(
          "id: {} recieved response from another script_id: {}", _id, e.id)));
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
    return write_materialized(materialized_ntp, std::move(e.reader))
      .then([this, ntp_ctx](bool crc_parse_failure) {
          if (crc_parse_failure) {
              vlog(coproclog.warn, "record_batch failed to pass crc checks");
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

ss::future<bool>
write_checked(storage::log log, model::record_batch_reader reader) {
    const storage::log_append_config write_cfg{
      .should_fsync = storage::log_append_config::fsync::no,
      .io_priority = ss::default_priority_class(),
      .timeout = model::no_timeout};
    return std::move(reader)
      .for_each_ref(
        coproc::reference_window_consumer(
          model::record_batch_crc_checker(), log.make_appender(write_cfg)),
        model::no_timeout)
      .then([](std::tuple<bool, ss::future<storage::append_result>> t) {
          const auto& [crc_parse_success, _] = t;
          return !crc_parse_success;
      });
}

ss::future<bool> script_context::write_materialized(
  const model::materialized_ntp& m_ntp, model::record_batch_reader reader) {
    auto found = _resources.log_mtx.find(m_ntp.input_ntp());
    if (found == _resources.log_mtx.end()) {
        found = _resources.log_mtx.emplace(m_ntp.input_ntp(), mutex()).first;
    }
    return found->second.with(
      [this, m_ntp, reader = std::move(reader)]() mutable {
          return get_log(_resources.api, m_ntp.input_ntp())
            .then([reader = std::move(reader)](storage::log log) mutable {
                return write_checked(std::move(log), std::move(reader));
            });
      });
}

} // namespace coproc
