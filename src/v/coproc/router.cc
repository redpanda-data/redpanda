// Copyright 2020 Vectorized, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md

#include "coproc/router.h"

#include "coproc/logger.h"
#include "coproc/reference_window_consumer.hpp"
#include "coproc/supervisor.h"
#include "coproc/types.h"
#include "likely.h"
#include "model/limits.h"
#include "model/record_batch_reader.h"
#include "rpc/backoff_policy.h"
#include "storage/types.h"
#include "units.h"
#include "vassert.h"
#include "vlog.h"

#include <seastar/core/gate.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/map_reduce.hh>

#include <absl/container/flat_hash_map.h>

namespace coproc {

ss::future<std::optional<router::offset_rbr_pair>>
router::extract_offset(model::record_batch_reader reader) {
    return model::consume_reader_to_memory(std::move(reader), model::no_timeout)
      .then([](model::record_batch_reader::data_t data) {
          if (data.empty()) {
              return std::optional<offset_rbr_pair>(std::nullopt);
          }
          const auto last_offset = data.back().last_offset();
          return std::optional<offset_rbr_pair>(std::make_pair(
            last_offset,
            model::make_memory_record_batch_reader(std::move(data))));
      });
}

router::router(ss::socket_address addr, ss::sharded<storage::api>& api)
  : _api(api)
  , _jitter(std::chrono::milliseconds(10))
  , _transport(
      {rpc::transport_configuration{
        .server_addr = addr, .credentials = std::nullopt}},
      rpc::make_exponential_backoff_policy<rpc::clock_type>(
        std::chrono::seconds(1), std::chrono::seconds(10))) {}

ss::future<result<supervisor_client_protocol>> router::get_client() {
    return _transport.get_connected().then(
      [this](result<rpc::transport*> transport)
        -> result<supervisor_client_protocol> {
          if (!transport) {
              auto err = transport.error();
              if (err != rpc::errc::exponential_backoff) {
                  if (_connection_attempts++ == 5) {
                      return rpc::errc::disconnected_endpoint;
                  }
              }
              vlog(
                coproclog.warn,
                "Failed attempt to connect to coproc server, attempt "
                "number: {}",
                _connection_attempts);
              return rpc::errc::client_request_timeout;
          }
          _connection_attempts = 0;
          return coproc::supervisor_client_protocol(*transport.value());
      });
}

ss::future<> router::route() {
    /**
     * Main run loop, polls constantly for new data on registered ntps.
     * New data is defined as the latest offset being greater then the
     * last observed offset for that ntp.
     *
     * Data is consumed from the topic (max 32KiB read) and sent to the
     * interested topics, the reply is processed as writes to new materialized
     * topics.
     *
     * The loop is broken if the abort_source has been initiated externally or
     * it can be initiated internally by detection of a failed connection to the
     * engine (after retry policy expires)
     */
    try {
        return ss::with_gate(_gate, [this] {
            if (unlikely(_abort_source.abort_requested())) {
                vlog(
                  coproclog.info, "Abort source triggered, shutting down loop");
                return ss::now();
            }
            return do_route();
        });
    } catch (const ss::gate_closed_exception& gce) {
        vlog(coproclog.debug, "Gate closed exception encountered: {}", gce);
        return ss::now();
    }
}

ss::future<> router::do_route() {
    auto reducer =
      [](std::vector<process_batch_request::data> acc, opt_req_data x) {
          if (x.has_value()) {
              acc.emplace_back(std::move(*x));
          }
          return acc;
      };
    return ss::map_reduce(
             _sources.begin(),
             _sources.end(),
             [this](std::pair<const model::ntp, topic_state>& p) {
                 return route_ntp(p.first, p.second);
             },
             std::vector<process_batch_request::data>(),
             std::move(reducer))
      .then([this](std::vector<process_batch_request::data> batch) {
          return process_batch(std::move(batch));
      })
      .then([this, next_loop = _jitter()] { _loop_timer.rearm(next_loop); });
}

ss::future<>
router::process_batch(std::vector<process_batch_request::data> batch) {
    if (batch.empty()) {
        return ss::now();
    }
    return get_client().then(
      [this, batch = std::move(batch)](
        result<supervisor_client_protocol> transport) mutable {
          if (!transport) {
              const auto err = transport.error();
              if (err == rpc::errc::disconnected_endpoint) {
                  vlog(
                    coproclog.error,
                    "Shutting down loop, failed to connect to "
                    "coproc server");
                  _abort_source.request_abort();
              }
              return ss::now();
          }
          return send_batch(
            transport.value(), process_batch_request{.reqs = std::move(batch)});
      });
}

ss::future<router::opt_req_data>
router::route_ntp(const model::ntp& ntp, topic_state& ts) {
    /**
     * Making a reader will grab a mutual exclusion lock on reading from the
     * requested log. This is OK for now since the only topic_ingestion_policy
     * supported will be 'latest'
     *
     * The last offset is recorded and the request is prepared and returned.
     */
    return make_reader_cfg(ts.log, ts.head)
      .then([this, ntp, log = ts.log, sids = ts.scripts](
              opt_cfg config) mutable {
          if (!config) {
              return ss::make_ready_future<opt_req_data>(std::nullopt);
          }
          return log.make_reader(*config)
            .then([this, ntp](model::record_batch_reader reader) {
                return extract_offset(std::move(reader));
            })
            .then([this, ntp, sids = std::move(sids)](
                    std::optional<offset_rbr_pair> p) {
                if (!p) {
                    return opt_req_data(std::nullopt);
                }
                auto& [offset, rbr] = *p;
                auto found = _sources.find(ntp);
                if (found == _sources.end()) {
                    vlog(
                      coproclog.info,
                      "Ntp removed before batch assemble: {}",
                      ntp);
                    return opt_req_data(std::nullopt);
                }
                found->second.head.dirty = offset;
                std::vector<script_id> ids(
                  std::make_move_iterator(sids.begin()),
                  std::make_move_iterator(sids.end()));
                return opt_req_data(process_batch_request::data{
                  .ids = std::move(ids), .ntp = ntp, .reader = std::move(rbr)});
            });
      });
}

ss::future<> router::send_batch(
  supervisor_client_protocol transport, process_batch_request r) {
    using reply_type = result<rpc::client_context<process_batch_reply>>;
    return transport
      .process_batch(std::move(r), rpc::client_opts(model::no_timeout))
      .then_wrapped([this](ss::future<reply_type> f) {
          try {
              auto reply = f.get0();
              if (reply) {
                  return process_reply(std::move(reply.value().data));
              }
              vlog(
                coproclog.error, "Error on copro request: {}", reply.error());
          } catch (const std::exception& e) {
              vlog(coproclog.error, "Copro request future threw: {}", e.what());
          }
          return ss::now();
      });
}

ss::future<> router::process_reply(process_batch_reply r) {
    if (r.resps.empty()) {
        vlog(coproclog.error, "Erroneous empty response received");
        return ss::now();
    }
    return ss::do_with(
      std::move(r.resps),
      [this](std::vector<process_batch_reply::data>& resps) mutable {
          const auto range = boost::irange<size_t>(0, resps.size());
          return ss::do_for_each(range, [this, &resps](size_t i) {
              return process_reply_one(std::move(resps[i]));
          });
      });
}

void router::bump_offset(const model::ntp& src_ntp, const script_id sid) {
    auto found = _sources.find(src_ntp);
    if (found == _sources.end()) {
        vlog(coproclog.warn, "Ntp removed before offset set: {}", src_ntp);
        return;
    }
    auto fsid = found->second.scripts.find(sid);
    if (fsid == found->second.scripts.end()) {
        vlog(coproclog.warn, "Script id removed before offset set: {}", sid);
        return;
    }
    found->second.head.committed = found->second.head.dirty;
}

ss::future<> router::process_reply_one(process_batch_reply::data e) {
    // Strip the source/dest topics from the materialized topic
    const auto mt = model::make_materialized_topic(e.ntp.tp.topic);
    if (!mt) {
        // For now this will signify a null response, which means the
        // record_batch was is to be filtered out of the materialized_topic.
        // Mark offset, to continue to next record and do nothing else.
        bump_offset(e.ntp, e.id);
        return ss::now();
    }
    // The original ntp without the .$<destination>$ part of the topic
    model::ntp src_ntp(e.ntp.ns, mt->src, e.ntp.tp.partition);
    // Create the materialized log, the name of the log will be of the
    // format: <src>.$<destination>$
    return get_log(e.ntp).then([this,
                                src_ntp,
                                id = e.id,
                                reader = std::move(e.reader)](
                                 storage::log log) mutable {
        // Append the requested data to the end of the log
        storage::log_append_config cfg{
          .should_fsync = storage::log_append_config::fsync::no,
          .io_priority = ss::default_priority_class(),
          .timeout = model::no_timeout};
        return std::move(reader)
          .for_each_ref(
            coproc::reference_window_consumer(
              model::record_batch_crc_checker(), log.make_appender(cfg)),
            model::no_timeout)
          .then(
            [this, src_ntp, id, log](
              std::tuple<bool, ss::future<storage::append_result>> t) mutable {
                /// TODO(rob) NOT ideal to flush on every response.
                /// Clubhouse ticket 'coprocessor enhancments' filed for this
                const auto& [crc_parse_success, _] = t;
                if (!crc_parse_success) {
                    vlog(
                      coproclog.warn,
                      "record_batch failed to pass crc checks, not promoting "
                      "log offset for source ntp: {}",
                      src_ntp);
                    return ss::now();
                }
                bump_offset(src_ntp, id);
                return log.flush();
            });
    });
}

ss::future<router::opt_cfg>
router::make_reader_cfg(storage::log log, topic_offsets& head) {
    return head.mtx.with([this, log, committed = head.committed]() {
        const storage::offset_stats ostats = log.offsets();
        if (committed >= ostats.committed_offset) {
            // Signifies materialized log is up-to-date with source, there
            // isn't anything more to read
            return opt_cfg(std::nullopt);
        }
        const model::offset start
          = (committed == model::model_limits<model::offset>::min())
              ? model::offset(0)
              : committed + model::offset(1);
        return opt_cfg(
          reader_cfg(start, model::model_limits<model::offset>::max()));
    });
}

ss::future<storage::log> router::get_log(const model::ntp& ntp) {
    auto found = _api.local().log_mgr().get(ntp);
    if (found) {
        return ss::make_ready_future<storage::log>(*found);
    }
    vlog(coproclog.info, "Making new log: {}", ntp);
    return _api.local().log_mgr().manage(
      storage::ntp_config(ntp, _api.local().log_mgr().config().base_dir));
}

errc router::add_source(
  const script_id id,
  const model::topic_namespace& tns,
  topic_ingestion_policy p) {
    // For now only support the 'latest' policy
    if (!is_valid_ingestion_policy(p)) {
        return errc::invalid_ingestion_policy;
    }
    auto logs = _api.local().log_mgr().get(tns);
    if (logs.empty()) {
        return errc::topic_does_not_exist;
    }

    for (auto& [ntp, log] : logs) {
        auto found = _sources.find(ntp);
        if (found == _sources.end()) {
            topic_state ts{
              .log = log, .head = topic_offsets(), .scripts = {id}};
            _sources.emplace(ntp, std::move(ts));
        } else {
            found->second.scripts.emplace(id);
        }
        vlog(coproclog.info, "Inserted ntp {} id {}", ntp, id);
    }
    return errc::success;
}

bool router::remove_source(const script_id sid) {
    absl::flat_hash_set<model::ntp> deleted;
    std::for_each(_sources.begin(), _sources.end(), [&deleted, sid](auto& p) {
        auto& scripts = p.second.scripts;
        scripts.erase(sid);
        vlog(coproclog.info, "Deleted script id: {}", sid);
        if (scripts.empty()) {
            deleted.emplace(p.first);
        }
    });
    // If no more scripts are tracking an ntp, remove the ntp
    absl::erase_if(_sources, [&deleted](const auto& p) {
        return deleted.contains(p.first);
    });

    return !deleted.empty();
}

bool router::script_id_exists(const script_id sid) const {
    return std::any_of(_sources.begin(), _sources.end(), [sid](const auto& p) {
        return p.second.scripts.contains(sid);
    });
}

storage::log_reader_config
router::reader_cfg(model::offset start, model::offset end) {
    return storage::log_reader_config(
      start,
      end,
      1,
      32_KiB,
      ss::default_priority_class(),
      model::well_known_record_batch_types[1],
      std::nullopt,
      _abort_source);
}

} // namespace coproc
