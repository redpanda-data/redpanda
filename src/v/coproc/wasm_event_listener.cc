/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include "coproc/wasm_event_listener.h"

#include "config/configuration.h"
#include "coproc/errc.h"
#include "coproc/logger.h"
#include "coproc/types.h"
#include "coproc/wasm_event.h"
#include "storage/directories.h"
#include "utils/file_io.h"
#include "utils/unresolved_address.h"
#include "vassert.h"
#include "vlog.h"

#include <seastar/core/future.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/sstring.hh>
#include <seastar/net/socket_defs.hh>

namespace coproc {

using namespace std::chrono_literals;

/// Either writes or removes the file from disk. In either case, logs warning if
/// the current state of the filesystem represents an unexpected issue
ss::future<> wasm_event_listener::resolve_wasm_script(wasm_script_action wsa) {
    std::filesystem::path active_path(_active_dir / wsa.name.c_str());
    return ss::file_exists(active_path.string())
      .then([this, active_path, wsa = std::move(wsa)](bool exists) mutable {
          if (wsa.action() == wasm_event_action::remove) {
              if (!exists) {
                  vlog(
                    coproclog.warn,
                    "Attempt to remove coprocessor {} but it doesn't exist in "
                    "the active directory {}",
                    wsa.name,
                    active_path);
                  return ss::now();
              }
              std::filesystem::path mvto(_inactive_dir / wsa.name.c_str());
              vlog(
                coproclog.info,
                "Moving coprocessor {} to inactive dir",
                active_path);
              return ss::rename_file(active_path.string(), mvto.string());
          } else {
              if (exists) {
                  vlog(
                    coproclog.warn,
                    "Attempt to deploy coprocessor with script {} but one "
                    "already exists: {}",
                    wsa.name,
                    active_path);
                  return ss::now();
              }
              vlog(
                coproclog.info,
                "Writing coprocessor {} to submit dir",
                _submit_dir);
              std::filesystem::path submit_src(_submit_dir / wsa.name.c_str());
              return write_fully(submit_src, std::move(wsa.source_code));
          }
      });
}

wasm_event_listener::wasm_event_listener(std::filesystem::path wasm_root)
  : _client(
    std::vector<unresolved_address>{unresolved_address("127.0.0.1", 9092)})
  , _wasm_root(std::move(wasm_root))
  , _active_dir(_wasm_root / "active")
  , _submit_dir(_wasm_root / "submit")
  , _inactive_dir(_wasm_root / "inactive") {}

void wasm_event_listener::start() {
    if (_gate.is_closed() || _gate.get_count() != 0) {
        throw std::logic_error(
          "Attempted to start() the wasm_event_notifier run "
          "loop, after it has already been started or closed");
    }
    (void)ss::with_gate(_gate, [this] {
        return storage::directories::initialize(_active_dir.string())
          .then([this] {
              return storage::directories::initialize(_inactive_dir.string());
          })
          .then([this] {
              return storage::directories::initialize(_submit_dir.string());
          })
          .then([this] { return _client.connect(); })
          .then([this] {
              return ss::do_until(
                [this] { return _abort_source.abort_requested(); },
                [this] { return do_start(); });
          });
    });
}

ss::future<> wasm_event_listener::do_start() {
    /// This method performs the main polling behavior. Within a repeat loop it
    /// will poll from data until it cannot poll anymore. Normally we would be
    /// concerned about keeping all of this data in memory, however the topic is
    /// compacted, we don't expect the size of unique records to be very big.
    return ss::do_with(
      model::record_batch_reader::data_t(),
      [this](model::record_batch_reader::data_t& events) {
          /// After polling for data, the events are mapped to
          /// 'wasm_event_action' types after passing verification
          return ss::repeat([this, &events] { return poll_topic(events); })
            .then([this, &events] { return process_wasm_events(events); })
            .then([this](std::vector<wasm_script_action> wsas) {
                return ss::do_with(
                  std::move(wsas),
                  [this](std::vector<wasm_script_action>& wsas) {
                      return ss::do_for_each(
                        wsas, [this](wasm_script_action& wsa) {
                            /// Finally the action is performed, either the
                            /// source is written to disk or a file is deleted
                            /// from disk
                            return resolve_wasm_script(std::move(wsa));
                        });
                  });
            })
            .then([this] {
                /// Every 2s the loop will pause, always looping attempting to
                /// deploy or remove the next coprocessor script
                return ss::sleep_abortable(2s, _abort_source)
                  .handle_exception([](std::exception_ptr eptr) {
                      vlog(
                        coproclog.debug, "Exited sleep call early: {}", eptr);
                  });
            });
      });
}

/// Map a model::record_batch -> std::vector<wasm_script_action>
/// The transform will only occur after the record has passed all validators
std::vector<wasm_script_action> wasm_event_listener::process_wasm_events(
  model::record_batch_reader::data_t& events) {
    std::vector<wasm_script_action> wsas;
    for (auto& record_batch : events) {
        record_batch.for_each_record([&wsas](model::record r) {
            wasm_event_errc mb_error = wasm_event_validate(r);
            if (mb_error != wasm_event_errc::none) {
                vlog(
                  coproclog.error,
                  "Erranous coproc record detected, issue: {}",
                  mb_error);
                return;
            }
            auto id = wasm_event_get_id(r);
            wsas.emplace_back(wasm_script_action{
              .name = std::move(*id), .source_code = r.share_value()});
        });
    }
    return wsas;
}

ss::future<ss::stop_iteration>
wasm_event_listener::poll_topic(model::record_batch_reader::data_t& events) {
    return _client.fetch_partition(_coproc_internal_tp, _offset, 64_KiB, 5s)
      .then([this, &events](kafka::fetch_response response) {
          if (
            (response.error != kafka::error_code::none)
            || _abort_source.abort_requested()) {
              return ss::stop_iteration::yes;
          }
          vassert(response.partitions.size() == 1, "Unexpected partition size");
          auto& p = response.partitions[0];
          vassert(p.name == _coproc_internal_tp.topic, "Unexpected topic name");
          vassert(p.responses.size() == 1, "Unexpected responses size");
          auto& pr = p.responses[0];
          if (!pr.has_error()) {
              iobuf record_set(std::move(*pr.record_set));
              while (!record_set.empty()) {
                  kafka::kafka_batch_adapter kba;
                  record_set = kba.adapt(std::move(record_set));
                  if (!kba.batch || !kba.valid_crc) {
                      vlog(coproclog.error, "Invalid batch and/or crc");
                      continue;
                  }
                  vassert(kba.batch, "kbas batch is nullopt");
                  events.push_back(std::move(*kba.batch));
                  /// Update so subsequent reads start at the correct offset
                  _offset = ++events.back().last_offset();
              }
          }
          /// The end of the log has been reached, exit the ss::repeat() loop
          /// that this method is executing within
          return (_offset >= pr.last_stable_offset) ? ss::stop_iteration::yes
                                                    : ss::stop_iteration::no;
      });
};

} // namespace coproc
