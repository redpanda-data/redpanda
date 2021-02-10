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
#include "ssx/future-util.h"
#include "storage/parser_utils.h"
#include "utils/file_io.h"
#include "utils/unresolved_address.h"
#include "vassert.h"
#include "vlog.h"

#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/sstring.hh>
#include <seastar/net/socket_defs.hh>

namespace coproc {

using namespace std::chrono_literals;

static wasm::event_action query_action(const iobuf& source_code) {
    /// If this came from a remove event, the validator would
    /// have failed if the value() field of the record wasn't
    /// empty. Therefore checking if this iobuf is empty is a
    /// certain way to know if the intended request was to
    /// deploy or remove
    return source_code.empty() ? wasm::event_action::remove
                               : wasm::event_action::deploy;
}

ss::future<> wasm_event_listener::stop() {
    _abort_source.request_abort();
    return _gate.close().then([this] { return _client.stop(); });
}

/// Either writes or removes the file from disk. In either case, logs warning if
/// the current state of the filesystem represents an unexpected issue
ss::future<>
wasm_event_listener::resolve_wasm_script(ss::sstring name, iobuf source_code) {
    std::filesystem::path active_path(_active_dir / name.c_str());
    return ss::file_exists(active_path.string())
      .then([this,
             active_path,
             name = std::move(name),
             source_code = std::move(source_code)](bool exists) mutable {
          if (query_action(source_code) == wasm::event_action::remove) {
              if (!exists) {
                  vlog(
                    coproclog.warn,
                    "Attempt to remove coprocessor {} but it doesn't exist in "
                    "the active directory {}",
                    name,
                    active_path);
                  return ss::now();
              }
              std::filesystem::path mvto(_inactive_dir / name.c_str());
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
                    name,
                    active_path);
                  return ss::now();
              }
              vlog(
                coproclog.info,
                "Writing coprocessor {} to submit dir {}",
                name,
                _submit_dir);
              std::filesystem::path submit_src(_submit_dir / name.c_str());
              return write_fully(submit_src, std::move(source_code));
          }
      });
}

wasm_event_listener::wasm_event_listener(std::filesystem::path data_dir)
  : _client(
    std::vector<unresolved_address>{unresolved_address("127.0.0.1", 9092)})
  , _wasm_root(data_dir.parent_path() / "coprocessor")
  , _active_dir(_wasm_root / "active")
  , _submit_dir(_wasm_root / "submit")
  , _inactive_dir(_wasm_root / "inactive") {}

ss::future<> wasm_event_listener::start() {
    if (_gate.is_closed() || _gate.get_count() != 0) {
        throw std::logic_error(
          "Attempted to start() the wasm_event_notifier run "
          "loop, after it has already been started or closed");
    }
    return ss::recursive_touch_directory(_active_dir.string())
      .then([this] {
          return ss::recursive_touch_directory(_inactive_dir.string());
      })
      .then(
        [this] { return ss::recursive_touch_directory(_submit_dir.string()); })
      .then([this] {
          (void)ss::with_gate(_gate, [this] {
              return _client.connect()
                .then([this] {
                    return ss::do_until(
                      [this] { return _abort_source.abort_requested(); },
                      [this] { return do_start(); });
                })
                .handle_exception([](const std::exception_ptr& e) {
                    vlog(
                      coproclog.debug,
                      "Error connecting to kafka endpoint - {}",
                      e);
                });
          });
      });
}

static ss::future<std::vector<model::record_batch>>
decompress_wasm_events(model::record_batch_reader::data_t events) {
    return ssx::parallel_transform(
      std::move(events), [](model::record_batch&& rb) {
          /// If batch isn't compressed, returns 'rb'
          return storage::internal::decompress_batch(std::move(rb));
      });
}

ss::future<> wasm_event_listener::do_start() {
    /// This method performs the main polling behavior. Within a repeat loop it
    /// will poll from data until it cannot poll anymore. Normally we would be
    /// concerned about keeping all of this data in memory, however the topic is
    /// compacted, we don't expect the size of unique records to be very big.
    using wasm_script_actions = absl::btree_map<ss::sstring, iobuf>;
    return ss::do_with(
      model::record_batch_reader::data_t(),
      [this](model::record_batch_reader::data_t& events) {
          /// The stop condition is met when its been detected that the stored
          /// offset has not moved.
          return ss::repeat([this, &events] {
                     model::offset initial_offset = _offset;
                     return poll_topic(events).then([this, initial_offset] {
                         return initial_offset == _offset
                                  ? ss::stop_iteration::yes
                                  : ss::stop_iteration::no;
                     });
                 })
            .then(
              [&events] { return decompress_wasm_events(std::move(events)); })
            .then([](std::vector<model::record_batch> events) {
                return wasm::reconcile_events(std::move(events));
            })
            .then([this](wasm_script_actions wsas) {
                return ss::do_with(
                  std::move(wsas), [this](wasm_script_actions& wsas) {
                      return ss::do_for_each(
                        wsas, [this](wasm_script_actions::value_type& wsa) {
                            /// Finally the action is performed, either the
                            /// source is written to disk or a file is deleted
                            /// from disk
                            return resolve_wasm_script(
                              wsa.first, std::move(wsa.second));
                        });
                  });
            })
            .then([this] { return ss::sleep_abortable(2s, _abort_source); })
            .handle_exception([](std::exception_ptr eptr) {
                vlog(coproclog.debug, "Exited sleep early: {}", eptr);
            });
      });
}

ss::future<>
wasm_event_listener::poll_topic(model::record_batch_reader::data_t& events) {
    return _client.fetch_partition(_coproc_internal_tp, _offset, 64_KiB, 5s)
      .then([this, &events](kafka::fetch_response response) {
          if (
            (response.error != kafka::error_code::none)
            || _abort_source.abort_requested()) {
              return ss::now();
          }
          vassert(response.partitions.size() == 1, "Unexpected partition size");
          auto& p = response.partitions[0];
          vassert(p.name == _coproc_internal_tp.topic, "Unexpected topic name");
          vassert(p.responses.size() == 1, "Unexpected responses size");
          auto& pr = p.responses[0];
          if (!pr.has_error()) {
              auto crs = kafka::batch_reader(std::move(*pr.record_set));
              while (!crs.empty()) {
                  auto kba = crs.consume_batch();
                  if (!kba.v2_format || !kba.valid_crc || !kba.batch) {
                      vlog(
                        coproclog.warn,
                        "Invalid batch pushed to internal wasm topic");
                      continue;
                  }
                  events.push_back(std::move(*kba.batch));
                  /// Update so subsequent reads start at the correct offset
                  _offset = events.back().last_offset() + model::offset(1);
              }
          }
          return ss::now();
      });
};

} // namespace coproc
