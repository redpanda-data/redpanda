/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/remote_partition.h"

#include "cloud_storage/logger.h"
#include "cloud_storage/offset_translation_layer.h"
#include "cloud_storage/remote_segment.h"
#include "cloud_storage/types.h"
#include "storage/parser_errc.h"
#include "storage/types.h"
#include "utils/retry_chain_node.h"
#include "utils/stream_utils.h"

#include <seastar/core/circular_buffer.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/queue.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/temporary_buffer.hh>

#include <chrono>
#include <exception>

using namespace std::chrono_literals;

namespace cloud_storage {

using data_t = model::record_batch_reader::data_t;
using foreign_data_t = model::record_batch_reader::foreign_data_t;
using storage_t = model::record_batch_reader::storage_t;

class record_batch_reader_impl final : public model::record_batch_reader::impl {
    using remote_segment_list_t = std::vector<std::unique_ptr<remote_segment>>;
    using remote_segment_iterator = remote_segment_list_t::iterator;

public:
    explicit record_batch_reader_impl(
      const storage::log_reader_config& config,
      ss::weak_ptr<remote_partition> part) noexcept
      : _partition(std::move(part))
      , _it(_partition->_segments.begin()) {
        vlog(cst_log.debug, "record_batch_reader_impl c-tor");
        if (config.abort_source) {
            vlog(cst_log.debug, "abort_source is set");
            auto sub = config.abort_source->get().subscribe(
              [this]() noexcept -> ss::future<> {
                  vlog(cst_log.debug, "Abort requested");
                  co_await set_end_of_stream();
              });
            if (sub) {
                _as_sub = std::move(*sub);
            } else {
                vlog(cst_log.debug, "abort_source is triggered in c-tor");
                _it = _partition->_segments.end();
                _state = {};
            }
        }
        if (!is_end_of_stream()) {
            initialize_reader_state(config);
        }
    }

    // ~record_batch_reader_impl() override = default;
    ~record_batch_reader_impl() {
        vlog(cst_log.debug, "record_batch_reader_impl d-tor");
    }
    record_batch_reader_impl(record_batch_reader_impl&& o) noexcept = delete;
    record_batch_reader_impl&
    operator=(record_batch_reader_impl&& o) noexcept = delete;
    record_batch_reader_impl(const record_batch_reader_impl& o) = delete;
    record_batch_reader_impl& operator=(const record_batch_reader_impl& o)
      = delete;

    bool is_end_of_stream() const override {
        return _it == _partition->_segments.end();
    }

    ss::future<storage_t>
    do_load_slice(model::timeout_clock::time_point deadline) override {
        if (is_end_of_stream()) {
            vlog(
              cst_log.debug, "record_batch_reader_impl do_load_slize - empty");
            co_return storage_t{};
        }
        if (_state->config.over_budget) {
            vlog(cst_log.debug, "We're overbudget, stopping");
            // We need to stop in such way that will keep the
            // reader in the reusable state, so we could reuse
            // it on next itertaion

            // The existing state have to be rebuilt
            if (_it->second.reader) {
                // we have some other reader which have to be removed
                auto rdr = std::move(_it->second.reader->reader);
                _partition->evict_reader(std::move(rdr));
            }
            _state->atime = ss::lowres_clock::now();
            _it->second.reader = std::move(_state);
            _it = _partition->_segments.end();
            co_return storage_t{};
        }
        while (_state) {
            if (co_await maybe_reset_reader()) {
                vlog(
                  cst_log.debug, "Invoking 'read_some' on current log reader");
                auto result = co_await _state->reader->read_some(deadline);
                if (
                  !result
                  && result.error() == storage::parser_errc::end_of_stream) {
                    vlog(cst_log.debug, "EOF error while reading from stream");
                    if (
                      _state->config.start_offset
                      < _state->reader->max_offset()) {
                        _state->config.start_offset
                          = _state->reader->max_offset() + model::offset(1);
                    }
                    // Next iteration will trigger transition in
                    // 'maybe_reset_reader'
                    continue;
                } else if (!result) {
                    vlog(cst_log.debug, "Unexpected error");
                    throw std::system_error(result.error());
                }
                data_t d = std::move(result.value());
                co_return storage_t{std::move(d)};
            }
        }
        vlog(
          cst_log.debug,
          "EOS reached {} {}",
          static_cast<bool>(_state),
          is_end_of_stream());
        co_return storage_t{};
    }

    void print(std::ostream& o) override {
        o << "cloud_storage_record_batch_reader";
    }

private:
    void initialize_reader_state(const storage::log_reader_config& config) {
        vlog(cst_log.debug, "record_batch_reader_impl initialize reader state");
        auto lookup_result = find_cached_reader(config);
        if (lookup_result) {
            auto&& [state, it] = lookup_result.value();
            _it = it;
            if (state) {
                vlog(
                  cst_log.debug,
                  "record_batch_reader_impl initialize reader state - reusing "
                  "reader");
                // We're reusing existing reader state
                _state = std::move(state);
                _state->config = config;
                _state->atime = ss::lowres_clock::now();
            } else {
                vlog(
                  cst_log.debug,
                  "record_batch_reader_impl initialize reader state - creating "
                  "reader");
                auto tmp = std::make_unique<remote_partition::reader_state>(
                  config);
                tmp->reader = std::make_unique<remote_segment_batch_reader>(
                  *_it->second.segment.get(),
                  tmp->config,
                  _it->second.segment->get_term());
                _state = std::move(tmp);
            }
            return;
        }
        vlog(
          cst_log.debug,
          "record_batch_reader_impl initialize reader state - segment not "
          "found");
        _it = _partition->_segments.end();
        _state = {};
    }

    struct cache_reader_lookup_result {
        std::unique_ptr<remote_partition::reader_state> reader_state;
        remote_partition::segment_map_t::iterator iter;
    };

    std::optional<cache_reader_lookup_result>
    find_cached_reader(const storage::log_reader_config& config) {
        if (!_partition) {
            return std::nullopt;
        }
        // auto it = _partition->_segments.begin();
        auto it = _partition->_segments.lower_bound(config.start_offset);
        if (
          it->first != config.start_offset
          && it != _partition->_segments.begin()) {
            --it;
        }
        while (it != _partition->_segments.end()) {
            auto segment = it->second.segment;
            vlog(
              cst_log.debug,
              "segment offset range {}-{}",
              segment->get_base_offset(),
              segment->get_max_offset());
            if (
              segment->get_base_offset() <= config.start_offset
              && segment->get_max_offset() >= config.start_offset) {
                auto& reader_state = it->second.reader;
                auto is_valid = it->second.reader && reader_state->reader;
                if (is_valid) {
                    vlog(
                      cst_log.debug,
                      "found a valid reader with start_offset={}",
                      reader_state->config.start_offset);
                    auto same_start_offset = reader_state->config.start_offset
                                             == config.start_offset;
                    if (same_start_offset) {
                        // We have found a matching reader
                        return {
                          {.reader_state = std::move(reader_state),
                           .iter = it}};
                    }
                }
                // Only iterator can be returned
                return {{.reader_state = nullptr, .iter = it}};
            }
            ++it;
        }
        return std::nullopt;
    }

    /// Reset reader if current segment is fully consumed.
    /// The object may transition onto a next segment or
    /// it will transtion into completed state with no reader
    /// attached.
    ss::future<bool> maybe_reset_reader() {
        vlog(cst_log.debug, "maybe_reset_reader called");
        if (!_state) {
            co_return false;
        }
        if (_state->config.start_offset > _state->config.max_offset) {
            vlog(
              cst_log.debug,
              "maybe_reset_stream called - stream already consumed, start "
              "{}, "
              "max {}",
              _state->config.start_offset,
              _state->config.max_offset);
            // Entire range is consumed, detach from remote_partition and
            // close the reader.
            co_await set_end_of_stream();
            co_return false;
        }
        if (_state->config.start_offset > _state->reader->max_offset()) {
            // move to the next segment
            vlog(cst_log.debug, "maybe_reset_stream condition triggered");
            auto tmp_it = _it;
            tmp_it++;
            if (tmp_it == _partition->_segments.end()) {
                co_await set_end_of_stream();
            } else {
                stop_tracking();
                _it++;
                // reuse state but replace the reader
                _partition->evict_reader(std::move(_state->reader));
                vlog(cst_log.debug, "initializing new log reader");
                _state->reader = std::make_unique<remote_segment_batch_reader>(
                  *_it->second.segment,
                  _state->config,
                  _it->second.segment->get_term());
                // _it->second.reader = std::move(_state);
            }
        }
        vlog(
          cst_log.debug,
          "maybe_reset_stream completed {} {}",
          static_cast<bool>(_state),
          is_end_of_stream());
        co_return static_cast<bool>(_state);
    }

    /// Transition reader to the completed state. Stop tracking state in
    /// the 'remote_partition'
    ss::future<> set_end_of_stream() {
        stop_tracking();
        auto tmp = std::move(_state->reader);
        co_await tmp->close();
        _it = _partition->_segments.end();
        _state = {};
    }

    /// Return 'true' if the state of the current reader is being tracked by
    /// the 'remote_partition'
    bool is_tracked() const { return _it->second.reader == _state; }

    /// Stop tracking current state inside the remote_partition
    void stop_tracking() {
        if (_state && is_tracked()) {
            _it->second.reader = {};
        }
    }

    ss::weak_ptr<remote_partition> _partition;
    /// Currently accessed segment
    remote_partition::segment_map_t::iterator _it;
    /// Cancelation subscription
    ss::abort_source::subscription _as_sub;
    /// Reader state which is recreated on every iteration
    std::unique_ptr<remote_partition::reader_state> _state;
};

remote_partition::remote_partition(
  const manifest& m, remote& api, cache& c, s3::bucket_name bucket)
  : _rtc()
  , _ctxlog(cst_log, _rtc, m.get_ntp().path())
  , _api(api)
  , _cache(c)
  , _manifest(m)
  , _translator(ss::make_lw_shared<offset_translator>())
  , _bucket(std::move(bucket)) {
    update_segmnets_incrementally();
    (void)run_eviction_loop();
}

ss::future<> remote_partition::run_eviction_loop() {
    // Evict readers asynchronously
    gate_guard g(_gate);
    while (!_gate.is_closed()) {
        try {
            co_await _cvar.wait(
              [this] { return _gate.is_closed() || !_eviction_list.empty(); });
        } catch (const ss::broken_condition_variable&) {
        }
        auto tmp_list = std::move(_eviction_list);
        for (auto& reader : tmp_list) {
            if (reader) {
                co_await reader->close();
            }
        }
    }
    vlog(_ctxlog.debug, "remote partition eviction loop stopped");
}

ss::future<> remote_partition::stop() {
    vlog(_ctxlog.debug, "remote partition stop {} segments", _segments.size());
    _cvar.broken();
    for (auto& [offset, seg] : _segments) {
        if (seg.reader && seg.reader->reader) {
            vlog(
              _ctxlog.debug,
              "remote partition stop reader for {}",
              seg.segment->get_base_offset());
            co_await seg.reader->reader->close();
            vlog(_ctxlog.debug, "remote partition stop reader done");
        }
        vlog(
          _ctxlog.debug,
          "remote partition stop segent {}",
          seg.segment->get_base_offset());
        co_await seg.segment->stop();
        vlog(_ctxlog.debug, "remote partition stop segent done");
    }
    co_await _gate.close();
}

void remote_partition::update_segmnets_incrementally() {
    vlog(_ctxlog.debug, "remote partition update segments incrementally");
    // (re)hydrate translator
    _translator->update(_manifest);
    // find new segments
    for (const auto& meta : _manifest) {
        auto b = meta.second.base_offset;
        if (_segments.contains(b)) {
            continue;
        }

        auto s = ss::make_lw_shared<remote_segment>(
          _api, _cache, _bucket, _manifest, meta.first, _rtc);

        _segments.insert(std::make_pair(
          b,
          remote_segment_state{
            .segment = std::move(s),
            .reader = nullptr,
          }));
    }
}

ss::future<model::record_batch_reader> remote_partition::make_reader(
  storage::log_reader_config config,
  std::optional<model::timeout_clock::time_point> deadline) {
    gate_guard g(_gate);
    vlog(
      _ctxlog.debug,
      "remote partition make_reader invoked, config: {}",
      config);
    vlog(
      _ctxlog.debug,
      "remote partition make_reader invoked, segments size: {}",
      _segments.size());
    if (_segments.size() < _manifest.size()) {
        update_segmnets_incrementally();
    }
    auto impl = std::make_unique<record_batch_reader_impl>(
      config, weak_from_this());
    model::record_batch_reader rdr(std::move(impl));
    co_return rdr;
}

} // namespace cloud_storage
