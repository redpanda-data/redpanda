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

inline ss::sstring manifest_key_to_string(const manifest::key& name) {
    ss::sstring result;
    if (std::holds_alternative<segment_name>(name)) {
        result = std::get<segment_name>(name)();
    } else if (std::holds_alternative<remote_segment_path>(name)) {
        auto tmp = std::get<remote_segment_path>(name)();
        result = tmp.string();
    }
    return result;
}

/// This function returns segment base offset as kafka offset
static model::offset get_kafka_base_offset(const manifest::segment_meta& m) {
    auto delta = std::clamp(
      m.delta_offset, model::offset(0), model::offset::max());
    return m.base_offset - delta;
}

/// This function returns segment max offset as kafka offset.
/// It doesn't return precise value, only the estimate. But the
/// error is limited by number of configuration batches inside the
/// segment.
static model::offset
estimate_kafka_max_offset(const manifest::segment_meta& m) {
    auto delta = std::clamp(
      m.delta_offset, model::offset(0), model::offset::max());
    return m.committed_offset - delta;
}

class record_batch_reader_impl final : public model::record_batch_reader::impl {
    using remote_segment_list_t = std::vector<std::unique_ptr<remote_segment>>;
    using remote_segment_iterator = remote_segment_list_t::iterator;

public:
    explicit record_batch_reader_impl(
      const log_reader_config& config,
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

    ~record_batch_reader_impl() override = default;
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
            _partition->return_reader(std::move(_state), _it->second);
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
                    _state->config.start_offset_redpanda
                      = _state->reader->max_rp_offset() + model::offset(1);
                    // Next iteration will trigger transition in
                    // 'maybe_reset_reader'
                    continue;
                } else if (!result) {
                    vlog(cst_log.debug, "Unexpected error");
                    throw std::system_error(result.error());
                }
                // empty result will also be propagated here
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
    // Initialize object using remote_partition as a source
    void initialize_reader_state(const log_reader_config& config) {
        vlog(cst_log.debug, "record_batch_reader_impl initialize reader state");
        auto lookup_result = find_cached_reader(config);
        if (lookup_result) {
            auto&& [state, it] = lookup_result.value();
            _state = std::move(state);
            _it = it;
            vlog(cst_log.debug, "record_batch_reader_impl reader initialized ");
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
    find_cached_reader(const log_reader_config& config) {
        if (!_partition) {
            return std::nullopt;
        }
        // NOTE: config.start_offset is a kafka offset, the _segments collection
        // contains estimated kafka offsets of segment committed offsets. This
        // offsets can be larger than the real value. But the error is limited
        // by the size of the individual segment. We can still have errors on
        // segment boundaries. In case of error the search will return previous
        // segment. The reader will scan and skip all batches from that segment.
        // To prevent this we double check the start_offset over the base-offset
        // of the next segment (which can be known precisely).
        auto it = _partition->_segments.lower_bound(config.start_offset);
        if (it != _partition->_segments.end()) {
            if (auto tmp = it; ++tmp != _partition->_segments.end()) {
                auto key = std::visit(
                  [](auto&& st) { return st->manifest_key; }, tmp->second);
                const auto meta = _partition->_manifest.get(key);
                vassert(
                  meta != nullptr,
                  "Manifest doesn't have key {}",
                  manifest_key_to_string(key));
                auto base = get_kafka_base_offset(*meta);
                if (base <= config.start_offset) {
                    it = tmp;
                }
            }
            auto reader = _partition->borrow_reader(
              config, it->first, it->second);
            // Here we know the exact type of the reader_state because of
            // the invariant of the borrow_reader
            const auto& segment
              = std::get<remote_partition::materialized_segment_ptr>(it->second)
                  ->segment;
            vlog(
              cst_log.debug,
              "segment offset range {}-{}, delta: {}",
              segment->get_base_rp_offset(),
              segment->get_max_rp_offset(),
              segment->get_base_offset_delta());
            if (segment->get_base_kafka_offset() == config.start_offset) {
                // prefetch if on the segment's boundary
                _partition->start_readahead(it);
            }
            return {{.reader_state = std::move(reader), .iter = it}};
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
        vlog(
          cst_log.debug,
          "maybe_reset_reader, config start_offset_redpanda: {}, reader "
          "max_offset: {}",
          _state->config.start_offset_redpanda,
          _state->reader->max_rp_offset());
        if (
          _state->config.start_offset_redpanda
          > _state->reader->max_rp_offset()) {
            // move to the next segment
            vlog(cst_log.debug, "maybe_reset_stream condition triggered");
            _it++;
            if (_it == _partition->_segments.end()) {
                co_await set_end_of_stream();
            } else {
                // reuse state but replace the reader
                _partition->evict_reader(std::move(_state->reader));
                vlog(cst_log.debug, "initializing new log reader");
                _state = _partition->borrow_reader(
                  _state->config, _it->first, _it->second);
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
        auto tmp = std::move(_state->reader);
        co_await tmp->stop();
        _it = _partition->_segments.end();
        _state = {};
    }

    ss::weak_ptr<remote_partition> _partition;
    /// Currently accessed segment
    remote_partition::segment_map_t::iterator _it;
    /// Reader state that was borrowed from the materialized_segment_state
    std::unique_ptr<remote_partition::reader_state> _state;
    /// Cancelation subscription
    ss::abort_source::subscription _as_sub;
};

remote_partition::remote_partition(
  const manifest& m, remote& api, cache& c, s3::bucket_name bucket)
  : _rtc()
  , _ctxlog(cst_log, _rtc, m.get_ntp().path())
  , _api(api)
  , _cache(c)
  , _manifest(m)
  , _bucket(std::move(bucket))
  , _stm_jitter(stm_jitter_duration) {}

ss::future<> remote_partition::start() {
    update_segmnets_incrementally();
    (void)run_eviction_loop();

    _stm_timer.set_callback([this] {
        gc_stale_materialized_segments();
        if (!_gate.is_closed()) {
            _stm_timer.rearm(_stm_jitter());
        }
    });
    _stm_timer.rearm(_stm_jitter());
    co_return;
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
        for (auto& rs : tmp_list) {
            co_await std::visit([](auto&& rs) { return rs->stop(); }, rs);
        }
    }
    vlog(_ctxlog.debug, "remote partition eviction loop stopped");
}

void remote_partition::start_readahead(
  remote_partition::segment_map_t::iterator current) {
    struct visit_hydrate {
        segment_state& state;
        remote_partition* part;
        const model::offset offset_key;

        void operator()(offloaded_segment_ptr& st) {
            vlog(
              cst_log.debug,
              "remote partition readahead, hydrating {}",
              manifest_key_to_string(st->manifest_key));
            auto tmp = st->materialize(*part, offset_key);
            (void)tmp->segment->hydrate().discard_result();
            state = std::move(tmp);
        }
        void operator()(materialized_segment_ptr&) {
            // The segment was accessed recently so it should be already
            // hydrated
        }
    };
    // prefetch if on the segment's boundary
    auto pi = current;
    auto readahead = stm_readahead;
    while (readahead--) {
        pi++;
        if (pi != _segments.end()) {
            std::visit(
              visit_hydrate{
                .state = pi->second, .part = this, .offset_key = pi->first},
              pi->second);
        } else {
            break;
        }
    }
}

void remote_partition::gc_stale_materialized_segments() {
    vlog(
      _ctxlog.debug,
      "collecting stale materialized segments, {} segments materialized, {} "
      "segments total",
      _materialized.size(),
      _segments.size());
    auto now = ss::lowres_clock::now();
    std::vector<model::offset> offsets;
    for (auto& st : _materialized) {
        if (now - st.atime > stm_max_idle_time) {
            vlog(
              _ctxlog.debug,
              "reader for segment with base offset {} is stale",
              st.offset_key);
            // this will delete and unlink the object from
            // _materialized collection
            offsets.push_back(st.offset_key);
        }
    }
    vlog(_ctxlog.debug, "found {} eviction candidates ", offsets.size());
    for (auto o : offsets) {
        vlog(_ctxlog.debug, "about to offload segment {}", o);
        auto it = _segments.find(o);
        vassert(it != _segments.end(), "Can't find offset {}", o);
        auto tmp = std::visit(
          [this](auto&& st) { return st->offload(this); }, _segments[o]);
        _segments[o] = std::move(tmp);
    }
}

model::offset remote_partition::first_uploaded_offset() {
    vlog(_ctxlog.debug, "remote partition first_uploaded_offset");
    if (_manifest.size() == 0) {
        return model::offset(0);
    }
    try {
        if (_first_uploaded_offset) {
            return *_first_uploaded_offset;
        }
        model::offset starting_offset = model::offset::max();
        for (const auto& m : _manifest) {
            starting_offset = std::min(
              starting_offset, get_kafka_base_offset(m.second));
            vlog(
              _ctxlog.debug,
              "remote partition first_uploaded_offset .. {}",
              starting_offset);
        }
        _first_uploaded_offset = starting_offset;
        vlog(
          _ctxlog.debug,
          "remote partition first_uploaded_offset set to {}",
          starting_offset);
        return starting_offset;
    } catch (...) {
        vlog(
          _ctxlog.error,
          "remote partition first_uploaded_offset error {}",
          std::current_exception());

        throw;
    }
}

ss::future<> remote_partition::stop() {
    vlog(_ctxlog.debug, "remote partition stop {} segments", _segments.size());
    _stm_timer.cancel();
    _cvar.broken();

    for (auto& [offset, seg] : _segments) {
        vlog(_ctxlog.debug, "remote partition stop {}", offset);
        co_await std::visit([](auto&& st) { return st->stop(); }, seg);
    }

    co_await _gate.close();
}

void remote_partition::update_segmnets_incrementally() {
    vlog(_ctxlog.debug, "remote partition update segments incrementally");
    // find new segments
    for (const auto& meta : _manifest) {
        auto o = estimate_kafka_max_offset(meta.second);
        if (_segments.contains(o)) {
            continue;
        }

        _segments.insert(std::make_pair(
          o, std::make_unique<offloaded_segment_state>(meta.first)));
    }
}

/// Materialize segment if needed and create a reader
std::unique_ptr<remote_partition::reader_state> remote_partition::borrow_reader(
  log_reader_config config, model::offset key, segment_state& st) {
    struct visit_materialize_make_reader {
        segment_state& state;
        remote_partition* part;
        const log_reader_config& config;
        const model::offset offset_key;

        std::unique_ptr<reader_state> operator()(offloaded_segment_ptr& st) {
            auto tmp = st->materialize(*part, offset_key);
            auto res = tmp->borrow_reader(config);
            state = std::move(tmp);
            return res;
        }
        std::unique_ptr<reader_state> operator()(materialized_segment_ptr& st) {
            return st->borrow_reader(config);
        }
    };
    return std::visit(
      visit_materialize_make_reader{
        .state = st, .part = this, .config = config, .offset_key = key},
      st);
}

/// Return reader back to segment_state
void remote_partition::return_reader(
  std::unique_ptr<reader_state> rs, segment_state& st) {
    struct visit_return_reader {
        segment_state& state;
        remote_partition* part;
        std::unique_ptr<reader_state> rst;

        void operator()(offloaded_segment_ptr&) {
            part->evict_reader(std::move(rst->reader));
        }
        void operator()(materialized_segment_ptr& st) {
            st->return_reader(std::move(rst));
        }
    };
    std::visit(
      visit_return_reader{.state = st, .part = this, .rst = std::move(rs)}, st);
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
      log_reader_config(config), weak_from_this());
    model::record_batch_reader rdr(std::move(impl));
    co_return rdr;
}

remote_partition::reader_state::reader_state(const log_reader_config& cfg)
  : config(cfg)
  , reader(nullptr) {}

ss::future<> remote_partition::reader_state::stop() {
    if (reader) {
        co_await reader->stop();
        reader.reset();
    }
}

remote_partition::offloaded_segment_state::offloaded_segment_state(
  manifest::key key)
  : manifest_key(std::move(key)) {}

std::unique_ptr<remote_partition::materialized_segment_state>
remote_partition::offloaded_segment_state::materialize(
  remote_partition& p, model::offset offset_key) {
    auto st = std::make_unique<materialized_segment_state>(
      manifest_key, offset_key, p);
    return st;
}

ss::future<> remote_partition::offloaded_segment_state::stop() {
    return ss::now();
}

std::unique_ptr<remote_partition::offloaded_segment_state>
remote_partition::offloaded_segment_state::offload(remote_partition*) {
    auto st = std::make_unique<offloaded_segment_state>(manifest_key);
    return st;
}

remote_partition::materialized_segment_state::materialized_segment_state(
  manifest::key mk, model::offset off_key, remote_partition& p)
  : manifest_key(std::move(mk))
  , offset_key(off_key)
  , segment(std::make_unique<remote_segment>(
      p._api, p._cache, p._bucket, p._manifest, manifest_key, p._rtc))
  , atime(ss::lowres_clock::now()) {
    p._materialized.push_back(*this);
}

void remote_partition::materialized_segment_state::return_reader(
  std::unique_ptr<reader_state> state) {
    atime = ss::lowres_clock::now();
    readers.push_back(std::move(state));
}

/// Borrow reader or make a new one.
/// In either case return a reader.
std::unique_ptr<remote_partition::reader_state>
remote_partition::materialized_segment_state::borrow_reader(
  const log_reader_config& cfg) {
    atime = ss::lowres_clock::now();
    for (auto it = readers.begin(); it != readers.end(); it++) {
        if ((*it)->config.start_offset == cfg.start_offset) {
            // here we're reusing the existing reader
            auto tmp = std::move(*it);
            tmp->config = cfg;
            readers.erase(it);
            return tmp;
        }
    }
    // this may only happen if we have some concurrency
    auto state = std::make_unique<reader_state>(cfg);
    state->reader = std::make_unique<remote_segment_batch_reader>(
      *segment, state->config, segment->get_term());
    return state;
}

ss::future<> remote_partition::materialized_segment_state::stop() {
    for (auto& rs : readers) {
        if (rs->reader) {
            co_await rs->reader->stop();
        }
    }
    co_await segment->stop();
}

std::unique_ptr<remote_partition::offloaded_segment_state>
remote_partition::materialized_segment_state::offload(
  remote_partition* partition) {
    _hook.unlink();
    for (auto&& rs : readers) {
        partition->evict_reader(std::move(rs->reader));
    }
    partition->evict_segment(std::move(segment));
    auto st = std::make_unique<offloaded_segment_state>(manifest_key);
    return st;
}

} // namespace cloud_storage
