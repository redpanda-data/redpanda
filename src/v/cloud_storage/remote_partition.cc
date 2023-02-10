/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/remote_partition.h"

#include "cloud_storage/logger.h"
#include "cloud_storage/offset_translation_layer.h"
#include "cloud_storage/remote_segment.h"
#include "cloud_storage/types.h"
#include "storage/log_reader.h"
#include "storage/parser_errc.h"
#include "storage/types.h"
#include "utils/gate_guard.h"
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
#include <iterator>
#include <variant>

using namespace std::chrono_literals;

namespace cloud_storage {

using data_t = model::record_batch_reader::data_t;
using storage_t = model::record_batch_reader::storage_t;

/// This function returns segment base offset as kafka offset
static model::offset
get_kafka_base_offset(const partition_manifest::segment_meta& m) {
    // Manifests created with the old version of redpanda won't have the
    // delta_offset field. In this case the value will be initialized to
    // model::offset::min(). In this case offset translation couldn't be
    // performed.
    auto delta = m.delta_offset == model::offset::min() ? model::offset(0)
                                                        : m.delta_offset;
    return m.base_offset - delta;
}
/// This function returns segment max offset as kafka offset
static model::offset
get_kafka_max_offset(const partition_manifest::segment_meta& m) {
    // Manifests created with the old version of redpanda won't have the
    // delta_offset field. In this case the value will be initialized to
    // model::offset::min(). In this case offset translation couldn't be
    // performed.
    auto delta = m.delta_offset == model::offset::min() ? model::offset(0)
                                                        : m.delta_offset;
    return m.committed_offset - delta;
}

class partition_record_batch_reader_impl final
  : public model::record_batch_reader::impl {
public:
    explicit partition_record_batch_reader_impl(
      const storage::log_reader_config& config,
      ss::lw_shared_ptr<remote_partition> part,
      ss::lw_shared_ptr<storage::offset_translator_state> ot_state) noexcept
      : _ctxlog(cst_log, _rtc, part->get_ntp().path())
      , _partition(std::move(part))
      , _ot_state(std::move(ot_state))
      , _it(_partition->begin())
      , _end(_partition->end())
      , _gate_guard(_partition->_gate) {
        if (config.abort_source) {
            vlog(_ctxlog.debug, "abort_source is set");
            auto sub = config.abort_source->get().subscribe([this]() noexcept {
                vlog(_ctxlog.debug, "abort requested via config.abort_source");
                _partition->evict_reader(std::move(_reader));
                _it = _end;
            });
            if (sub) {
                _as_sub = std::move(*sub);
            } else {
                vlog(_ctxlog.debug, "abort_source is triggered in c-tor");
                _it = _end;
                _reader = {};
            }
        }
        if (!is_end_of_stream()) {
            initialize_reader_state(config);
        }
        _partition->_probe.reader_created();
    }

    ~partition_record_batch_reader_impl() noexcept override {
        _partition->_probe.reader_destroyed();
        if (_reader) {
            // We must not destroy this reader: it is not safe to do so
            // without calling stop() on it.  The remote_partition is
            // responsible for cleaning up readers, including calling
            // stop() on them in a background fiber so that we don't have to.
            // (https://github.com/redpanda-data/redpanda/issues/3378)
            vlog(
              _ctxlog.debug,
              "partition_record_batch_reader_impl::~ releasing reader on "
              "destruction");

            try {
                _partition->return_reader(std::move(_reader), _it->second);
            } catch (...) {
                // Failure to return the reader causes the reader destructor
                // to execute synchronously inside this function.  That might
                // succeed, if the reader was already stopped, so give it
                // a chance rather than asserting out here.
                vlog(
                  _ctxlog.error,
                  "partition_record_batch_reader_impl::~ exception while "
                  "releasing reader: {}",
                  std::current_exception());
            }
        }
    }
    partition_record_batch_reader_impl(
      partition_record_batch_reader_impl&& o) noexcept = delete;
    partition_record_batch_reader_impl&
    operator=(partition_record_batch_reader_impl&& o) noexcept = delete;
    partition_record_batch_reader_impl(
      const partition_record_batch_reader_impl& o)
      = delete;
    partition_record_batch_reader_impl&
    operator=(const partition_record_batch_reader_impl& o)
      = delete;

    bool is_end_of_stream() const override { return _it == _end; }

    ss::future<storage_t>
    do_load_slice(model::timeout_clock::time_point deadline) override {
        std::exception_ptr unknown_exception_ptr = nullptr;
        try {
            if (is_end_of_stream()) {
                vlog(
                  _ctxlog.debug,
                  "partition_record_batch_reader_impl do_load_slice - empty");
                co_return storage_t{};
            }
            if (_reader->config().over_budget) {
                vlog(_ctxlog.debug, "We're overbudget, stopping");
                // We need to stop in such way that will keep the
                // reader in the reusable state, so we could reuse
                // it on next itertaion

                // The existing state have to be rebuilt
                _partition->return_reader(std::move(_reader), _it->second);
                _it = _end;
                co_return storage_t{};
            }
            while (co_await maybe_reset_reader()) {
                if (_partition->_gate.is_closed()) {
                    co_await set_end_of_stream();
                    co_return storage_t{};
                }
                vlog(
                  _ctxlog.debug,
                  "Invoking 'read_some' on current log reader with config: {}",
                  _reader->config());
                auto result = co_await _reader->read_some(deadline, *_ot_state);
                if (!result) {
                    vlog(
                      _ctxlog.debug,
                      "Error while reading from stream '{}'",
                      result.error());
                    co_await set_end_of_stream();
                    throw std::system_error(result.error());
                }
                data_t d = std::move(result.value());
                for (const auto& batch : d) {
                    _partition->_probe.add_bytes_read(
                      batch.header().size_bytes);
                    _partition->_probe.add_records_read(batch.record_count());
                }
                co_return storage_t{std::move(d)};
            }
        } catch (const ss::gate_closed_exception&) {
            vlog(
              _ctxlog.debug,
              "gate_closed_exception while reading from remote_partition");
            _it = _end;
            _reader = {};
        } catch (const std::exception& e) {
            vlog(
              _ctxlog.warn,
              "exception thrown while reading from remote_partition: {}",
              e.what());
            unknown_exception_ptr = std::current_exception();
        }

        // The reader may have been left in an indeterminate state.
        // Re-set the pointer to it to ensure that it will not be reused.
        if (unknown_exception_ptr) {
            if (_reader) {
                co_await set_end_of_stream();
            }

            std::rethrow_exception(unknown_exception_ptr);
        }

        vlog(
          _ctxlog.debug,
          "EOS reached, reader available: {}, is end of stream: {}",
          static_cast<bool>(_reader),
          is_end_of_stream());
        co_return storage_t{};
    }

    void print(std::ostream& o) override {
        o << "cloud_storage_partition_record_batch_reader";
    }

private:
    // Initialize object using remote_partition as a source
    void initialize_reader_state(const storage::log_reader_config& config) {
        vlog(
          _ctxlog.debug,
          "partition_record_batch_reader_impl initialize reader state");
        auto lookup_result = find_cached_reader(config);
        if (lookup_result) {
            auto&& [reader, it] = lookup_result.value();
            _reader = std::move(reader);
            _it = it;
            return;
        }
        vlog(
          _ctxlog.debug,
          "partition_record_batch_reader_impl initialize reader state - "
          "segment not "
          "found");
        _it = _end;
        _reader = {};
    }

    struct cache_reader_lookup_result {
        std::unique_ptr<remote_segment_batch_reader> reader;
        remote_partition::iterator iter;
    };

    /// Find the starting segment for a reader or _partition->end() if
    /// no suitable segement exists.
    remote_partition::iterator
    seek_segment(const storage::log_reader_config& config) {
        if (config.first_timestamp) {
            // Seek by timestamp
            return _partition->seek_by_timestamp(*config.first_timestamp);
        } else {
            // Seek by offset
            auto it = _partition->upper_bound(config.start_offset);
            if (it != _partition->begin()) {
                it = std::prev(it);
            }
            return it;
        }
    }

    std::optional<cache_reader_lookup_result>
    find_cached_reader(const storage::log_reader_config& config) {
        if (!_partition || _partition->_segments.empty()) {
            return std::nullopt;
        }

        auto it = seek_segment(config);

        if (it == _partition->end()) {
            // this can happen, for example, when a timequery is made with a
            // time point after our latest message
            return std::nullopt;
        }

        auto reader = _partition->borrow_reader(config, it->first, it->second);
        // Here we know the exact type of the reader_state because of
        // the invariant of the borrow_reader
        const auto& segment
          = std::get<remote_partition::materialized_segment_ptr>(it->second)
              ->segment;
        vlog(
          _ctxlog.debug,
          "segment offset range {}-{}, delta: {}",
          segment->get_base_rp_offset(),
          segment->get_max_rp_offset(),
          segment->get_base_offset_delta());
        return {{.reader = std::move(reader), .iter = it}};
    }

    /// Reset reader if current segment is fully consumed.
    /// The object may transition onto a next segment or
    /// it will transtion into completed state with no reader
    /// attached.
    ss::future<bool> maybe_reset_reader() {
        vlog(_ctxlog.debug, "maybe_reset_reader called");
        if (!_reader) {
            co_return false;
        }
        if (_reader->config().start_offset > _reader->config().max_offset) {
            vlog(
              _ctxlog.debug,
              "maybe_reset_stream called - stream already consumed, start "
              "{}, "
              "max {}",
              _reader->config().start_offset,
              _reader->config().max_offset);
            // Entire range is consumed, detach from remote_partition and
            // close the reader.
            co_await set_end_of_stream();
            co_return false;
        }
        vlog(
          _ctxlog.debug,
          "maybe_reset_reader, config start_offset: {}, reader max_offset: {}",
          _reader->config().start_offset,
          _reader->max_rp_offset());
        if (_reader->is_eof()) {
            // move to the next segment
            vlog(_ctxlog.debug, "maybe_reset_stream condition triggered");
            _it++;
            // We're comparing to the cached _end instead of
            // _partition->_segments.end() to avoid the following pitfall. If
            // the currently referenced segment has no data batches and we get
            // to this point, but at the same time the new segment with the same
            // base_offset (in kafka terms) was added to the map this new
            // segment will replace the current one and we won't read it (since
            // we're already done with the segment). This isn't a problem since
            // we will just stop and the next fetch request will be able to read
            // the data. But if not one but two or more segments were added to
            // the _segments map we will just skip the segment with the same
            // base_offset and proceed with the next one. The client will see a
            // gap. The caching of the _segments.end() prevents this.
            if (_it == _end) {
                co_await set_end_of_stream();
            } else {
                // reuse config but replace the reader
                auto config = _reader->config();
                _partition->evict_reader(std::move(_reader));
                vlog(_ctxlog.debug, "initializing new segment reader");
                _reader = _partition->borrow_reader(
                  config, _it->first, _it->second);
            }
        }
        vlog(
          _ctxlog.debug,
          "maybe_reset_reader completed, reader is present: {}, is end of "
          "stream: {}",
          static_cast<bool>(_reader),
          is_end_of_stream());
        co_return static_cast<bool>(_reader);
    }

    /// Transition reader to the completed state. Stop tracking state in
    /// the 'remote_partition'
    ss::future<> set_end_of_stream() {
        co_await _reader->stop();
        _it = _partition->end();
        _reader = {};
    }

    retry_chain_node _rtc;
    retry_chain_logger _ctxlog;

    ss::lw_shared_ptr<remote_partition> _partition;
    ss::lw_shared_ptr<storage::offset_translator_state> _ot_state;
    /// Currently accessed segment
    remote_partition::iterator _it;
    remote_partition::iterator _end;
    /// Reader state that was borrowed from the materialized_segment_state
    std::unique_ptr<remote_segment_batch_reader> _reader;
    /// Cancelation subscription
    ss::abort_source::subscription _as_sub;
    /// Guard for the partition gate
    gate_guard _gate_guard;
};

remote_partition::remote_partition(
  const partition_manifest& m, remote& api, cache& c, s3::bucket_name bucket)
  : _rtc()
  , _ctxlog(cst_log, _rtc, m.get_ntp().path())
  , _api(api)
  , _cache(c)
  , _manifest(m)
  , _bucket(std::move(bucket))
  , _stm_jitter(stm_jitter_duration)
  , _probe(m.get_ntp()) {}

ss::future<> remote_partition::start() {
    update_segments_incrementally();
    (void)run_eviction_loop();

    _stm_timer.set_callback([this] {
        gc_stale_materialized_segments(false);
        _stm_timer.rearm(_stm_jitter());
    });
    _stm_timer.rearm(_stm_jitter());
    co_return;
}

ss::future<> remote_partition::run_eviction_loop() {
    // Evict readers asynchronously
    gate_guard g(_gate);
    try {
        while (true) {
            co_await _cvar.wait([this] { return !_eviction_list.empty(); });
            auto tmp_list = std::exchange(_eviction_list, {});
            for (auto& rs : tmp_list) {
                co_await std::visit([](auto&& rs) { return rs->stop(); }, rs);
            }
        }
    } catch (const ss::broken_condition_variable&) {
    }
    vlog(_ctxlog.debug, "remote partition eviction loop stopped");
}

void remote_partition::gc_stale_materialized_segments(bool force_collection) {
    // The remote_segment instances are materialized on demand. They are
    // collected after some period of inactivity.
    // To prevent high memory consumption in some corner cases the
    // materialization of the new remote_segment triggers GC. The idea is
    // that remote_partition should have only one remote_segment in materialized
    // state when it's constantly in use and zero if not in use.
    vlog(
      _ctxlog.debug,
      "collecting stale materialized segments, {} segments materialized, {} "
      "segments total",
      _materialized.size(),
      _segments.size());

    auto now = ss::lowres_clock::now();
    auto max_idle = force_collection ? 0ms : stm_max_idle_time;

    std::vector<model::offset> offsets;
    for (auto& st : _materialized) {
        auto deadline = st.atime + max_idle;
        if (now >= deadline && !st.segment->download_in_progress()) {
            if (st.segment.owned()) {
                vlog(
                  _ctxlog.debug,
                  "reader for segment with base offset {} is stale",
                  st.offset_key);
                // this will delete and unlink the object from
                // _materialized collection
                offsets.push_back(st.offset_key);
            } else {
                vlog(
                  _ctxlog.debug,
                  "Materialized segment with base-offset {} is not stale: {} "
                  "{} {} {} readers={}",
                  st.base_rp_offset,
                  now - st.atime > stm_max_idle_time,
                  st.segment->download_in_progress(),
                  st.segment.owned(),
                  st.segment.use_count(),
                  st.readers.size());

                // Readers hold a reference to the segment, so for the
                // segment.owned() check to pass, we need to clear them out.
                while (!st.readers.empty()) {
                    evict_reader(std::move(st.readers.front()));
                    st.readers.pop_front();
                }
            }
        }
    }
    vlog(_ctxlog.debug, "found {} eviction candidates ", offsets.size());
    for (auto o : offsets) {
        vlog(_ctxlog.debug, "about to offload segment {}", o);
        auto it = _segments.find(o);
        vassert(it != _segments.end(), "Can't find offset {}", o);
        auto tmp = std::visit(
          [this](auto&& st) { return st->offload(this); }, it->second);
        it->second = tmp;
    }
}

model::offset remote_partition::first_uploaded_offset() {
    vassert(
      _manifest.size() > 0,
      "The manifest for {} is not expected to be empty",
      _manifest.get_ntp());
    try {
        auto it = _manifest.begin();
        auto off = get_kafka_base_offset(it->second);
        vlog(_ctxlog.debug, "remote partition first_uploaded_offset: {}", off);
        return off;
    } catch (...) {
        vlog(
          _ctxlog.error,
          "remote partition first_uploaded_offset error {}",
          std::current_exception());

        throw;
    }
}

model::offset remote_partition::last_uploaded_offset() {
    vassert(
      _manifest.size() > 0,
      "The manifest for {} is not expected to be empty",
      _manifest.get_ntp());
    return _manifest.get_last_offset();
}

const model::ntp& remote_partition::get_ntp() const {
    return _manifest.get_ntp();
}

bool remote_partition::is_data_available() const {
    return _manifest.size() > 0;
}

// returns term last kafka offset
std::optional<model::offset>
remote_partition::get_term_last_offset(model::term_id term) const {
    vassert(
      _manifest.size() > 0,
      "The manifest for {} is not expected to be empty",
      _manifest.get_ntp());

    // look for first segment in next term, segments are sorted by base_offset
    // and term
    for (auto const& p : _manifest) {
        if (p.first.term > term) {
            return get_kafka_base_offset(p.second) - model::offset(1);
        }
    }
    // if last segment term is equal to the one we look for return it
    if (_manifest.rbegin()->first.term == term) {
        return get_kafka_max_offset(_manifest.rbegin()->second);
    }

    return std::nullopt;
}

ss::future<std::vector<cluster::rm_stm::tx_range>>
remote_partition::aborted_transactions(offset_range offsets) {
    gate_guard guard(_gate);
    // Here we have to use kafka offsets to locate the segments and
    // redpanda offsets to extract aborted transactions metadata because
    // tx-manifests contains redpanda offsets.
    std::vector<cluster::rm_stm::tx_range> result;

    // that's a stable btree iterator that makes key lookup on increment
    auto first_it = upper_bound(offsets.begin);
    if (first_it != begin()) {
        first_it = std::prev(first_it);
    }
    for (auto it = first_it; it != end(); it++) {
        if (it->first > offsets.end) {
            break;
        }
        auto tx = co_await ss::visit(
          it->second,
          [this, offsets, offset_key = it->first](
            offloaded_segment_state& off_state) {
              auto tmp = off_state->materialize(*this, offset_key);
              auto res = tmp->segment->aborted_transactions(
                offsets.begin_rp, offsets.end_rp);
              _segments.insert_or_assign(offset_key, std::move(tmp));
              return res;
          },
          [offsets](materialized_segment_ptr& m_state) {
              return m_state->segment->aborted_transactions(
                offsets.begin_rp, offsets.end_rp);
          });
        std::copy(tx.begin(), tx.end(), std::back_inserter(result));
    }
    // Adjacent segments might return the same transaction record.
    // In this case we will have a duplicate. The duplicates will always
    // be located next to each other in the sequence.
    auto last = std::unique(result.begin(), result.end());
    result.erase(last, result.end());
    vlog(
      _ctxlog.debug,
      "found {} aborted transactions for {}-{} offset range ({}-{} before "
      "offset translaction)",
      result.size(),
      offsets.begin,
      offsets.end,
      offsets.begin_rp,
      offsets.end_rp);
    co_return result;
}

ss::future<> remote_partition::stop() {
    vlog(_ctxlog.debug, "remote partition stop {} segments", _segments.size());
    _stm_timer.cancel();
    _cvar.broken();

    co_await _gate.close();

    // Do the last pass over the eviction list to stop remaining items returned
    // from readers after the eviction loop stopped.
    for (auto& rs : _eviction_list) {
        co_await std::visit([](auto&& rs) { return rs->stop(); }, rs);
    }

    for (auto it = begin(); it != end(); it++) {
        vlog(_ctxlog.debug, "remote partition stop {}", it->first);
        co_await std::visit([](auto&& st) { return st->stop(); }, it->second);
    }
}

void remote_partition::update_segments_incrementally() {
    vlog(_ctxlog.debug, "remote partition update segments incrementally");
    // find new segments
    for (const auto& meta : _manifest) {
        auto o = get_kafka_base_offset(meta.second);
        auto prev_it = _segments.find(o);
        if (prev_it != _segments.end()) {
            // The key can be in the map in two cases:
            // - we've already added the segment to the map
            // - the key that we've added previously doesn't have data batches
            //   in this case it can be safely replaced by the new one
            auto prev_off = std::visit(
              [](auto&& p) { return p->base_rp_offset; }, prev_it->second);
            auto manifest_it = _manifest.find(prev_off);
            vassert(
              manifest_it != _manifest.end(),
              "Can't find key {} in the manifest. NTP: {}",
              prev_off,
              _manifest.get_ntp());
            if (
              meta.second == manifest_it->second
              || manifest_it->first.base_offset > meta.second.base_offset) {
                // This path can be taken if we've already added the
                // segment with data batches and the current segment
                // that the loop is checking doesn't have any.
                // The check works because if we have two segments and
                // one of them doesn't have data batches, the one that
                // have data batches will have larger base_offset (in
                // redpanda terms). Note that the key in the map is a
                // kafka offset.
                continue;
            }
            vlog(
              _ctxlog.debug,
              "Segment with kafka-offset {} will be replaced. New segment "
              "{{base: {}, max: {}, delta: {}}}, previous segment "
              "{{base: {}, max: {}, delta: {}}}",
              o,
              meta.second.base_offset,
              meta.second.committed_offset,
              meta.second.delta_offset,
              manifest_it->first.base_offset,
              manifest_it->second.committed_offset,
              manifest_it->second.delta_offset);

            std::visit([this](auto&& p) { p->offload(this); }, prev_it->second);
        }
        auto res = _segments.insert_or_assign(
          o, offloaded_segment_state(meta.first.base_offset));
        if (res.second) {
            _probe.segment_added();
        }
    }
}

/// Materialize segment if needed and create a reader
std::unique_ptr<remote_segment_batch_reader> remote_partition::borrow_reader(
  storage::log_reader_config config, model::offset key, segment_state& st) {
    if (std::holds_alternative<offloaded_segment_state>(st)) {
        gc_stale_materialized_segments(true);
    }
    return ss::visit(
      st,
      [this, &config, offset_key = key, &st](
        offloaded_segment_state& off_state) {
          auto tmp = off_state->materialize(*this, offset_key);
          auto res = tmp->borrow_reader(config, this->_ctxlog, _probe);
          st = std::move(tmp);
          return res;
      },
      [this, &config](materialized_segment_ptr& m_state) {
          return m_state->borrow_reader(config, _ctxlog, _probe);
      });
}

/// Return reader back to segment_state
void remote_partition::return_reader(
  std::unique_ptr<remote_segment_batch_reader> reader, segment_state& st) {
    return ss::visit(
      st,
      [this, &reader](offloaded_segment_state&) {
          evict_reader(std::move(reader));
      },
      [&reader](materialized_segment_ptr& m_state) {
          m_state->return_reader(std::move(reader));
      });
}

ss::future<storage::translating_reader> remote_partition::make_reader(
  storage::log_reader_config config,
  std::optional<model::timeout_clock::time_point> deadline) {
    std::ignore = deadline;
    vlog(
      _ctxlog.debug,
      "remote partition make_reader invoked, config: {}, num segments {}",
      config,
      _segments.size());
    if (_segments.size() < static_cast<ssize_t>(_manifest.size())) {
        update_segments_incrementally();
    }
    auto ot_state = ss::make_lw_shared<storage::offset_translator_state>(
      get_ntp());
    auto impl = std::make_unique<partition_record_batch_reader_impl>(
      config, shared_from_this(), ot_state);
    co_return storage::translating_reader{
      model::record_batch_reader(std::move(impl)), std::move(ot_state)};
}

ss::future<std::optional<storage::timequery_result>>
remote_partition::timequery(storage::timequery_config cfg) {
    if (static_cast<size_t>(_segments.size()) < _manifest.size()) {
        update_segments_incrementally();
    }

    if (_segments.empty()) {
        vlog(_ctxlog.debug, "timequery: no segments");
        co_return std::nullopt;
    }

    auto start_offset = _segments.begin()->first;

    // Synthesize a log_reader_config from our timequery_config
    storage::log_reader_config config(
      start_offset,
      cfg.max_offset,
      0,
      2048, // We just need one record batch
      cfg.prio,
      cfg.type_filter,
      cfg.time,
      cfg.abort_source);

    // Construct a reader that will skip to the requested timestamp
    // by virtue of log_reader_config::start_timestamp
    auto translating_reader = co_await make_reader(config);
    auto ot_state = std::move(translating_reader.ot_state);

    // Read one batch from the reader to learn the offset
    model::record_batch_reader::storage_t data
      = co_await model::consume_reader_to_memory(
        std::move(translating_reader.reader), model::no_timeout);

    auto& batches = std::get<model::record_batch_reader::data_t>(data);
    vlog(_ctxlog.debug, "timequery: {} batches", batches.size());

    if (batches.size()) {
        co_return storage::batch_timequery(*(batches.begin()), cfg.time);
    } else {
        co_return std::nullopt;
    }
}

remote_partition::offloaded_segment_state::offloaded_segment_state(
  model::offset base_offset)
  : base_rp_offset(base_offset) {}

std::unique_ptr<remote_partition::materialized_segment_state>
remote_partition::offloaded_segment_state::materialize(
  remote_partition& p, model::offset offset_key) {
    auto st = std::make_unique<materialized_segment_state>(
      base_rp_offset, offset_key, p);
    p._probe.segment_materialized();
    return st;
}

ss::future<> remote_partition::offloaded_segment_state::stop() {
    return ss::now();
}

remote_partition::offloaded_segment_state
remote_partition::offloaded_segment_state::offload(remote_partition*) {
    return offloaded_segment_state(base_rp_offset);
}

remote_partition::materialized_segment_state::materialized_segment_state(
  model::offset base_offset, model::offset off_key, remote_partition& p)
  : base_rp_offset(base_offset)
  , offset_key(off_key)
  , segment(ss::make_lw_shared<remote_segment>(
      p._api, p._cache, p._bucket, p._manifest, base_offset, p._rtc))
  , atime(ss::lowres_clock::now()) {
    p._materialized.push_back(*this);
}

void remote_partition::materialized_segment_state::return_reader(
  std::unique_ptr<remote_segment_batch_reader> state) {
    atime = ss::lowres_clock::now();
    readers.push_back(std::move(state));
}

/// Borrow reader or make a new one.
/// In either case return a reader.
std::unique_ptr<remote_segment_batch_reader>
remote_partition::materialized_segment_state::borrow_reader(
  const storage::log_reader_config& cfg,
  retry_chain_logger& ctxlog,
  partition_probe& probe) {
    atime = ss::lowres_clock::now();
    for (auto it = readers.begin(); it != readers.end(); it++) {
        if ((*it)->config().start_offset == cfg.start_offset) {
            // here we're reusing the existing reader
            auto tmp = std::move(*it);
            tmp->config() = cfg;
            readers.erase(it);
            vlog(
              ctxlog.debug,
              "reusing existing reader, config: {}",
              tmp->config());
            return tmp;
        }
    }
    vlog(ctxlog.debug, "creating new reader, config: {}", cfg);
    return std::make_unique<remote_segment_batch_reader>(segment, cfg, probe);
}

ss::future<> remote_partition::materialized_segment_state::stop() {
    for (auto& rs : readers) {
        co_await rs->stop();
    }
    co_await segment->stop();
}

remote_partition::offloaded_segment_state
remote_partition::materialized_segment_state::offload(
  remote_partition* partition) {
    _hook.unlink();
    for (auto&& rs : readers) {
        partition->evict_reader(std::move(rs));
    }
    partition->evict_segment(std::move(segment));
    partition->_probe.segment_offloaded();
    return offloaded_segment_state(base_rp_offset);
}

remote_partition::iterator remote_partition::begin() {
    if (_segments.empty()) {
        return end();
    }
    auto it = _segments.begin();
    return iterator(_segments, it->first);
}

remote_partition::iterator remote_partition::end() {
    return iterator(_segments);
}

remote_partition::iterator remote_partition::upper_bound(model::offset o) {
    auto it = _segments.upper_bound(o);
    if (it != _segments.end()) {
        return iterator(_segments, it->first);
    }
    return end();
}

remote_partition::iterator
remote_partition::seek_by_timestamp(model::timestamp t) {
    auto segment_meta = _manifest.timequery(t);

    if (segment_meta) {
        model::offset o = get_kafka_base_offset(*segment_meta);
        auto found = _segments.find(o);

        // Cannot happen because timequery() calls
        // update_segments_incrementally first.
        vassert(
          found != _segments.end(),
          "Timequery t={} found offset {} in manifest, but no segment "
          "state found for that offset");
        return iterator(_segments, found->first);
    } else {
        // They queried a time that is later than all our data
        return end();
    }
}

} // namespace cloud_storage
