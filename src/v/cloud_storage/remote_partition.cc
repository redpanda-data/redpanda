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
#include "cloud_storage/materialized_segments.h"
#include "cloud_storage/offset_translation_layer.h"
#include "cloud_storage/partition_manifest.h"
#include "cloud_storage/topic_manifest.h"
#include "cloud_storage/tx_range_manifest.h"
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
static kafka::offset
get_kafka_base_offset(const partition_manifest::segment_meta& m) {
    // Manifests created with the old version of redpanda won't have the
    // delta_offset field. In this case the value will be initialized to
    // model::offset::min(). In this case offset translation couldn't be
    // performed.
    auto delta = m.delta_offset == model::offset_delta::min()
                   ? model::offset_delta(0)
                   : m.delta_offset;
    return m.base_offset - delta;
}
/// This function returns segment max offset as kafka offset
static kafka::offset
get_kafka_max_offset(const partition_manifest::segment_meta& m) {
    // Manifests created with the old version of redpanda won't have the
    // delta_offset field. In this case the value will be initialized to
    // model::offset::min(). In this case offset translation couldn't be
    // performed.
    auto delta = m.delta_offset == model::offset_delta::min()
                   ? model::offset_delta(0)
                   : m.delta_offset;
    return m.committed_offset - delta;
}

class partition_record_batch_reader_impl final
  : public model::record_batch_reader::impl {
public:
    explicit partition_record_batch_reader_impl(
      const storage::log_reader_config& config,
      ss::shared_ptr<remote_partition> part,
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
                _partition->materialized().evict_reader(std::move(_reader));
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
                dispose_current_reader();
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
                dispose_current_reader();
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
    /// Return or evict currently referenced reader
    ///
    /// If the reader belongs to existing segment it will be returned
    /// back to the materialized segment.
    /// If it belongs to evicted segment the reader itself wil be evicted.
    void dispose_current_reader() {
        if (!_it.is_invalidated()) {
            _partition->return_reader(std::move(_reader), _it->second);
        } else {
            // The segment was evicted or replaced by the compacted
            // segment. In this case we can't return the reader back
            // to the materialized segment and have to remove it.
            _partition->materialized().evict_reader(std::move(_reader));
        }
    }

    // Initialize object using remote_partition as a source
    void initialize_reader_state(const storage::log_reader_config& config) {
        _partition->maybe_sync_with_manifest();
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

    /// Find the starting segment for a reader
    remote_partition::iterator
    seek_segment(const storage::log_reader_config& config) {
        if (config.first_timestamp) {
            // Seek by timestamp
            return _partition->seek_by_timestamp(*config.first_timestamp);
        } else {
            // Seek by offset
            auto it = _partition->upper_bound(
              model::offset_cast(config.start_offset));
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
        auto reader = _partition->borrow_reader(config, it->first, it->second);
        // Here we know the exact type of the reader_state because of
        // the invariant of the borrow_reader
        const auto& segment
          = std::get<remote_partition::materialized_segment_ptr>(it->second)
              ->segment;
        vlog(
          _ctxlog.debug,
          "segment offset range {}-{}, delta: {}, log reader config: {}",
          segment->get_base_rp_offset(),
          segment->get_max_rp_offset(),
          segment->get_base_offset_delta(),
          config);
        if (it->first > config.max_offset) {
            _partition->materialized().evict_reader(std::move(reader));
            return std::nullopt;
        }
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
            _partition->maybe_sync_with_manifest();
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
                // This branch might be taken if the last segment was merged
                // with the previous one. (TODO: fix this)
                co_await set_end_of_stream();
            } else {
                // reuse config but replace the reader
                auto config = _reader->config();
                _partition->materialized().evict_reader(std::move(_reader));
                vlog(_ctxlog.debug, "initializing new segment reader");
                // It's safe to dereference '_it' since we just incremented it.
                // If the underlying segment was deleted the _it will be equal
                // to _end. But just in case if anyone will insert an
                // asynchronous operation in between the increment and
                // dereferencing we're checking the state of the iterator.
                vassert(
                  !_it.is_invalidated(),
                  "race condition detedted, ntp: {}, config: {}",
                  _partition->get_ntp(),
                  config);
                vlog(
                  _ctxlog.debug,
                  "initializing new segment reader {} {}",
                  config.start_offset,
                  _it->first);
                if (model::offset_cast(config.start_offset) < _it->first) {
                    // Invariant: we got here by incrementing _it so it's safe
                    // to decrement it This branch will be taken if the
                    // underlying segments was merged and the manifest has
                    // changed. For instance, let's say that originally we had
                    // three segments [100-199], [200-299], and [300-399] and
                    // _it pointed to the first one. Then, the manifest was
                    // updated and segmets were merged to [100-299],[300, 399].
                    // After the increment the iterator will be pointing to
                    // [300, 399] (because it caches the key internally and
                    // doing a map lookup on increment). This mean that we will
                    // have a gap in offsets that reader sees. To avoid this we
                    // need to be able to detect this situation.
                    _it = std::prev(_it);
                }
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

    ss::shared_ptr<remote_partition> _partition;
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
  , _probe(m.get_ntp()) {}

ss::future<> remote_partition::start() {
    maybe_sync_with_manifest();

    co_return;
}

kafka::offset remote_partition::first_uploaded_offset() {
    vassert(
      _manifest.size() > 0,
      "The manifest for {} is not expected to be empty",
      _manifest.get_ntp());
    maybe_sync_with_manifest();
    try {
        // Invariant: _manifest has at least one segment so start offset is
        // guaranteed to not to be nullopt.
        auto so = _manifest.get_start_offset();
        // Invariant: start offset is only advanced forward by segment boundary.
        // It can only be equal to base_offset of one of the segments so we can
        // always convert it to kafka offset.
        auto it = _manifest.find(*so);
        vassert(
          it != _manifest.end(),
          "unexpected start_offset {} in {}",
          *so,
          _manifest.get_ntp());
        auto off = get_kafka_base_offset(it->second);
        vlog(_ctxlog.trace, "remote partition first_uploaded_offset: {}", off);
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
    maybe_sync_with_manifest();
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
    // Only advertize data if we have segments _and_ our start offset
    // corresponds to some data (not if our start_offset points off
    // the end of the manifest, as a result of a truncation where we
    // have not yet cleaned out the segments.
    return _manifest.size() > 0
           && _manifest.find(_manifest.get_start_offset().value())
                != _manifest.end();
}

// returns term last kafka offset
std::optional<kafka::offset>
remote_partition::get_term_last_offset(model::term_id term) const {
    vassert(
      _manifest.size() > 0,
      "The manifest for {} is not expected to be empty",
      _manifest.get_ntp());

    // look for first segment in next term, segments are sorted by base_offset
    // and term
    for (auto const& p : _manifest) {
        if (p.second.segment_term > term) {
            return get_kafka_base_offset(p.second) - kafka::offset(1);
        }
    }
    // if last segment term is equal to the one we look for return it
    if (_manifest.rbegin()->second.segment_term == term) {
        return get_kafka_max_offset(_manifest.rbegin()->second);
    }

    return std::nullopt;
}

ss::future<std::vector<model::tx_range>>
remote_partition::aborted_transactions(offset_range offsets) {
    maybe_sync_with_manifest();
    gate_guard guard(_gate);
    // Here we have to use kafka offsets to locate the segments and
    // redpanda offsets to extract aborted transactions metadata because
    // tx-manifests contains redpanda offsets.
    std::vector<model::tx_range> result;

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

    co_await _gate.close();

    for (auto it = begin(); it != end(); it++) {
        vlog(_ctxlog.debug, "remote partition stop {}", it->first);
        co_await std::visit([](auto&& st) { return st->stop(); }, it->second);
    }

    // We may have some segment or reader objects enqueued for stop in
    // the shared eviction queue: must flush it, or they can outlive
    // us and trigger assertion in retry_chain_node destructor.
    // This waits for all evictions, not just ours, but that's okay because
    // stopping readers is fast, the queue is not usually long, and destroying
    // partitions is relatively infrequent.
    co_await materialized().flush_evicted();
}

void remote_partition::maybe_sync_with_manifest() {
    if (_insync_offset == _manifest.get_insync_offset()) {
        // Fast path, the manifest is not expected to be updated frequently
        // and most of the time the call is a no-op.
        return;
    }
    vlog(
      _ctxlog.debug,
      "updating remote_partition from {} to {}",
      _insync_offset,
      _manifest.get_insync_offset());

    // Algorithm outline
    // - If the segment is offloaded we can just re-create it
    // - If the segment is materialized we need to check if the
    //   underlying segment was replaced by looking at its remote path
    absl::btree_map<remote_segment_path, materialized_segment_ptr>
      materialized_segments;
    for (auto& it : _segments) {
        if (std::holds_alternative<materialized_segment_ptr>(it.second)) {
            auto& st = std::get<materialized_segment_ptr>(it.second);
            auto path = st->segment->get_segment_path();
            materialized_segments.insert(
              std::make_pair(std::move(path), std::move(st)));
        }
    }
    _segments.clear();

    // Rebuild the _segments collection using the
    // prevoiusly materialized segments stored in
    // materialized_segments collection.
    for (const auto& meta : _manifest) {
        auto o = get_kafka_base_offset(meta.second);
        auto prev_it = _segments.find(o);
        if (prev_it != _segments.end()) {
            // The key can be in the map if the previous segment doesn't have
            // data batches. In this case it can be safely replaced by the new
            // one.
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
              || manifest_it->second.base_offset > meta.second.base_offset) {
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
              manifest_it->second.base_offset,
              manifest_it->second.committed_offset,
              manifest_it->second.delta_offset);

            std::visit([this](auto&& p) { p->offload(this); }, prev_it->second);
        }
        auto spath = _manifest.generate_segment_path(meta.second);
        if (auto it = materialized_segments.find(spath);
            it != materialized_segments.end()) {
            auto ms = std::move(it->second);
            _segments.insert_or_assign(o, std::move(ms));
            materialized_segments.erase(it);
        } else {
            _segments.insert_or_assign(
              o, offloaded_segment_state(meta.second.base_offset));
        }
    }
    _insync_offset = _manifest.get_insync_offset();

    for (auto& kv : materialized_segments) {
        kv.second->offload(this);
    }
}

/// Materialize segment if needed and create a reader
std::unique_ptr<remote_segment_batch_reader> remote_partition::borrow_reader(
  storage::log_reader_config config, kafka::offset key, segment_state& st) {
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
          materialized().evict_reader(std::move(reader));
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
    maybe_sync_with_manifest();
    auto ot_state = ss::make_lw_shared<storage::offset_translator_state>(
      get_ntp());
    auto impl = std::make_unique<partition_record_batch_reader_impl>(
      config, shared_from_this(), ot_state);
    co_return storage::translating_reader{
      model::record_batch_reader(std::move(impl)), std::move(ot_state)};
}

ss::future<std::optional<storage::timequery_result>>
remote_partition::timequery(storage::timequery_config cfg) {
    maybe_sync_with_manifest();

    if (_segments.empty()) {
        vlog(_ctxlog.debug, "timequery: no segments");
        co_return std::nullopt;
    }

    auto start_offset = _segments.begin()->first;

    // Synthesize a log_reader_config from our timequery_config
    storage::log_reader_config config(
      kafka::offset_cast(start_offset),
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

remote_partition::iterator remote_partition::upper_bound(kafka::offset o) {
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
        kafka::offset o = get_kafka_base_offset(*segment_meta);
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

/**
 * This is an error-handling wrapper around remote::delete_object.
 *
 * During topic deletion, we wish to handle DeleteObject errors in a particular
 * way:
 * - Throw on timeout: this indicates a transient condition,
 *   and we shall rely on the controller to try applying this
 *   topic deletion operation again.
 * - Give up on deletion on other errors non-timeout errors may be
 *   permanent (e.g. bucket does not exist, or authorization errors).  In these
 * cases we log the issue and give up: worst case we are leaving garbage behind.
 *   We do _not_ proceed to delete subsequent objects (such
 *   as the manifest), to preserve the invariant that a removed
 *   offset means the segments were already removed.
 *
 * @return true if the caller should stop trying to delete things and silently
 *         return, false if successful, throws if deletion should be retried.
 */
ss::future<bool> remote_partition::tolerant_delete_object(
  const s3::bucket_name& bucket,
  const s3::object_key& path,
  retry_chain_node& parent) {
    auto result = co_await _api.delete_object(bucket, path, parent);
    if (result == upload_result::timedout) {
        throw std::runtime_error(fmt::format(
          "Timed out deleting objects for partition {} (key {})",
          _manifest.get_ntp(),
          path));
    } else if (result != upload_result::success) {
        vlog(
          _ctxlog.warn,
          "Error ({}) deleting objects for partition (key {})",
          result,
          path);
        co_return true;
    } else {
        co_return false;
    }
}

/**
 * The caller is responsible for determining whether it is appropriate
 * to delete data in S3, for example it is not appropriate if this is
 * a read replica topic.
 *
 * Q: Why is erase() part of remote_partition, when in general writes
 *    flow through the archiver and remote_partition is mainly for
 *    reads?
 * A: Because the erase operation works in terms of what it reads from
 *    S3, not the state of the archival metadata stm (which can be out
 *    of date if e.g. we were not the leader)
 */
ss::future<> remote_partition::erase() {
    // TODO: Edge case 1
    // There is a rare race in which objects might get left behind in S3.
    //
    // Consider 3 nodes 1,2,3.
    // Node 1 is controller leader.  Node 3 is this partition's leader.
    // Node 1 receives a delete topic request, replicates to 2, and
    // acks to the client.
    // Nodes 1 and 2 proceed to tear down the partition.
    // Node 3 experiences a delay seeing the controller message,
    // and is still running archival fiber, writing new objects to S3.
    // Now stop Node 3, and force-decommission it.
    // The last couple of objects written by Node 3 are left behind in S3.
    // (TODO: perhaps document this as a caveat of force-decom operations?)

    // TODO: Edge case 2
    // Archiver writes objects to S3 before it writes an update manifest.
    // If a topic is deleted while undergoing writes, it may end up leaving
    // some garbage segments behind.
    // (TODO: fix this by implementing a scrub operation that finds objects
    //  not mentioned in the manifest, and call this before erase())

    static constexpr ss::lowres_clock::duration erase_timeout = 60s;
    static constexpr ss::lowres_clock::duration erase_backoff = 1s;
    retry_chain_node local_rtc(erase_timeout, erase_backoff, &_rtc);

    // Read the partition manifest fresh: we might already have
    // dropped local archival_stm state related to this partition.
    partition_manifest manifest(
      _manifest.get_ntp(), _manifest.get_revision_id());

    auto manifest_path = manifest.get_manifest_path();
    auto manifest_get_result = co_await _api.maybe_download_manifest(
      _bucket, manifest_path, manifest, local_rtc);

    if (manifest_get_result == download_result::timedout) {
        // Throw on transient connectivity issues, so that controller
        // will retry this deletion
        throw std::runtime_error(
          fmt::format("Timeout reading manifest for {}", manifest.get_ntp()));
    } else if (manifest_get_result == download_result::failed) {
        // Treat non-timeout errors as permanent, to avoid controller
        // getting stuck.
        vlog(
          _ctxlog.warn,
          "Error downloading manifest: objects will not be deleted for this "
          "topic");
        co_return;
    }

    // Having handled errors above, now our partition manifest fetch was either
    // a notfound (skip straight to erasing topic manifest), or a success
    // (iterate through manifest deleting segements)
    if (manifest_get_result != download_result::notfound) {
        // Erase all segments
        for (const auto& i : manifest) {
            auto path = manifest.generate_segment_path(i.second);
            vlog(_ctxlog.debug, "Erasing segment {}", path);
            // On failure, we throw: this should cause controller to retry
            // the topic deletion operation that called us, until it eventually
            // succeeds.
            // TODO: S3 API has a plural delete API, which would be more
            // suitable.
            if (co_await tolerant_delete_object(
                  _bucket, s3::object_key(path), local_rtc)) {
                co_return;
            };

            auto tx_range_manifest_path
              = tx_range_manifest(path).get_manifest_path();
            if (co_await tolerant_delete_object(
                  _bucket, s3::object_key(tx_range_manifest_path), local_rtc)) {
                co_return;
            };
        }

        // Erase the partition manifest
        vlog(_ctxlog.debug, "Erasing partition manifest {}", manifest_path);
        if (co_await tolerant_delete_object(
              _bucket, s3::object_key(manifest_path), local_rtc)) {
            co_return;
        };
    }

    // If I am partition 0, also delete the topic manifest
    // Note: this behavior means that absence of the topic manifest does
    // *not* imply full removal of topic, whereas absence of partition
    // manifest does imply full removal of partition data.
    if (get_ntp().tp.partition == model::partition_id{0}) {
        auto topic_manifest_path = topic_manifest::get_topic_manifest_path(
          get_ntp().ns, get_ntp().tp.topic);
        vlog(_ctxlog.debug, "Erasing topic manifest {}", topic_manifest_path);
        if (co_await tolerant_delete_object(
              _bucket, s3::object_key(topic_manifest_path), local_rtc)) {
            co_return;
        };
    }
}

void remote_partition::offload_segment(kafka::offset o) {
    vlog(_ctxlog.debug, "about to offload segment {}", o);

    auto it = _segments.find(o);
    vassert(it != _segments.end(), "Can't find offset {}", o);
    auto tmp = std::visit(
      [this](auto&& st) { return st->offload(this); }, it->second);
    it->second = tmp;
}

materialized_segments& remote_partition::materialized() {
    return _api.materialized();
}

} // namespace cloud_storage
