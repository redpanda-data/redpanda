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
#include "cloud_storage/remote_segment.h"
#include "cloud_storage/topic_manifest.h"
#include "cloud_storage/tx_range_manifest.h"
#include "cloud_storage/types.h"
#include "model/fundamental.h"
#include "storage/log_reader.h"
#include "storage/parser_errc.h"
#include "storage/types.h"
#include "utils/gate_guard.h"
#include "utils/retry_chain_node.h"
#include "utils/stream_utils.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/circular_buffer.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/queue.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/temporary_buffer.hh>

#include <boost/range/adaptor/reversed.hpp>

#include <chrono>
#include <exception>
#include <iterator>
#include <variant>

using namespace std::chrono_literals;

namespace cloud_storage {

using data_t = model::record_batch_reader::data_t;
using storage_t = model::record_batch_reader::storage_t;

remote_partition::iterator
remote_partition::materialize_segment(const segment_meta& meta) {
    _as.check();
    auto base_kafka_offset = meta.base_offset - meta.delta_offset;
    auto units = materialized().get_segment_units();
    auto st = std::make_unique<materialized_segment_state>(
      meta.base_offset, *this, std::move(units));
    auto [iter, ok] = _segments.insert(
      std::make_pair(meta.base_offset, std::move(st)));
    vassert(
      ok,
      "Segment with base log offset {} and base kafka offset {} is already "
      "materialized, max offset of the new segment {}, max offset of the "
      "existing segment {}",
      meta.base_offset,
      base_kafka_offset,
      meta.committed_offset,
      iter->second->segment->get_max_rp_offset());
    _probe.segment_materialized();
    return iter;
}

remote_partition::borrow_result_t remote_partition::borrow_next_reader(
  storage::log_reader_config config, model::offset hint) {
    // The code find the materialized that can satisfy the reader. If the
    // segment is not materialized it materializes it.
    // The following situations are possible:
    // - The config.start_offset belongs to the same segment if
    //   - Last record batches are config batches so config.start_offset
    //     is <= segment.committed_offset.
    //   - The underlying segment was reuploaded and now covers the wider
    //     offset range. In this case we just stopped reading previous
    //     version of the segment which now became longer.
    // - The config.start_offset matches the base_offset - delta_offset of
    //   the next segment.

    auto ko = model::offset_cast(config.start_offset);
    // Two level lookup:
    // - find segment meta based on kafka offset
    //   this allow us to avoid any abiguity in case if the segment
    //   doesn't have any data. The 'segment_containing' method of the
    //   manifest takes this into account.
    // - find materialized segment or materialize the new one
    auto mit = _manifest.end();
    if (hint == model::offset{}) {
        // This code path is only used for the first lookup. It
        // could be either lookup by kafka offset or by timestamp.
        if (config.first_timestamp) {
            auto maybe_meta = _manifest.timequery(*config.first_timestamp);
            if (maybe_meta) {
                mit = _manifest.segment_containing(maybe_meta->base_offset);
            }
        } else {
            // In this case the lookup is perfomed by kafka offset.
            // Normally, this would be the first lookup done by the
            // partition_record_batch_reader_impl. This lookup will
            // skip segments without data batches (the logic is implemented
            // inside the partition_manifest).
            mit = _manifest.segment_containing(ko);
        }
        if (mit == _manifest.end()) {
            // Segment that matches exactly can't be found in the manifest. In
            // this case we want to start scanning from the begining of the
            // partition if the start of the manifest is contained by the scan
            // range.
            auto so = _manifest.get_start_kafka_offset().value_or(
              kafka::offset::min());
            if (config.start_offset < so && config.max_offset > so) {
                mit = _manifest.begin();
            }
        }
    } else {
        mit = _manifest.segment_containing(hint);
        while (mit != _manifest.end()) {
            // The segment 'mit' points to might not have any
            // data batches. In this case we need to move iterator forward.
            // The check can only be done if we have 'delta_offset_end'.
            if (mit->delta_offset_end == model::offset_delta{}) {
                break;
            }

            // If a segment contains kafka data batches, its next offset will
            // be greater than its base offset.
            auto b = mit->base_kafka_offset();
            auto end = mit->next_kafka_offset();
            if (b != end) {
                break;
            }
            ++mit;
        }
    }
    if (mit == _manifest.end()) {
        // No such segment
        return borrow_result_t{};
    }
    auto iter = _segments.find(mit->base_offset);
    if (iter != _segments.end()) {
        if (
          iter->second->segment->get_max_rp_offset() != mit->committed_offset) {
            offload_segment(iter->first);
            iter = _segments.end();
        }
    }
    if (iter == _segments.end()) {
        iter = materialize_segment(*mit);
    }
    auto mit_committed_offset = mit->committed_offset;
    auto next_it = std::next(std::move(mit));
    while (next_it != _manifest.end()) {
        // Normally, the segments in the manifest do not overlap.
        // But in some cases we may see them overlapping, for instance
        // if they were produced by older version of redpanda.
        // In this case we want to skip segment if its offset range
        // lies withing the offset range of the current segment.
        if (mit_committed_offset < next_it->committed_offset) {
            break;
        }
        ++next_it;
    }
    model::offset next_offset = next_it == _manifest.end()
                                  ? model::offset{}
                                  : next_it->base_offset;
    return borrow_result_t{
      .reader = iter->second->borrow_reader(config, _ctxlog, _probe),
      .next_segment_offset = next_offset};
}

class partition_record_batch_reader_impl final
  : public model::record_batch_reader::impl {
public:
    explicit partition_record_batch_reader_impl(
      const storage::log_reader_config& config,
      ss::shared_ptr<remote_partition> part,
      ss::lw_shared_ptr<storage::offset_translator_state> ot_state) noexcept
      : _rtc(part->_as)
      , _ctxlog(cst_log, _rtc, part->get_ntp().path())
      , _partition(std::move(part))
      , _ot_state(std::move(ot_state))
      , _gate_guard(_partition->_gate) {
        auto ntp = _partition->get_ntp();
        vlog(_ctxlog.trace, "Constructing reader {}", ntp);
        if (config.abort_source) {
            vlog(_ctxlog.debug, "abort_source is set");
            auto sub = config.abort_source->get().subscribe([this]() noexcept {
                vlog(_ctxlog.debug, "abort requested via config.abort_source");
                dispose_current_reader();
            });
            if (sub) {
                _as_sub = std::move(*sub);
            } else {
                vlog(_ctxlog.debug, "abort_source is triggered in c-tor");
                _reader = {};
            }
        }
        initialize_reader_state(config);
        _partition->_probe.reader_created();
    }

    ~partition_record_batch_reader_impl() noexcept override {
        auto ntp = _partition->get_ntp();
        vlog(_ctxlog.trace, "Destructing reader {}", ntp);
        _partition->_probe.reader_destroyed();
        if (_reader) {
            // We must not destroy this reader: it is not safe to do so
            // without calling stop() on it.  The remote_partition is
            // responsible for cleaning up readers, including calling
            // stop() on them in a background fiber so that we don't have
            // to. (https://github.com/redpanda-data/redpanda/issues/3378)
            vlog(
              _ctxlog.debug,
              "partition_record_batch_reader_impl::~ releasing reader on "
              "destruction");

            try {
                dispose_current_reader();
            } catch (...) {
                // Failure to return the reader causes the reader destructor
                // to execute synchronously inside this function.  That
                // might succeed, if the reader was already stopped, so give
                // it a chance rather than asserting out here.
                vlog(
                  _ctxlog.error,
                  "partition_record_batch_reader_impl::~ exception while "
                  "releasing reader: {}",
                  std::current_exception());
            }
        }
    }
    partition_record_batch_reader_impl(
      partition_record_batch_reader_impl&& o) noexcept
      = delete;
    partition_record_batch_reader_impl&
    operator=(partition_record_batch_reader_impl&& o) noexcept
      = delete;
    partition_record_batch_reader_impl(
      const partition_record_batch_reader_impl& o)
      = delete;
    partition_record_batch_reader_impl&
    operator=(const partition_record_batch_reader_impl& o)
      = delete;

    bool is_end_of_stream() const override { return _reader == nullptr; }

    ss::future<storage_t>
    do_load_slice(model::timeout_clock::time_point deadline) override {
        std::exception_ptr unknown_exception_ptr = nullptr;
        try {
            if (is_end_of_stream()) {
                vlog(
                  _ctxlog.debug,
                  "partition_record_batch_reader_impl do_load_slice - "
                  "empty");
                co_return storage_t{};
            }
            if (_reader->config().over_budget) {
                vlog(_ctxlog.debug, "We're overbudget, stopping");
                // We need to stop in such way that will keep the
                // reader in the reusable state, so we could reuse
                // it on next itertaion

                // The existing state have to be rebuilt
                dispose_current_reader();
                co_return storage_t{};
            }
            while (co_await maybe_reset_reader()) {
                if (_partition->_gate.is_closed()) {
                    co_await set_end_of_stream();
                    co_return storage_t{};
                }
                auto reader_delta = _reader->current_delta();
                if (
                  !_ot_state->empty()
                  && _ot_state->last_delta() > reader_delta) {
                    // It's not safe to call 'read_sone' with the current
                    // offset translator state becuase delta offset of the
                    // current reader is below last delta registred by the
                    // offset translator. The offset translator contains data
                    // from the previous segment and there is an inconsistency
                    // between them.
                    //
                    // If the reader never produced any data we can simply reset
                    // the offset translator state and continue. Otherwise, we
                    // need to stop producing.
                    //
                    // This trick should guarantee us forward progress. If the
                    // reader will stop right away before it will be able to
                    // produce any batches (in the first branch) that belong to
                    // the current segment the next fetch request will likely
                    // have start_offset that corresponds to the previous
                    // segment. In this case the reader will have to skip all
                    // previously seen batches and _first_produced_offset will
                    // be set to default value. This will allow reader to reset
                    // the ot_state and move to the current segment in the
                    // second branch. This is safe because the ot_state won't
                    // have any information about the previous segment.
                    vlog(
                      _ctxlog.info,
                      "Detected inconsistency. Reader config: {}, delta offset "
                      "of the current reader: {}, delta offset of the offset "
                      "translator: {}, first offset produced by this reader: "
                      "{}",
                      _reader->config(),
                      reader_delta,
                      _ot_state->last_delta(),
                      _first_produced_offset);
                    if (_first_produced_offset != model::offset{}) {
                        co_await set_end_of_stream();
                        co_return storage_t{};
                    } else {
                        _ot_state->reset();
                    }
                }
                vlog(
                  _ctxlog.debug,
                  "Invoking 'read_some' on current log reader with config: "
                  "{}",
                  _reader->config());

                try {
                    auto result = co_await _reader->read_some(
                      deadline, *_ot_state);
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
                        _partition->_probe.add_records_read(
                          batch.record_count());
                    }
                    if (
                      _first_produced_offset == model::offset{} && !d.empty()) {
                        _first_produced_offset = d.front().base_offset();
                    }
                    co_return storage_t{std::move(d)};
                } catch (const stuck_reader_exception& ex) {
                    vlog(
                      _ctxlog.warn,
                      "stuck reader: current rp offset: {}, max rp offset: {}",
                      ex.rp_offset,
                      _reader->max_rp_offset());

                    // If the reader is stuck because of a mismatch between
                    // segment data and manifest entry, set reader to EOF and
                    // try to reset reader on the next loop iteration. We only
                    // do this when the reader has not reached eof. For example,
                    // the segment ends at offset 10 but the manifest has max
                    // offset at 11 for the segment, with offset 11 actually
                    // present in the next segment. When the reader is stuck,
                    // the current offset will be 10 which we will not be able
                    // to read from. Switching to the next segment should enable
                    // reads to proceed.
                    if (
                      model::next_offset(ex.rp_offset)
                        >= _next_segment_base_offset
                      && !_reader->is_eof()) {
                        vlog(
                          _ctxlog.info,
                          "mismatch between current segment end and manifest "
                          "data: current rp offset {}, manifest max rp offset "
                          "{}, next segment base offset {}, reader is EOF: {}. "
                          "set EOF on reader and try to "
                          "reset",
                          ex.rp_offset,
                          _reader->max_rp_offset(),
                          _next_segment_base_offset,
                          _reader->is_eof());
                        _reader->set_eof();
                        continue;
                    }
                    throw;
                }
            }
        } catch (const ss::gate_closed_exception&) {
            vlog(
              _ctxlog.debug,
              "gate_closed_exception while reading from remote_partition");
        } catch (const ss::abort_requested_exception&) {
            vlog(
              _ctxlog.debug,
              "abort_requested_exception while reading from remote_partition");
        } catch (const std::exception& e) {
            vlog(
              _ctxlog.warn,
              "exception thrown while reading from remote_partition: {}",
              e.what());
            unknown_exception_ptr = std::current_exception();
        }

        // If we've made it through the above try block without returning,
        // we've thrown an exception. Regardless of which error, the reader may
        // have been left in an indeterminate state. Re-set the pointer to it
        // to ensure that it will not be reused.
        if (_reader) {
            co_await set_end_of_stream();
        }
        if (unknown_exception_ptr) {
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
    void dispose_current_reader() {
        if (_reader) {
            _partition->return_reader(std::move(_reader));
        }
    }

    // Initialize object using remote_partition as a source
    void initialize_reader_state(const storage::log_reader_config& config) {
        vlog(
          _ctxlog.debug,
          "partition_record_batch_reader_impl initialize reader state");
        auto [reader, next_offset] = find_cached_reader(config);
        if (reader) {
            _reader = std::move(reader);
            _next_segment_base_offset = next_offset;
            return;
        }
        vlog(
          _ctxlog.debug,
          "partition_record_batch_reader_impl initialize reader state - "
          "segment not "
          "found");
        _reader = {};
        _next_segment_base_offset = {};
    }

    remote_partition::borrow_result_t
    find_cached_reader(const storage::log_reader_config& config) {
        if (!_partition || _partition->_manifest.size() == 0) {
            return {};
        }
        auto res = _partition->borrow_next_reader(config);
        if (res.reader) {
            // Here we know the exact type of the reader_state because of
            // the invariant of the borrow_reader
            vlog(
              _ctxlog.debug,
              "segment log offset range {}-{}, next log offset: {}, "
              "log reader config: {}",
              res.reader->base_rp_offset(),
              res.reader->max_rp_offset(),
              res.next_segment_offset,
              config);
        }
        return res;
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
          "maybe_reset_reader, config start_offset: {}, reader max_offset: "
          "{}",
          _reader->config().start_offset,
          _reader->max_rp_offset());

        // The next offset should be below the next segment base offset if the
        // reader has not finished. If the next offset to be read from has
        // reached the next segment but the reader is not finished, then the
        // state is inconsistent.
        if (_reader->is_eof()) {
            auto prev_max_offset = _reader->max_rp_offset();
            auto config = _reader->config();
            vlog(
              _ctxlog.debug,
              "maybe_reset_stream condition triggered after offset: {}, "
              "reader's current log offset: {}, config.start_offset: {}, "
              "reader's max log offset: {}, is EOF: {}, next base_offset "
              "estimate: {}",
              prev_max_offset,
              _reader->current_rp_offset(),
              config.start_offset,
              _reader->max_rp_offset(),
              _reader->is_eof(),
              _next_segment_base_offset);
            _partition->evict_reader(std::move(_reader));
            vlog(
              _ctxlog.debug,
              "initializing new segment reader {}, next offset {}",
              config.start_offset,
              _next_segment_base_offset);
            if (_next_segment_base_offset != model::offset{}) {
                auto [new_reader, new_next_offset]
                  = _partition->borrow_next_reader(
                    config, _next_segment_base_offset);
                _next_segment_base_offset = new_next_offset;
                _reader = std::move(new_reader);
            }
            if (_reader != nullptr) {
                vassert(
                  prev_max_offset != _reader->max_rp_offset(),
                  "Progress stall detected, ntp: {}, max offset of prev "
                  "reader: {}, max offset of the new reader {}",
                  _partition->get_ntp(),
                  prev_max_offset,
                  _reader->max_rp_offset());
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
        _reader = {};
    }

    retry_chain_node _rtc;
    retry_chain_logger _ctxlog;

    ss::shared_ptr<remote_partition> _partition;
    ss::lw_shared_ptr<storage::offset_translator_state> _ot_state;
    /// Reader state that was borrowed from the materialized_segment_state
    std::unique_ptr<remote_segment_batch_reader> _reader;
    /// Cancelation subscription
    ss::abort_source::subscription _as_sub;
    /// Guard for the partition gate
    gate_guard _gate_guard;
    model::offset _next_segment_base_offset{};
    /// Contains offset of the first produced record batch or min()
    /// if no data were produced yet
    model::offset _first_produced_offset{};
};

remote_partition::remote_partition(
  const partition_manifest& m,
  remote& api,
  cache& c,
  cloud_storage_clients::bucket_name bucket)
  : _rtc(_as)
  , _ctxlog(cst_log, _rtc, m.get_ntp().path())
  , _api(api)
  , _cache(c)
  , _manifest(m)
  , _bucket(std::move(bucket))
  , _probe(m.get_ntp()) {}

ss::future<> remote_partition::start() {
    // Fiber that consumers from _eviction_list and calls stop on items before
    // destroying them.
    ssx::spawn_with_gate(_gate, [this] { return run_eviction_loop(); });
    co_return;
}

void remote_partition::evict_reader(
  std::unique_ptr<remote_segment_batch_reader> reader) {
    _eviction_pending.push_back(std::move(reader));
    _has_evictions_cvar.signal();
}

void remote_partition::evict_segment(
  ss::lw_shared_ptr<remote_segment> segment) {
    _eviction_pending.push_back(std::move(segment));
    _has_evictions_cvar.signal();
}

ss::future<> remote_partition::run_eviction_loop() {
    // Evict readers asynchronously.
    // NOTE: exits when the condition variable is broken.
    while (true) {
        co_await _has_evictions_cvar.wait(
          [this] { return !_eviction_pending.empty(); });
        auto eviction_in_flight = std::exchange(_eviction_pending, {});
        co_await ss::max_concurrent_for_each(
          eviction_in_flight, 200, [](auto&& rs_variant) {
              return std::visit(
                [](auto&& rs) { return rs->stop(); }, rs_variant);
          });
    }
}

kafka::offset remote_partition::first_uploaded_offset() {
    vassert(
      _manifest.size() > 0,
      "The manifest for {} is not expected to be empty",
      _manifest.get_ntp());
    auto so = _manifest.get_start_kafka_offset().value();
    vlog(_ctxlog.trace, "remote partition first_uploaded_offset: {}", so);
    return so;
}

kafka::offset remote_partition::next_kafka_offset() {
    vassert(
      _manifest.size() > 0,
      "The manifest for {} is not expected to be empty",
      _manifest.get_ntp());
    auto next = _manifest.get_next_kafka_offset().value();
    vlog(_ctxlog.debug, "remote partition next_kafka_offset: {}", next);
    return next;
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

uint64_t remote_partition::cloud_log_size() const {
    return _manifest.cloud_log_size();
}

ss::future<> remote_partition::serialize_json_manifest_to_output_stream(
  ss::output_stream<char>& output) const {
    return _manifest.serialize_json(output);
}

// returns term last kafka offset
std::optional<kafka::offset>
remote_partition::get_term_last_offset(model::term_id term) const {
    vassert(
      _manifest.size() > 0,
      "The manifest for {} is not expected to be empty",
      _manifest.get_ntp());

    // look for first segment in next term, segments are sorted by
    // base_offset and term
    for (auto const& p : _manifest) {
        if (p.segment_term > term) {
            return p.base_kafka_offset() - kafka::offset(1);
        }
    }
    // if last segment term is equal to the one we look for return it
    auto last = _manifest.last_segment();
    vassert(
      last.has_value(),
      "The manifest for {} is not expected to be empty",
      _manifest.get_ntp());
    if (last->segment_term == term) {
        return last->next_kafka_offset() - kafka::offset(1);
    }

    return std::nullopt;
}

ss::future<std::vector<model::tx_range>>
remote_partition::aborted_transactions(offset_range offsets) {
    gate_guard guard(_gate);
    // Here we have to use kafka offsets to locate the segments and
    // redpanda offsets to extract aborted transactions metadata because
    // tx-manifests contains redpanda offsets.
    std::vector<model::tx_range> result;
    for (auto it = _manifest.segment_containing(offsets.begin);
         it != _manifest.end();
         ++it) {
        if (it->base_offset > offsets.end_rp) {
            break;
        }

        // Segment might be materialized, we need a
        // second map lookup to learn if this is the case.
        auto m = _segments.find(it->base_offset);
        if (m == _segments.end()) {
            m = materialize_segment(*it);
        }
        auto tx = co_await m->second->segment->aborted_transactions(
          offsets.begin_rp, offsets.end_rp);
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

    // Prevent further segment materialization, readers, etc.
    _as.request_abort();

    // Signal to the eviction loop that it should terminate.
    _has_evictions_cvar.broken();

    co_await _gate.close();
    // Remove materialized_segment_state from the list that contains it, to
    // avoid it getting registered for eviction and stop.
    for (auto& pair : _segments) {
        vlog(
          _ctxlog.debug, "unlinking segment {}", pair.second->base_rp_offset());
        pair.second->unlink();
    }

    // Swap segment map into a local variable to avoid map mutation while the
    // segments are being stopped.
    decltype(_segments) segments_to_stop;
    segments_to_stop.swap(_segments);
    co_await ss::max_concurrent_for_each(
      segments_to_stop, 200, [this](auto& iter) {
          auto& [offset, segment] = iter;
          vlog(
            _ctxlog.debug,
            "remote partition stop {}",
            segment->base_rp_offset());
          return segment->stop();
      });

    // Do the last pass over the eviction list to stop remaining items returned
    // from readers after the eviction loop stopped.
    for (auto& rs : _eviction_pending) {
        co_await std::visit(
          [](auto&& rs) {
              if (!rs->is_stopped()) {
                  return rs->stop();
              } else {
                  return ss::make_ready_future<>();
              }
          },
          rs);
    }

    vlog(_ctxlog.debug, "remote partition stopped");
}

/// Return reader back to segment_state
void remote_partition::return_reader(
  std::unique_ptr<remote_segment_batch_reader> reader) {
    auto offset = reader->base_rp_offset();
    if (auto it = _segments.find(offset);
        it != _segments.end()
        && reader->reads_from_segment(*it->second->segment)) {
        // The segment may already be replaced by compacted segment at this
        // point. In this case it's possible that the remote_segment instance
        // which 'it' points to belongs to the new segment.
        it->second->return_reader(std::move(reader));
    } else {
        evict_reader(std::move(reader));
    }
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
    auto ot_state = ss::make_lw_shared<storage::offset_translator_state>(
      get_ntp());
    auto impl = std::make_unique<partition_record_batch_reader_impl>(
      config, shared_from_this(), ot_state);
    co_return storage::translating_reader{
      model::record_batch_reader(std::move(impl)), std::move(ot_state)};
}

ss::future<std::optional<storage::timequery_result>>
remote_partition::timequery(storage::timequery_config cfg) {
    if (_manifest.size() == 0) {
        vlog(_ctxlog.debug, "timequery: no segments");
        co_return std::nullopt;
    }

    auto start_offset = _manifest.get_start_kafka_offset().value();

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

bool remote_partition::bounds_timestamp(model::timestamp t) const {
    auto last_seg = _manifest.last_segment();
    if (last_seg.has_value()) {
        return t <= last_seg.value().max_timestamp;
    } else {
        return false;
    }
}

static constexpr ss::lowres_clock::duration erase_timeout = 60s;
static constexpr ss::lowres_clock::duration erase_backoff = 1s;

ss::future<remote_partition::finalize_result>
remote_partition::finalize(ss::abort_source& as) {
    vlog(_ctxlog.info, "Finalizing remote storage state...");

    // This function is called after ::stop, so we may not use our
    // main retry_chain_node which is bound to our abort source,
    // and construct a special one.
    retry_chain_node local_rtc(as, erase_timeout, erase_backoff);

    partition_manifest remote_manifest(
      _manifest.get_ntp(), _manifest.get_revision_id());

    auto [manifest_get_result, result_fmt]
      = co_await _api.try_download_partition_manifest(
        _bucket, remote_manifest, local_rtc);

    if (manifest_get_result != download_result::success) {
        vlog(
          _ctxlog.debug,
          "Failed to fetch manifest during finalize(), maybe another node "
          "already completed deletion. Error: {}",
          manifest_get_result);
        co_return finalize_result{.get_status = manifest_get_result};
    }

    if (remote_manifest.get_insync_offset() > _manifest.get_insync_offset()) {
        // Our local manifest is behind the remote: return a copy of the
        // remote manifest for use in deletion
        vlog(
          _ctxlog.debug,
          "Remote manifest has newer state than local ({} > {}), using this "
          "for deletion during finalize",
          remote_manifest.get_insync_offset(),
          _manifest.get_insync_offset());
        co_return finalize_result{
          .manifest = std::move(remote_manifest),
          .get_status = manifest_get_result};
    } else if (
      remote_manifest.get_insync_offset() < _manifest.get_insync_offset()) {
        // TODO: should this be behind a feature table check?

        // The remote manifest is out of date, upload a fresh one
        vlog(
          _ctxlog.debug,
          "Remote manifest has older state than local ({} < {}), attempting to "
          "upload latest manifest",
          remote_manifest.get_insync_offset(),
          _manifest.get_insync_offset());
        auto manifest_tags
          = cloud_storage::remote::make_partition_manifest_tags(
            _manifest.get_ntp(), _manifest.get_revision_id());

        auto manifest_put_result = co_await _api.upload_manifest(
          _bucket, _manifest, local_rtc, manifest_tags);

        if (manifest_put_result != upload_result::success) {
            vlog(
              _ctxlog.warn,
              "Failed to write manifest during finalize(), remote manifest may "
              "be left in incomplete state after topic deletion. Error: {}",
              manifest_put_result);
        }

        co_return finalize_result{
          .manifest = std::nullopt, .get_status = manifest_get_result};
    } else {
        // Remote and local state is in sync, no action required.
        vlog(
          _ctxlog.debug,
          "Remote manifest is in sync with local state during finalize "
          "(insync_offset={})",
          _manifest.get_insync_offset());
        co_return finalize_result{
          .manifest = std::nullopt, .get_status = manifest_get_result};
    }
}

ss::future<remote_partition::erase_result> remote_partition::erase(
  cloud_storage::remote& api,
  cloud_storage_clients::bucket_name bucket,
  partition_manifest manifest,
  ss::abort_source& as) {
    // This function is called after ::stop, so we may not use our
    // main retry_chain_node which is bound to our abort source,
    // and construct a special one.
    retry_chain_node local_rtc(as, erase_timeout, erase_backoff);

    auto replaced_segments = manifest.replaced_segments();

    // Erase all segments
    // Loop over normal segments and replaced (pending deletion) segments
    // at the same time, to batch them together into as few DeleteObjects
    // requests as possible
    const size_t batch_size = 1000;
    auto segment_i = manifest.begin();
    auto replaced_i = replaced_segments.begin();

    while (segment_i != manifest.end()
           || replaced_i != replaced_segments.end()) {
        std::vector<cloud_storage_clients::object_key> batch_keys;
        batch_keys.reserve(batch_size);

        std::vector<cloud_storage_clients::object_key> tx_batch_keys;
        tx_batch_keys.reserve(batch_size);

        std::vector<cloud_storage_clients::object_key> index_keys;
        index_keys.reserve(batch_size);

        for (
          size_t k = 0;
          k < batch_size
          && (segment_i != manifest.end() || replaced_i != replaced_segments.end());
          ++k) {
            remote_segment_path segment_path;
            if (segment_i != manifest.end()) {
                segment_path = manifest.generate_segment_path(*segment_i);
                ++segment_i;
            } else {
                vassert(
                  replaced_i != replaced_segments.end(),
                  "Loop condition should ensure one iterator is always valid");
                segment_path = manifest.generate_segment_path(*replaced_i);
                ++replaced_i;
            }
            batch_keys.emplace_back(segment_path);
            tx_batch_keys.emplace_back(
              tx_range_manifest(segment_path).get_manifest_path());
            index_keys.emplace_back(generate_index_path(segment_path));
        }

        vlog(
          cst_log.info,
          "[{}] Erasing segments {}-{}",
          manifest.get_ntp(),
          *(batch_keys.begin()),
          *(--batch_keys.end()));

        for (const auto& object_set : {batch_keys, tx_batch_keys, index_keys}) {
            if (
              co_await api.delete_objects(
                bucket, std::move(object_set), local_rtc)
              != upload_result::success) {
                vlog(
                  cst_log.info,
                  "[{}] Failed to erase some segments, deferring deletion",
                  manifest.get_ntp());
                co_return erase_result::failed;
            }
        }
    }

    // If we got this far, we succeeded deleting all objects referenced by
    // the manifest, so many delete the manifest itself.
    auto manifest_path = manifest.get_manifest_path();
    vlog(
      cst_log.debug,
      "[{}] Erasing partition manifest {}",
      manifest.get_ntp(),
      manifest_path);
    if (
      co_await api.delete_object(
        bucket, cloud_storage_clients::object_key(manifest_path), local_rtc)
      != upload_result::success) {
        vlog(
          cst_log.info,
          "[{}] Failed to erase {}, deferring deletion",
          manifest.get_ntp(),
          manifest_path);
        co_return erase_result::failed;
    };

    co_return erase_result::erased;
}

/**
 * The caller is responsible for determining whether it is appropriate
 * to delete data in S3, for example it is not appropriate if this is
 * a read replica topic.
 *
 * Q: Why is try_erase() part of remote_partition, when in general writes
 *    flow through the archiver and remote_partition is mainly for
 *    reads?
 * A: Because the erase operation works in terms of what it reads from
 *    S3, not the state of the archival metadata stm (which can be out
 *    of date if e.g. we were not the leader)
 */
ss::future<> remote_partition::try_erase(ss::abort_source& as) {
    vlog(
      _ctxlog.info,
      "Attempting to erase remote storage content for {}",
      _manifest.get_ntp());

    // This function is called after ::stop, so we may not use our
    // main retry_chain_node which is bound to our abort source,
    // and construct a special one.
    retry_chain_node local_rtc(as, erase_timeout, erase_backoff);

    // Download manifest because we may not be running on the most up to
    // date node, but hopefully the latest leader will have uploaded
    // manifest in finalize().
    partition_manifest manifest(
      _manifest.get_ntp(), _manifest.get_revision_id());
    auto [manifest_get_result, result_fmt]
      = co_await _api.try_download_partition_manifest(
        _bucket, manifest, local_rtc, true);

    if (manifest_get_result != download_result::success) {
        vlog(
          _ctxlog.info,
          "Failed to fetch manifest {}, deferring cleanup on topic erase",
          manifest.get_manifest_path(result_fmt));
        co_return;
    }

    co_await erase(_api, _bucket, std::move(manifest), as);
}

void remote_partition::offload_segment(model::offset o) {
    vlog(_ctxlog.debug, "about to offload segment {}", o);
    auto it = _segments.find(o);
    vassert(it != _segments.end(), "Can't find offset {}", o);
    it->second->offload(this);
    _segments.erase(it);
}

materialized_segments& remote_partition::materialized() {
    return _api.materialized();
}

cache_usage_target remote_partition::get_cache_usage_target() const {
    // minimum and nice-to-have chunks per partition
    constexpr auto min_chunks = 1;
    constexpr auto wanted_chunks = 20;

    // minimum and nice-to-have segments per partition are roughly the same
    // (compared to chunks) as segments are much lager. for example, 2 segments
    // at the default segment size is about the same as 20 default sized chunks.
    constexpr auto min_segments = 1;
    constexpr auto wanted_segments = 2;

    // check for a recent materialized segment
    for (const auto& it : boost::adaptors::reverse(_segments)) {
        const auto& seg = it.second->segment;
        if (!seg) {
            continue;
        }
        auto [size, chunked] = seg->min_cache_cost();
        if (chunked) {
            return cache_usage_target{
              .target_min_bytes = min_chunks * size,
              .target_bytes = wanted_chunks * size,
              .chunked = chunked,
            };
        } else {
            return cache_usage_target{
              .target_min_bytes = min_segments * size,
              .target_bytes = wanted_segments * size,
              .chunked = chunked,
            };
        }
    }

    // we may not have any materialized segments, but we can decode some info
    // about them anyway from the manifest.
    auto seg = _manifest.last_segment();
    if (seg.has_value()) {
        const auto chunked
          = !config::shard_local_cfg().cloud_storage_disable_chunk_reads()
            && seg.value().sname_format > segment_name_format::v2;
        if (chunked) {
            return cache_usage_target{
              .target_min_bytes
              = min_chunks
                * config::shard_local_cfg().cloud_storage_cache_chunk_size(),
              .target_bytes
              = wanted_chunks
                * config::shard_local_cfg().cloud_storage_cache_chunk_size(),
              .chunked = chunked,
            };
        } else {
            return cache_usage_target{
              .target_min_bytes = min_segments * seg.value().size_bytes,
              .target_bytes = wanted_segments * seg.value().size_bytes,
              .chunked = chunked,
            };
        }
    }

    // with no information at all, we'll just make a reasonable guess
    if (!config::shard_local_cfg().cloud_storage_disable_chunk_reads()) {
        return cache_usage_target{
          .target_min_bytes
          = min_chunks
            * config::shard_local_cfg().cloud_storage_cache_chunk_size(),
          .target_bytes
          = wanted_chunks
            * config::shard_local_cfg().cloud_storage_cache_chunk_size(),
          .chunked = true,
        };
    } else {
        return cache_usage_target{
          .target_min_bytes = min_segments
                              * config::shard_local_cfg().log_segment_size(),
          .target_bytes = wanted_segments
                          * config::shard_local_cfg().log_segment_size(),
          .chunked = false,
        };
    }
}

} // namespace cloud_storage
