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
                mit = _manifest.segment_containing(
                  maybe_meta->get().base_offset);
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
            if (mit->second.delta_offset_end == model::offset_delta{}) {
                break;
            }
            auto b = mit->second.base_kafka_offset();
            auto end = mit->second.next_kafka_offset() - kafka::offset(1);
            if (b != end) {
                break;
            }
            mit++;
        }
    }
    if (mit == _manifest.end()) {
        // No such segment
        return borrow_result_t{};
    }
    auto iter = _segments.find(mit->first);
    if (iter != _segments.end()) {
        if (
          iter->second->segment->get_max_rp_offset()
          != mit->second.committed_offset) {
            offload_segment(iter->first);
            iter = _segments.end();
        }
    }
    if (iter == _segments.end()) {
        iter = materialize_segment(mit->second);
    }
    auto next_it = std::next(mit);
    while (next_it != _manifest.end()) {
        // Normally, the segments in the manifest do not overlap.
        // But in some cases we may see them overlapping, for instance
        // if they were produced by older version of redpanda.
        // In this case we want to skip segment if its offset range
        // lies withing the offset range of the current segment.
        if (mit->second.committed_offset < next_it->second.committed_offset) {
            break;
        }
        next_it++;
    }
    model::offset next_offset = next_it == _manifest.end() ? model::offset{}
                                                           : next_it->first;
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
      partition_record_batch_reader_impl&& o) noexcept = delete;
    partition_record_batch_reader_impl&
    operator=(partition_record_batch_reader_impl&& o) noexcept = delete;
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
                if (_first_produced_offset == model::offset{} && !d.empty()) {
                    _first_produced_offset = d.front().base_offset();
                }
                co_return storage_t{std::move(d)};
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
              "initializing new segment reader {}, next offset",
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

ss::future<> remote_partition::serialize_manifest_to_output_stream(
  ss::output_stream<char>& output) const {
    return _manifest.serialize(output);
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
        if (p.second.segment_term > term) {
            return p.second.base_kafka_offset() - kafka::offset(1);
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
    auto first_it = _manifest.segment_containing(offsets.begin);
    for (auto it = first_it; it != _manifest.end(); it++) {
        if (it->second.base_offset > offsets.end_rp) {
            break;
        }

        // Segment might be materialized, we need a
        // second map lookup to learn if this is the case.
        auto m = _segments.find(it->first);
        if (m == _segments.end()) {
            m = materialize_segment(it->second);
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

/**
 * This is an error-handling wrapper around remote::delete_object.
 *
 * During topic deletion, we wish to handle DeleteObject errors in a
 * particular way:
 * - Throw on timeout: this indicates a transient condition,
 *   and we shall rely on the controller to try applying this
 *   topic deletion operation again.
 * - Give up on deletion on other errors non-timeout errors may be
 *   permanent (e.g. bucket does not exist, or authorization errors).  In
 * these cases we log the issue and give up: worst case we are leaving
 * garbage behind. We do _not_ proceed to delete subsequent objects (such as
 * the manifest), to preserve the invariant that a removed offset means the
 * segments were already removed.
 *
 * @return true if the caller should stop trying to delete things and
 * silently return, false if successful, throws if deletion should be
 * retried.
 */
ss::future<bool> remote_partition::tolerant_delete_object(
  const cloud_storage_clients::bucket_name& bucket,
  const cloud_storage_clients::object_key& path,
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

ss::future<bool> remote_partition::tolerant_delete_objects(
  const cloud_storage_clients::bucket_name& bucket,
  std::vector<cloud_storage_clients::object_key>&& keys,
  retry_chain_node& parent) {
    if (keys.empty()) {
        co_return false;
    }

    // Copy first key so that we can use it in error messages
    auto first_key = keys[0];

    auto result = co_await _api.delete_objects(bucket, std::move(keys), parent);
    if (result == upload_result::timedout) {
        throw std::runtime_error(fmt::format(
          "Timed out deleting objects for partition {} (keys {}-)",
          _manifest.get_ntp(),
          first_key));
    } else if (result != upload_result::success) {
        vlog(
          _ctxlog.warn,
          "Error ({}) deleting objects for partition (keys {}-)",
          result,
          first_key);
        co_return true;
    } else {
        co_return false;
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

    auto manifest_path = remote_manifest.get_manifest_path();
    auto manifest_get_result = co_await _api.maybe_download_manifest(
      _bucket, manifest_path, remote_manifest, local_rtc);

    if (manifest_get_result != download_result::success) {
        vlog(
          _ctxlog.warn,
          "Failed to fetch manifest during finalize(), remote manifest may be "
          "left in incomplete state after topic deletion. Error: {}",
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
ss::future<> remote_partition::erase(ss::abort_source& as, bool do_finalize) {
    // Even though we are going to delete objects, it is still important
    // to flush metadata first, so that if we are interrupted, a future
    // node doing a tombstone-driven deletion can accurately get the
    // segment list.
    remote_partition::finalize_result finalize_result;
    if (do_finalize) {
        finalize_result = co_await finalize(as);
    } else {
        finalize_result = remote_partition::finalize_result{
          // Synthetic success, as we will no-op on non-voters
          .get_status = download_result::success};
    }

    if (finalize_result.get_status != download_result::success) {
        // If we couldn't read a remote manifest at all, give up here rather
        // than issuing a lot of deletion requests that will probably also fail.
        co_return;
    }

    const partition_manifest& manifest = finalize_result.manifest.has_value()
                                           ? finalize_result.manifest.value()
                                           : _manifest;

    vlog(_ctxlog.info, "Erasing remote storage content...");

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

    // This function is called after ::stop, so we may not use our
    // main retry_chain_node which is bound to our abort source,
    // and construct a special one.
    retry_chain_node local_rtc(as, erase_timeout, erase_backoff);

    // Having handled errors above, now our partition manifest fetch was
    // either a notfound (skip straight to erasing topic manifest), or a
    // success (iterate through manifest deleting segements)

    // Erase all segments
    const size_t batch_size = 1000;
    auto segment_i = manifest.begin();
    while (segment_i != manifest.end()) {
        std::vector<cloud_storage_clients::object_key> batch_keys;
        batch_keys.reserve(batch_size);

        std::vector<cloud_storage_clients::object_key> tx_batch_keys;
        tx_batch_keys.reserve(batch_size);

        std::vector<cloud_storage_clients::object_key> index_keys;
        index_keys.reserve(batch_size);

        for (size_t k = 0; k < batch_size && segment_i != manifest.end(); ++k) {
            auto segment_path = manifest.generate_segment_path(
              segment_i->second);
            batch_keys.emplace_back(segment_path);
            tx_batch_keys.emplace_back(
              tx_range_manifest(segment_path).get_manifest_path());
            index_keys.emplace_back(generate_remote_index_path(segment_path));
            segment_i++;
        }

        vlog(
          _ctxlog.debug,
          "Erasing segments {}-{}",
          *(batch_keys.begin()),
          *(--batch_keys.end()));

        if (co_await tolerant_delete_objects(
              _bucket, std::move(batch_keys), local_rtc)) {
            co_return;
        }

        if (co_await tolerant_delete_objects(
              _bucket, std::move(tx_batch_keys), local_rtc)) {
            co_return;
        }

        if (co_await tolerant_delete_objects(
              _bucket, std::move(index_keys), local_rtc)) {
            co_return;
        }
    }

    // Erase the partition manifest
    auto manifest_path = manifest.get_manifest_path();
    vlog(_ctxlog.debug, "Erasing partition manifest {}", manifest_path);
    if (co_await tolerant_delete_object(
          _bucket,
          cloud_storage_clients::object_key(manifest_path),
          local_rtc)) {
        co_return;
    };

    // If I am partition 0, also delete the topic manifest
    // Note: this behavior means that absence of the topic manifest does
    // *not* imply full removal of topic, whereas absence of partition
    // manifest does imply full removal of partition data.
    if (get_ntp().tp.partition == model::partition_id{0}) {
        auto topic_manifest_path = topic_manifest::get_topic_manifest_path(
          get_ntp().ns, get_ntp().tp.topic);
        vlog(_ctxlog.debug, "Erasing topic manifest {}", topic_manifest_path);
        if (co_await tolerant_delete_object(
              _bucket,
              cloud_storage_clients::object_key(topic_manifest_path),
              local_rtc)) {
            co_return;
        };
    }
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

} // namespace cloud_storage
