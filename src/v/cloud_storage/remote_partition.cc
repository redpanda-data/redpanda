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

#include "base/vlog.h"
#include "cloud_storage/async_manifest_view.h"
#include "cloud_storage/logger.h"
#include "cloud_storage/materialized_resources.h"
#include "cloud_storage/offset_translation_layer.h"
#include "cloud_storage/partition_manifest.h"
#include "cloud_storage/partition_manifest_downloader.h"
#include "cloud_storage/remote_path_provider.h"
#include "cloud_storage/remote_segment.h"
#include "cloud_storage/tx_range_manifest.h"
#include "cloud_storage/types.h"
#include "cloud_storage_clients/types.h"
#include "model/fundamental.h"
#include "model/timestamp.h"
#include "net/connection.h"
#include "ssx/future-util.h"
#include "ssx/watchdog.h"
#include "storage/log_reader.h"
#include "storage/parser_errc.h"
#include "storage/types.h"
#include "utils/retry_chain_node.h"
#include "utils/stream_utils.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/circular_buffer.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/queue.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/temporary_buffer.hh>

#include <boost/range/adaptor/reversed.hpp>

#include <chrono>
#include <exception>
#include <iterator>
#include <stdexcept>
#include <variant>

using namespace std::chrono_literals;

namespace cloud_storage {

using data_t = model::record_batch_reader::data_t;
using storage_t = model::record_batch_reader::storage_t;

remote_partition::iterator remote_partition::get_or_materialize_segment(
  const remote_segment_path& path,
  const segment_meta& meta,
  segment_units unit) {
    _as.check();

    if (auto iter = _segments.find(meta.base_offset); iter != _segments.end()) {
        vlog(
          _ctxlog.debug,
          "Reusing materialized segment for meta {} with path {}",
          meta,
          path);

        return iter;
    }

    auto st = std::make_unique<materialized_segment_state>(
      meta, path, *this, std::move(unit));
    auto [new_iter, ok] = _segments.insert(
      std::make_pair(meta.base_offset, std::move(st)));

    // Should never fire since there's no scheduling point between
    // the existence check and the insertion.
    vassert(
      ok,
      "Segment with base offset {} is already materialized",
      meta.base_offset);

    _ts_probe.segment_materialized();

    vlog(
      _ctxlog.debug,
      "Materialized new segment for meta {} with path {}",
      meta,
      path);
    _ts_probe.segment_materialized();

    return new_iter;
}

remote_partition::borrow_result_t remote_partition::borrow_next_segment_reader(
  const partition_manifest& manifest,
  storage::log_reader_config config,
  segment_units segment_unit,
  segment_reader_units segment_reader_unit,
  model::offset hint) {
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
    //   this allow us to avoid any ambiguity in case if the segment
    //   doesn't have any data. The 'segment_containing' method of the
    //   manifest takes this into account.
    // - find materialized segment or materialize the new one
    auto mit = manifest.end();
    if (hint == model::offset{}) {
        // This code path is only used for the first lookup. It
        // could be either lookup by kafka offset or by timestamp.
        if (config.first_timestamp) {
            auto maybe_meta = manifest.timequery(*config.first_timestamp);
            if (maybe_meta) {
                mit = manifest.segment_containing(maybe_meta->base_offset);
            }
        } else {
            // In this case the lookup is performed by kafka offset.
            // Normally, this would be the first lookup done by the
            // partition_record_batch_reader_impl. This lookup will
            // skip segments without data batches (the logic is implemented
            // inside the partition_manifest).
            mit = manifest.segment_containing(ko);
            if (mit == manifest.end()) {
                // Segment that matches exactly can't be found in the manifest.
                // In this case we want to start scanning from the beginning of
                // the partition if the start of the manifest is contained by
                // the scan range.
                auto so = manifest.get_start_kafka_offset().value_or(
                  kafka::offset::min());
                if (
                  model::offset_cast(config.start_offset) < so
                  && model::offset_cast(config.max_offset) > so) {
                    mit = manifest.begin();
                }
            }
        }
    } else {
        mit = manifest.segment_containing(hint);
        while (mit != manifest.end()) {
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
    const auto partition_start_offset
      = _manifest_view->stm_manifest().full_log_start_offset();
    while (mit != manifest.end()
           && mit->committed_offset < partition_start_offset) {
        // Make sure to skip segments that have been removed via archival GC,
        // which wouldn't be reflected in a spillover manifest's segment list.
        //
        // NOTE: offset-based queries can easily reject an out-of-range request
        // before getting here, but that isn't the case for timequeries.
        ++mit;
    }
    if (mit == manifest.end()) {
        // No such segment
        return borrow_result_t{};
    }
    auto iter = _segments.find(mit->base_offset);
    if (iter != _segments.end()) {
        if (
          iter->second->segment->get_segment_path()
          != manifest.generate_segment_path(
            *mit, _manifest_view->path_provider())) {
            // The segment was replaced and doesn't match metadata anymore. We
            // want to avoid picking it up because otherwise we won't be able to
            // make any progress.
            offload_segment(iter->first);
            iter = _segments.end();
        }
    }
    if (iter == _segments.end()) {
        auto path = manifest.generate_segment_path(
          *mit, _manifest_view->path_provider());
        iter = get_or_materialize_segment(path, *mit, std::move(segment_unit));
    }
    auto mit_committed_offset = mit->committed_offset;
    auto next_it = std::next(std::move(mit));
    while (next_it != manifest.end()) {
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
    model::offset next_offset = next_it == manifest.end()
                                  ? model::offset{}
                                  : next_it->base_offset;
    return borrow_result_t{
      .reader = iter->second->borrow_reader(
        config, _ctxlog, _probe, _ts_probe, std::move(segment_reader_unit)),
      .next_segment_offset = next_offset};
}

class partition_record_batch_reader_impl final
  : public model::record_batch_reader::impl {
public:
    explicit partition_record_batch_reader_impl(
      ss::shared_ptr<remote_partition> part,
      ss::lw_shared_ptr<storage::offset_translator_state> ot_state,
      ssx::semaphore_units units) noexcept
      : _rtc(part->_as)
      , _ctxlog(cst_log, _rtc, part->get_ntp().path())
      , _partition(std::move(part))
      , _ot_state(std::move(ot_state))
      , _gate_guard(_partition->_gate)
      , _units(std::move(units)) {
        auto ntp = _partition->get_ntp();
        vlog(_ctxlog.trace, "Constructing reader {}", ntp);
    }

    ss::future<> start(storage::log_reader_config config) {
        if (config.abort_source) {
            vlog(_ctxlog.debug, "abort_source is set");
            _partition_reader_as = config.abort_source;
            auto sub = config.abort_source->get().subscribe(
              [this](const std::optional<std::exception_ptr>& eptr) noexcept {
                  auto reason = eptr.has_value()
                                  ? net::is_disconnect_exception(*eptr)
                                  : "shutdown";
                  if (reason) {
                      vlog(
                        _ctxlog.debug,
                        "abort requested via config.abort_source: {}",
                        reason);
                  } else {
                      vlog(
                        _ctxlog.debug,
                        "abort requested via config.abort_source");
                  }
                  if (_seg_reader) {
                      _partition->evict_segment_reader(std::move(_seg_reader));
                  }
              });
            if (sub) {
                _as_sub = std::move(*sub);
            } else {
                vlog(_ctxlog.debug, "abort_source is triggered in c-tor");
                _seg_reader = {};
            }
        }

        vlog(
          _ctxlog.trace,
          "partition_record_batch_reader_impl:: start: creating cursor: {}",
          config);
        co_await init_cursor(config);
        vlog(
          _ctxlog.trace,
          "partition_record_batch_reader_impl:: start: created cursor: {}",
          config);
        _partition->_ts_probe.reader_created();
    }

    ~partition_record_batch_reader_impl() noexcept override {
        auto ntp = _partition->get_ntp();
        vlog(_ctxlog.trace, "Destructing reader {}", ntp);
        _partition->_ts_probe.reader_destroyed();
        if (_seg_reader) {
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

    bool is_end_of_stream() const override { return _seg_reader == nullptr; }

    void throw_on_external_abort() {
        _partition->_as.check();

        if (
          _partition_reader_as
          && _partition_reader_as.value().get().abort_requested()) {
            // Avoid logging boring exceptions
            try {
                _partition_reader_as.value().get().check();
            } catch (...) {
                auto eptr = std::current_exception();
                if (ssx::is_shutdown_exception(eptr)) {
                    // Shutdown exception is not expected here. We're only
                    // using this abort source to propagate exceptions.
                    // The exception will be handled correctly but we need
                    // to have an audit trail.
                    vlog(_ctxlog.error, "Unexpected shutdown error: {}", eptr);
                } else {
                    auto reason = net::is_disconnect_exception(eptr);
                    if (reason.has_value()) {
                        // This indicates that the exception is
                        // uninteresting and can be ignored for good.
                        // Convert it to abort_requested_exception.
                        vlog(
                          _ctxlog.debug,
                          "Kafka client disconnected: {}",
                          reason.value());
                        eptr = std::make_exception_ptr(
                          ss::abort_requested_exception());
                    }
                }
                std::rethrow_exception(eptr);
            }
        }
    }

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
            if (_seg_reader->config().over_budget) {
                vlog(
                  _ctxlog.debug,
                  "We're over-budget, stopping, config: {}",
                  _seg_reader->config());
                // We need to stop in such way that will keep the
                // reader in the reusable state, so we could reuse
                // it on next iteration

                // The existing state have to be rebuilt
                dispose_current_reader();
                co_return storage_t{};
            }
            while (co_await maybe_reset_reader()) {
                if (_partition->_gate.is_closed()) {
                    co_await set_end_of_stream();
                    co_return storage_t{};
                }

                throw_on_external_abort();
                auto reader_delta = _seg_reader->current_delta();
                if (
                  !_ot_state->empty()
                  && _ot_state->last_delta() > reader_delta) {
                    // It's not safe to call 'read_sone' with the current
                    // offset translator state because delta offset of the
                    // current reader is below last delta registered by the
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
                      _seg_reader->config(),
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
                  _seg_reader->config());

                auto result = co_await _seg_reader->read_some(
                  deadline, *_ot_state);
                throw_on_external_abort();

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
                } else {
                    auto current_ko = _ot_state->from_log_offset(
                      _seg_reader->current_rp_offset());
                    vlog(
                      _ctxlog.debug,
                      "No results, current rp offset: {}, current kafka "
                      "offset: {}, max rp offset: "
                      "{}",
                      _seg_reader->current_rp_offset(),
                      current_ko,
                      _seg_reader->config().max_offset);
                    if (current_ko > _seg_reader->config().max_offset) {
                        // Reader overshoot the offset. If we will not reset
                        // the stream the loop inside the
                        // record_batch_reader will keep calling this method
                        // again and again. We will be returning empty
                        // result every time because the current offset
                        // overshoot the max allowed offset. Resetting the
                        // segment reader fixes the issue.
                        //
                        // We can get into the situation when the current
                        // reader returns empty result in several cases:
                        // - we reached max_offset (covered here)
                        // - we reached end of stream (covered above right
                        //   after the 'read_some' call)
                        //
                        // If we reached max-bytes then the result won't be
                        // empty. It will have at least one record batch.
                        co_await set_end_of_stream();
                    }
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
        if (_seg_reader) {
            co_await set_end_of_stream();
        }
        if (unknown_exception_ptr) {
            std::rethrow_exception(unknown_exception_ptr);
        }

        vlog(
          _ctxlog.debug,
          "EOS reached, reader available: {}, is end of stream: {}",
          static_cast<bool>(_seg_reader),
          is_end_of_stream());
        co_return storage_t{};
    }

    void print(std::ostream& o) override {
        o << "cloud_storage_partition_record_batch_reader";
    }

private:
    /// Return or evict currently referenced reader
    void dispose_current_reader() {
        if (_seg_reader) {
            _partition->return_segment_reader(std::move(_seg_reader));
        }
    }

    ss::future<> init_cursor(storage::log_reader_config config) {
        auto segment_unit = co_await _partition->materialized()
                              .get_segment_units(config.abort_source);
        auto segment_reader_unit
          = co_await _partition->materialized().get_segment_reader_units(
            config.abort_source);

        async_view_search_query_t query;
        if (config.first_timestamp.has_value()) {
            query = async_view_timestamp_query(
              model::offset_cast(config.start_offset),
              config.first_timestamp.value(),
              model::offset_cast(config.max_offset));
        } else {
            // NOTE: config.start_offset actually contains kafka offset
            // stored using model::offset type.
            query = model::offset_cast(config.start_offset);
        }
        // Find manifest that contains requested offset or timestamp
        auto cur = co_await _partition->_manifest_view->get_cursor(query);
        if (cur.has_failure()) {
            if (cur.error() == error_outcome::shutting_down) {
                co_return;
            }

            // Out of range queries are unexpected. The caller must take care
            // to send only valid queries to remote_partition. I.e. the fetch
            // handler does such validation. Similar validation is done inside
            // remote partition.
            //
            // Out of range at this point is due to a race condition or due to
            // a bug. In both cases the only valid action is to throw an
            // exception and let the caller deal with it. If the caller doesn't
            // handle it it leads to a closed kafka connection which the
            // end clients retry.
            if (cur.error() == error_outcome::out_of_range) {
                ss::visit(
                  query,
                  [&](model::offset) {
                      vassert(
                        false,
                        "Unreachable code. Remote partition doesn't know how "
                        "to "
                        "handle model::offset queries.");
                  },
                  [&](kafka::offset query_offset) {
                      // Bug or retention racing with the query.
                      const auto log_start_offset
                        = _partition->_manifest_view->stm_manifest()
                            .full_log_start_kafka_offset();

                      if (
                        log_start_offset && query_offset < *log_start_offset) {
                          vlog(
                            _ctxlog.warn,
                            "Manifest query below the log's start Kafka "
                            "offset: "
                            "{} < {}",
                            query_offset(),
                            log_start_offset.value()());
                      }
                  },
                  [&](const async_view_timestamp_query& query_ts) {
                      // Special case, it can happen when a timequery falls
                      // below the clean offset. Caused when the query races
                      // with retention/gc.
                      const auto& spillovers = _partition->_manifest_view
                                                 ->stm_manifest()
                                                 .get_spillover_map();

                      bool timestamp_inside_spillover
                        = query_ts.ts()
                          <= spillovers.get_max_timestamp_column()
                               .last_value()
                               .value_or(model::timestamp::min()());

                      if (timestamp_inside_spillover) {
                          vlog(
                            _ctxlog.debug,
                            "Manifest query raced with retention and the "
                            "result "
                            "is below the clean/start offset for {}",
                            query_ts);
                      }
                  });
            }

            throw std::runtime_error(fmt::format(
              "Failed to query spillover manifests: {}, query: {}",
              cur.error(),
              query));
        }
        _view_cursor = std::move(cur.value());
        co_await _view_cursor->with_manifest(
          [this,
           config,
           segment_unit = std::move(segment_unit),
           segment_reader_unit = std::move(segment_reader_unit)](
            const partition_manifest& manifest) mutable {
              initialize_reader_state(
                manifest,
                config,
                std::move(segment_unit),
                std::move(segment_reader_unit));
          });
        co_return;
    }

    // Initialize object using remote_partition as a source
    void initialize_reader_state(
      const partition_manifest& manifest,
      const storage::log_reader_config& config,
      segment_units segment_unit,
      segment_reader_units segment_reader_unit) {
        vlog(
          _ctxlog.debug,
          "partition_record_batch_reader_impl initialize reader state, config: "
          "{}",
          config);
        auto [reader, next_offset] = find_cached_reader(
          manifest,
          config,
          std::move(segment_unit),
          std::move(segment_reader_unit));
        if (reader) {
            _seg_reader = std::move(reader);
            _next_segment_base_offset = next_offset;
            return;
        }
        vlog(
          _ctxlog.debug,
          "partition_record_batch_reader_impl initialize reader state - "
          "segment not "
          "found, config: {}",
          config);
        _seg_reader = {};
        _next_segment_base_offset = {};
    }

    remote_partition::borrow_result_t find_cached_reader(
      const partition_manifest& manifest,
      const storage::log_reader_config& config,
      segment_units segment_unit,
      segment_reader_units segment_reader_unit) {
        if (!_partition || _partition->_manifest_view->stm_manifest().empty()) {
            return {};
        }
        auto res = _partition->borrow_next_segment_reader(
          manifest,
          config,
          std::move(segment_unit),
          std::move(segment_reader_unit));
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
    /// it will transition into completed state with no reader
    /// attached.
    ss::future<bool> maybe_reset_reader() {
        vlog(_ctxlog.debug, "maybe_reset_reader called");
        if (!_seg_reader) {
            co_return false;
        }
        if (
          _seg_reader->config().start_offset
          > _seg_reader->config().max_offset) {
            vlog(
              _ctxlog.debug,
              "maybe_reset_stream called - stream already consumed, start "
              "{}, "
              "max {}",
              _seg_reader->config().start_offset,
              _seg_reader->config().max_offset);
            // Entire range is consumed, detach from remote_partition and
            // close the reader.
            co_await set_end_of_stream();
            co_return false;
        }
        vlog(
          _ctxlog.debug,
          "maybe_reset_reader, config start_offset: {}, reader max_offset: "
          "{}",
          _seg_reader->config().start_offset,
          _seg_reader->max_rp_offset());

        // The next offset should be below the next segment base offset if the
        // reader has not finished. If the next offset to be read from has
        // reached the next segment but the reader is not finished, then the
        // state is inconsistent.
        if (_seg_reader->is_eof()) {
            auto prev_max_offset = _seg_reader->max_rp_offset();
            auto config = _seg_reader->config();
            vlog(
              _ctxlog.debug,
              "maybe_reset_stream condition triggered after offset: {}, "
              "reader's current log offset: {}, config.start_offset: {}, "
              "reader's max log offset: {}, is EOF: {}, next base_offset "
              "estimate: {}",
              prev_max_offset,
              _seg_reader->current_rp_offset(),
              config.start_offset,
              _seg_reader->max_rp_offset(),
              _seg_reader->is_eof(),
              _next_segment_base_offset);
            _partition->evict_segment_reader(std::move(_seg_reader));
            vlog(
              _ctxlog.debug,
              "initializing new segment reader {}, next offset {}, manifest "
              "cursor status: {}",
              config.start_offset,
              _next_segment_base_offset,
              _view_cursor->get_status());

            auto segment_unit = co_await _partition->materialized()
                                  .get_segment_units(config.abort_source);
            auto segment_reader_unit
              = co_await _partition->materialized().get_segment_reader_units(
                config.abort_source);
            auto maybe_manifest = _view_cursor->manifest();
            if (
              maybe_manifest.has_value()
              && _next_segment_base_offset == model::offset{}
              && _view_cursor->get_status()
                   == async_manifest_view_cursor_status::
                     materialized_spillover) {
                // End of the manifest is reached, but the cursor is pointing at
                // the spillover manifest. We need to reset the cursor to the
                // next manifest and try to find the next segment there.
                vlog(
                  _ctxlog.debug,
                  "maybe_reset_reader, end of the manifest is reached, "
                  "resetting cursor to the next manifest");
                auto stop = co_await _view_cursor->next_iter();
                if (stop == ss::stop_iteration::yes) {
                    vlog(_ctxlog.debug, "maybe_reset_reader, last manifest");
                    co_return false;
                }
                try {
                    _next_segment_base_offset
                      = co_await _view_cursor->with_manifest(
                        [&](const partition_manifest& m) {
                            return m.get_start_offset().value_or(
                              model::offset{});
                        });
                } catch (...) {
                    vlog(
                      _ctxlog.debug,
                      "maybe_reset_reader, failed to get next manifest: {}",
                      std::current_exception());
                    co_return false;
                }
                // NOTE: maybe_manifest is invalidated at this point
                maybe_manifest = _view_cursor->manifest();
            }
            if (
              maybe_manifest.has_value()
              && _next_segment_base_offset != model::offset{}) {
                // Our segment lookup may return incorrect results if the
                // offset we're looking for has been moved out of this manifest
                // (e.g. spillover of the STM manifest).
                auto& manifest = *maybe_manifest;
                if (
                  unlikely(
                    _initial_stm_start_offset.has_value()
                    && _initial_stm_start_offset != manifest.get_start_offset()
                    && (!manifest.get_start_offset().has_value() || manifest.get_start_offset() > _next_segment_base_offset))) {
                    vlog(
                      _ctxlog.debug,
                      "maybe_reset_reader, STM manifest start offset moved to "
                      "{} while iterating to {}. Stopping iteration",
                      manifest.get_start_offset(),
                      _next_segment_base_offset);
                    co_return false;
                }

                auto [new_reader, new_next_offset]
                  = _partition->borrow_next_segment_reader(
                    maybe_manifest.value(),
                    config,
                    std::move(segment_unit),
                    std::move(segment_reader_unit),
                    _next_segment_base_offset);
                _next_segment_base_offset = new_next_offset;
                _seg_reader = std::move(new_reader);
            }
            if (maybe_manifest.has_value() && _seg_reader != nullptr) {
                vassert(
                  prev_max_offset != _seg_reader->max_rp_offset(),
                  "Progress stall detected, ntp: {}, max offset of prev "
                  "reader: {}, max offset of the new reader {}",
                  _partition->get_ntp(),
                  prev_max_offset,
                  _seg_reader->max_rp_offset());
            }
        }
        vlog(
          _ctxlog.debug,
          "maybe_reset_reader completed, reader is present: {}, is end of "
          "stream: {}",
          static_cast<bool>(_seg_reader),
          is_end_of_stream());
        co_return static_cast<bool>(_seg_reader);
    }

    /// Transition reader to the completed state. Stop tracking state in
    /// the 'remote_partition'
    ss::future<> set_end_of_stream() {
        if (!_seg_reader) {
            co_return;
        }
        // It's critical that we swap out the reader before calling stop().
        // Otherwise, another fiber may swap it out while we're stopping!
        auto reader = std::move(_seg_reader);
        co_await reader->stop();
    }

    retry_chain_node _rtc;
    retry_chain_logger _ctxlog;

    ss::shared_ptr<remote_partition> _partition;
    /// Manifest view cursor
    std::unique_ptr<async_manifest_view_cursor> _view_cursor;

    // Start of the STM manifest at the time the cursor was constructed, if
    // constructed pointing at the STM manifest.
    //
    // It's critical for correctness that as readers look for segments in the
    // STM manifest, that they check whether the STM has changed (e.g. because
    // of spillover). If so, segment lookups within the manifest may
    // incorrectly consider a reduced set of segments and subsequently skip
    // segments.
    //
    // Set to nullopt if view_cursor doesn't point at the STM manifest, or if
    // the STM manifest has no start offset (it's empty), in which case such a
    // check needn't be done.
    //
    // NOTE: the reader iterates through at most one manifest
    // TODO: make this less fragile.
    std::optional<model::offset> _initial_stm_start_offset{std::nullopt};

    ss::lw_shared_ptr<storage::offset_translator_state> _ot_state;
    /// Reader state that was borrowed from the materialized_segment_state
    std::unique_ptr<remote_segment_batch_reader> _seg_reader;
    /// Cancellation subscription
    ss::abort_source::subscription _as_sub;
    /// Reference to the abort source of the partition reader
    storage::opt_abort_source_t _partition_reader_as;
    /// Guard for the partition gate
    ss::gate::holder _gate_guard;
    model::offset _next_segment_base_offset{};
    /// Contains offset of the first produced record batch or min()
    /// if no data were produced yet
    model::offset _first_produced_offset{};

    /// RAII units managed by `materialized_resources` to limit concurrent
    /// readers, we simply carry the object around and then free on destruction.
    ssx::semaphore_units _units;
};

remote_partition::remote_partition(
  ss::shared_ptr<async_manifest_view> m,
  remote& api,
  cache& c,
  cloud_storage_clients::bucket_name bucket,
  partition_probe& probe)
  : _ntp(m->get_ntp())
  , _rtc(_as)
  , _ctxlog(cst_log, _rtc, m->get_ntp().path())
  , _api(api)
  , _cache(c)
  , _manifest_view(m)
  , _bucket(std::move(bucket))
  , _probe(probe)
  , _ts_probe(api.materialized().get_read_path_probe()) {}

ss::future<> remote_partition::start() {
    // Fiber that consumers from _eviction_list and calls stop on items before
    // destroying them.
    ssx::spawn_with_gate(_gate, [this] { return run_eviction_loop(); });
    co_return;
}

void remote_partition::evict_segment_reader(
  std::unique_ptr<remote_segment_batch_reader> reader) {
    _eviction_pending.emplace_back(std::move(reader));
    _has_evictions_cvar.signal();
}

void remote_partition::evict_segment(
  ss::lw_shared_ptr<remote_segment> segment) {
    _eviction_pending.emplace_back(std::move(segment));
    _has_evictions_cvar.signal();
}

ss::future<> remote_partition::run_eviction_loop() {
    // Evict readers asynchronously.
    // NOTE: exits when the condition variable is broken.
    while (true) {
        co_await _has_evictions_cvar.wait(
          [this] { return !_eviction_pending.empty(); });
        // This watchdog is used to detect situations when the eviction loop
        // got stuck. The deadline is set to 5 minutes to avoid false positives.
        // The callback is self sufficient and can outlive the remote_partition
        // instance.
        watchdog wd(300s, [ntp = _ntp] {
            vlog(cst_log.error, "Eviction loop for partition {} stuck", ntp);
        });
        auto eviction_in_flight = std::exchange(_eviction_pending, {});
        co_await ss::max_concurrent_for_each(
          eviction_in_flight, 200, [this](auto&& rs_variant) {
              return std::visit(
                [this](auto&& rs) {
                    try {
                        return ssx::ignore_shutdown_exceptions(rs->stop());
                    } catch (...) {
                        vlog(
                          _ctxlog.error,
                          "Unexpected exception in the background eviction "
                          "fiber {}",
                          std::current_exception());
                        throw;
                    }
                },
                rs_variant);
          });
    }
}

kafka::offset remote_partition::first_uploaded_offset() {
    vassert(
      _manifest_view->stm_manifest().size() > 0,
      "The manifest for {} is not expected to be empty",
      _ntp);
    auto so
      = _manifest_view->stm_manifest().full_log_start_kafka_offset().value();
    vlog(_ctxlog.trace, "remote partition first_uploaded_offset: {}", so);
    return so;
}

kafka::offset remote_partition::next_kafka_offset() {
    vassert(
      _manifest_view->stm_manifest().size() > 0,
      "The manifest for {} is not expected to be empty",
      _ntp);
    auto next = _manifest_view->stm_manifest().get_next_kafka_offset().value();
    vlog(_ctxlog.debug, "remote partition next_kafka_offset: {}", next);
    return next;
}

const model::ntp& remote_partition::get_ntp() const { return _ntp; }

bool remote_partition::is_data_available() const {
    const auto& stmm = _manifest_view->stm_manifest();
    const auto start_offset = stmm.get_start_offset();

    // If the start offset for the STM region is not set, then the cloud log is
    // empty. There's one special case, where the start offset is set, and yet
    // the cloud log should be considered emtpy: retention in the STM region
    // advanced the start offset, but the garbage collection, and subsequent
    // truncation, did not happen yet.
    return start_offset.has_value()
           && start_offset.value() <= stmm.get_last_offset();
}

uint64_t remote_partition::cloud_log_size() const {
    return _manifest_view->stm_manifest().cloud_log_size();
}

ss::future<> remote_partition::serialize_json_manifest_to_output_stream(
  ss::output_stream<char>& output) const {
    auto tmp = _manifest_view->stm_manifest().clone();
    co_await tmp.serialize_json(output);
}

// returns term last kafka offset
ss::future<std::optional<kafka::offset>>
remote_partition::get_term_last_offset(model::term_id term) const {
    const auto res = co_await _manifest_view->get_term_last_offset(term);
    if (res.has_error()) {
        throw std::system_error(res.error());
    } else {
        co_return res.value();
    }
}

ss::future<std::vector<model::tx_range>>
remote_partition::aborted_transactions(offset_range offsets) {
    auto guard = _gate.hold();
    const auto& stm_manifest = _manifest_view->stm_manifest();
    const auto so = stm_manifest.get_start_offset();
    std::vector<model::tx_range> result;
    if (so.has_value() && offsets.begin > model::offset_cast(so.value())) {
        // Here we have to use kafka offsets to locate the segments and
        // redpanda offsets to extract aborted transactions metadata because
        // tx-manifests contains redpanda offsets.
        std::deque<ss::lw_shared_ptr<remote_segment>> remote_segs;
        for (auto it = stm_manifest.segment_containing(offsets.begin);
             it != stm_manifest.end();
             ++it) {
            if (it->base_offset > offsets.end_rp) {
                break;
            }

            // Note: This is not buletproof: the segment might be
            // re-uploaded/merged while waiting for the units which may result
            // in a failure to materialise. This should be transient however.
            // One solution for this is to grab all the required segment units
            // up front at the start of the function.
            auto segment_unit = co_await materialized().get_segment_units(
              std::nullopt);
            auto path = stm_manifest.generate_segment_path(
              *it, _manifest_view->path_provider());
            auto m = get_or_materialize_segment(
              path, *it, std::move(segment_unit));
            remote_segs.emplace_back(m->second->segment);
        }
        for (const auto& segment : remote_segs) {
            auto tx = co_await segment->aborted_transactions(
              offsets.begin_rp, offsets.end_rp);
            std::copy(tx.begin(), tx.end(), std::back_inserter(result));
        }
    } else {
        // Target archive section of the log
        std::deque<std::pair<segment_meta, remote_segment_path>>
          meta_to_materialize;

        meta_to_materialize.clear();
        auto cur_res = co_await _manifest_view->get_cursor(offsets.begin);
        if (cur_res.has_failure()) {
            if (cur_res.error() == error_outcome::shutting_down) {
                throw std::runtime_error("Async manifest view shutting down");
            }

            vlog(
              _ctxlog.error,
              "Failed to traverse archive part of the log: {}",
              cur_res.error());
            throw std::runtime_error(fmt_with_ctx(
              fmt::format, "Failed to get the cursor {}", cur_res.error()));
        }
        auto cursor = std::move(cur_res.value());
        co_await for_each_manifest(
          std::move(cursor),
          [&offsets, &meta_to_materialize, this](
            ssx::task_local_ptr<const partition_manifest> manifest) {
              for (auto it = manifest->segment_containing(offsets.begin);
                   it != manifest->end();
                   ++it) {
                  if (it->base_offset > offsets.end_rp) {
                      return ss::stop_iteration::yes;
                  }
                  auto path = manifest->generate_segment_path(
                    *it, _manifest_view->path_provider());
                  meta_to_materialize.emplace_back(*it, path);
              }
              return ss::stop_iteration::no;
          });

        for (const auto& [meta, path] : meta_to_materialize) {
            auto segment_unit = co_await materialized().get_segment_units(
              std::nullopt);
            auto m = get_or_materialize_segment(
              path, meta, std::move(segment_unit));
            auto tx = co_await m->second->segment->aborted_transactions(
              offsets.begin_rp, offsets.end_rp);
            std::copy(tx.begin(), tx.end(), std::back_inserter(result));
        }
    }

    // Adjacent segments might return the same transaction record.
    // In this case we will have a duplicate. The duplicates will always
    // be located next to each other in the sequence.
    auto last = std::unique(result.begin(), result.end());
    result.erase(last, result.end());
    vlog(
      _ctxlog.debug,
      "found {} aborted transactions for {}-{} offset range ({}-{} before "
      "offset translation)",
      result.size(),
      offsets.begin,
      offsets.end,
      offsets.begin_rp,
      offsets.end_rp);
    co_return result;
}

ss::future<> remote_partition::stop() {
    vlog(_ctxlog.debug, "remote partition stop {} segments", _segments.size());
    watchdog wd(300s, [ntp = get_ntp()] {
        vlog(cst_log.error, "remote_partition {} stop operation stuck", ntp);
    });

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
void remote_partition::return_segment_reader(
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
        evict_segment_reader(std::move(reader));
    }
}

size_t remote_partition::reader_mem_use_estimate() noexcept {
    return sizeof(storage::translating_reader)
           + sizeof(partition_record_batch_reader_impl);
}

ss::future<storage::translating_reader> remote_partition::make_reader(
  storage::log_reader_config config,
  std::optional<model::timeout_clock::time_point> deadline) {
    std::ignore = deadline;
    vlog(
      _ctxlog.debug,
      "remote partition make_reader invoked (waiting for units), config: {}, "
      "num segments {}",
      config,
      _segments.size());

    auto units = co_await _api.materialized().get_partition_reader_units(
      config.abort_source);
    auto ot_state = ss::make_lw_shared<storage::offset_translator_state>(
      get_ntp());

    vlog(
      _ctxlog.trace,
      "remote partition make_reader invoked (units acquired), config: {}, "
      "num segments {}",
      config,
      _segments.size());

    auto impl = std::make_unique<partition_record_batch_reader_impl>(
      shared_from_this(), ot_state, std::move(units));
    co_await impl->start(config);
    co_return storage::translating_reader{
      model::record_batch_reader(std::move(impl)), std::move(ot_state)};
}

ss::future<std::optional<storage::timequery_result>>
remote_partition::timequery(storage::timequery_config cfg) {
    const auto& stm_manifest = _manifest_view->stm_manifest();
    if (stm_manifest.size() == 0) {
        vlog(_ctxlog.debug, "timequery: no segments");
        co_return std::nullopt;
    }

    auto start_offset = std::max(
      cfg.min_offset,
      kafka::offset_cast(stm_manifest.full_log_start_kafka_offset().value()));

    // Synthesize a log_reader_config from our timequery_config
    storage::log_reader_config config(
      start_offset,
      cfg.max_offset,
      0,
      2048, // We just need one record batch
      cfg.prio,
      cfg.type_filter,
      cfg.time,
      cfg.abort_source,
      cfg.client_address);

    // Construct a reader that will skip to the requested timestamp
    // by virtue of log_reader_config::start_timestamp
    auto translating_reader = co_await make_reader(config);

    // Read one batch from the reader to learn the offset
    model::record_batch_reader::storage_t data
      = co_await model::consume_reader_to_memory(
        std::move(translating_reader.reader), model::no_timeout);

    auto& batches = std::get<model::record_batch_reader::data_t>(data);
    vlog(_ctxlog.debug, "timequery: {} batches", batches.size());

    if (batches.size()) {
        co_return storage::batch_timequery(
          *(batches.begin()), cfg.min_offset, cfg.time, cfg.max_offset);
    } else {
        co_return std::nullopt;
    }
}

bool remote_partition::bounds_timestamp(model::timestamp t) const {
    auto last_seg = _manifest_view->stm_manifest().last_segment();
    if (last_seg.has_value()) {
        return t <= last_seg.value().max_timestamp;
    } else {
        return false;
    }
}

static constexpr ss::lowres_clock::duration finalize_timeout = 20s;
static constexpr ss::lowres_clock::duration finalize_backoff = 1s;

/// When a remote_partition is being destroyed for the last time, we save this
/// subset of its state into a background fiber that tries to do a final update
/// of remote metadata.
struct finalize_data {
    model::ntp ntp;
    model::initial_revision_id revision;
    cloud_storage_clients::bucket_name bucket;
    iobuf serialized_manifest;
    model::offset insync_offset;
};

ss::future<> finalize_background(
  remote& api, finalize_data data, remote_path_provider path_provider) {
    // This function runs as a detached background fiber, so has no shutdown
    // logic of its own: our remote operations will be shut down when the
    // `remote` object is shut down.
    ss::abort_source& as = api.as();

    retry_chain_node local_rtc(as, finalize_timeout, finalize_backoff);

    partition_manifest remote_manifest(data.ntp, data.revision);

    partition_manifest_downloader dl(
      data.bucket, path_provider, data.ntp, data.revision, api);
    auto manifest_get_result = co_await dl.download_manifest(
      local_rtc, &remote_manifest);
    if (manifest_get_result.has_error()) {
        vlog(
          cst_log.error,
          "[{}] Failed to fetch manifest during finalize(). Error: {}",
          data.ntp,
          manifest_get_result.error());
        co_return;
    }
    if (
      manifest_get_result.value()
      == find_partition_manifest_outcome::no_matching_manifest) {
        vlog(
          cst_log.error,
          "[{}] Failed to fetch manifest during finalize(). Not found",
          data.ntp);
        co_return;
    }

    if (remote_manifest.get_insync_offset() > data.insync_offset) {
        // Our local manifest is behind the remote: return a copy of the
        // remote manifest for use in deletion
        vlog(
          cst_log.debug,
          "[{}] Remote manifest has newer state than local ({} > {}), using "
          "this "
          "for deletion during finalize",
          data.ntp,
          remote_manifest.get_insync_offset(),
          data.insync_offset);
    } else if (remote_manifest.get_insync_offset() < data.insync_offset) {
        // The remote manifest is out of date, upload a fresh one
        vlog(
          cst_log.info,
          "[{}] Remote manifest has older state than local ({} < {}), "
          "attempting to "
          "upload latest manifest",
          data.ntp,
          remote_manifest.get_insync_offset(),
          data.insync_offset);

        const auto key = cloud_storage_clients::object_key{
          path_provider.partition_manifest_path(data.ntp, data.revision)};
        auto manifest_put_result = co_await api.upload_object(
          {.transfer_details
           = {.bucket = data.bucket, .key = key, .parent_rtc = local_rtc},
           .type = upload_type::manifest,
           .payload = std::move(data.serialized_manifest)});

        if (manifest_put_result != upload_result::success) {
            vlog(
              cst_log.warn,
              "[{}] Failed to write manifest during finalize(), remote "
              "manifest may "
              "be left in incomplete state after topic deletion. Error: {}",
              data.ntp,
              manifest_put_result);
        }
    } else {
        // Remote and local state is in sync, no action required.
        vlog(
          cst_log.debug,
          "[{}] Remote manifest is in sync with local state during "
          "finalize "
          "(insync_offset={})",
          data.ntp,
          data.insync_offset);
    }
}

void remote_partition::finalize() {
    vlog(_ctxlog.info, "Finalizing remote storage state...");

    // We do this in the background, because
    //  - this function is called by
    //    the controller, and we don't want to block the controller if our
    //    remote requests get slow.
    //  - if remote requests are slow, we don't want to delay releasing
    //    the memory occupied by remote_partition and cluster::partition.
    //    This function reduces memory footprint to just the serialized
    //    manifest that is passed into the background fiber.

    const auto& stm_manifest = _manifest_view->stm_manifest();
    auto serialized_manifest = stm_manifest.to_iobuf();

    finalize_data data{
      .ntp = get_ntp(),
      .revision = stm_manifest.get_revision_id(),
      .bucket = _bucket,
      .serialized_manifest = std::move(serialized_manifest),
      .insync_offset = stm_manifest.get_insync_offset()};

    ssx::spawn_with_gate(
      _api.gate(),
      [&api = _api,
       data = std::move(data),
       pp = _manifest_view->path_provider().copy()]() mutable -> ss::future<> {
          return finalize_background(api, std::move(data), pp.copy());
      });
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
ss::future<remote_partition::erase_result> remote_partition::erase(
  cloud_storage::remote& api,
  cloud_storage_clients::bucket_name bucket,
  const remote_path_provider& path_provider,
  partition_manifest manifest,
  remote_manifest_path manifest_path,
  retry_chain_node& parent_rtc) {
    retry_chain_node local_rtc(&parent_rtc);
    retry_chain_logger ctxlog(cst_log, local_rtc);

    size_t segments_to_remove_count = 0;
    std::deque<cloud_storage_clients::object_key> objects_to_remove;

    auto replaced_segments = manifest.lw_replaced_segments();
    for (const auto& lw_meta : replaced_segments) {
        const auto path = manifest.generate_segment_path(
          lw_meta, path_provider);
        ++segments_to_remove_count;

        objects_to_remove.emplace_back(path);
        objects_to_remove.emplace_back(
          cloud_storage::generate_remote_tx_path(path));
        objects_to_remove.emplace_back(
          cloud_storage::generate_index_path(path));
    }

    for (const auto& meta : manifest) {
        const auto path = manifest.generate_segment_path(meta, path_provider);
        ++segments_to_remove_count;

        objects_to_remove.emplace_back(path);
        objects_to_remove.emplace_back(
          cloud_storage::generate_remote_tx_path(path));
        objects_to_remove.emplace_back(
          cloud_storage::generate_index_path(path));
    }

    vlog(
      ctxlog.info,
      "[{}] Erasing {} segments",
      manifest.get_ntp(),
      segments_to_remove_count);
    if (
      co_await api.delete_objects(
        bucket, std::move(objects_to_remove), local_rtc)
      != upload_result::success) {
        vlog(
          ctxlog.info,
          "[{}] Failed to erase some segments, deferring deletion",
          manifest.get_ntp());
        co_return erase_result::failed;
    }

    // If we got this far, we succeeded deleting all objects referenced by
    // the manifest, so many delete the manifest itself.
    vlog(
      ctxlog.debug,
      "[{}] Erasing partition manifest {}",
      manifest.get_ntp(),
      manifest_path);
    if (
      co_await api.delete_object(
        bucket, cloud_storage_clients::object_key(manifest_path), local_rtc)
      != upload_result::success) {
        vlog(
          ctxlog.info,
          "[{}] Failed to erase {}, deferring deletion",
          manifest.get_ntp(),
          manifest_path);
        co_return erase_result::failed;
    };

    co_return erase_result::erased;
}

void remote_partition::offload_segment(model::offset o) {
    vlog(_ctxlog.debug, "about to offload segment {}", o);
    auto it = _segments.find(o);
    vassert(it != _segments.end(), "Can't find offset {}", o);
    it->second->offload(this);
    _segments.erase(it);
}

materialized_resources& remote_partition::materialized() {
    return _api.materialized();
}

cache_usage_target remote_partition::get_cache_usage_target() const {
    // minimum and nice-to-have chunks per partition
    constexpr auto min_chunks = 1;
    constexpr auto wanted_chunks = 20;

    // minimum and nice-to-have segments per partition are roughly the same
    // (compared to chunks) as segments are much lager. for example, 2
    // segments at the default segment size is about the same as 20 default
    // sized chunks.
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

    // we may not have any materialized segments, but we can decode some
    // info about them anyway from the manifest.
    auto seg = _manifest_view->stm_manifest().last_segment();
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
