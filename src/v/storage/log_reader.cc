// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "storage/log_reader.h"

#include "base/vassert.h"
#include "base/vlog.h"
#include "bytes/iobuf.h"
#include "model/fundamental.h"
#include "model/offset_interval.h"
#include "model/record.h"
#include "storage/logger.h"
#include "storage/offset_translator_state.h"
#include "storage/parser_errc.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/circular_buffer.hh>
#include <seastar/core/coroutine.hh>

#include <fmt/ostream.h>

#include <exception>

namespace {
model::record_batch make_ghost_batch(
  model::offset start_offset, model::offset end_offset, model::term_id term) {
    auto delta = end_offset - start_offset;
    auto now = model::timestamp::now();
    model::record_batch_header header = {
      .size_bytes = model::packed_record_batch_header_size,
      .base_offset = start_offset,
      .type = model::record_batch_type::ghost_batch,
      .crc = 0, // crc computed later
      .attrs = model::record_batch_attributes{} |= model::compression::none,
      .last_offset_delta = static_cast<int32_t>(delta),
      .first_timestamp = now,
      .max_timestamp = now,
      .producer_id = -1,
      .producer_epoch = -1,
      .base_sequence = -1,
      .record_count = static_cast<int32_t>(delta() + 1),
      .ctx = model::record_batch_header::context(term, ss::this_shard_id())};

    model::record_batch batch(
      std::move(header), model::record_batch::compressed_records{});

    batch.header().crc = model::crc_record_batch(batch);
    batch.header().header_crc = model::internal_header_only_crc(batch.header());
    return batch;
}

/**
 * makes multiple ghost batches required to fill the gap in a way that max batch
 * size (max of int32_t) is not exceeded
 */
std::vector<model::record_batch> make_ghost_batches(
  model::offset start_offset, model::offset end_offset, model::term_id term) {
    std::vector<model::record_batch> batches;
    while (start_offset <= end_offset) {
        static constexpr model::offset max_batch_size{
          std::numeric_limits<int32_t>::max()};
        // limit max batch size
        const model::offset delta = std::min<model::offset>(
          max_batch_size, end_offset - start_offset);

        batches.push_back(
          make_ghost_batch(start_offset, delta + start_offset, term));
        start_offset = next_offset(batches.back().last_offset());
    }

    return batches;
}

} // anonymous namespace

namespace storage {
using records_t = ss::circular_buffer<model::record_batch>;

batch_consumer::consume_result skipping_consumer::accept_batch_start(
  const model::record_batch_header& header) const {
    // check for holes in the offset range on disk
    // skip check for compacted logs
    if (unlikely(header.base_offset < _expected_next_batch)) {
        throw std::runtime_error(fmt::format(
          "incorrect offset encountered reading from disk log: "
          "expected batch offset {} (actual {})",
          _expected_next_batch,
          header.base_offset()));
    }

    /**
     * Check if parser have to be stopped
     */
    if (header.base_offset() > _reader._config.max_offset) {
        return batch_consumer::consume_result::stop_parser;
    }
    if (
      (_reader._config.strict_max_bytes || _reader._config.bytes_consumed)
      && (_reader._config.bytes_consumed + header.size_bytes)
           > _reader._config.max_bytes) {
        // signal to log reader to stop (see log_reader::is_done)
        _reader._config.over_budget = true;
        return batch_consumer::consume_result::stop_parser;
    }
    /**
     * Check if we have to skip the batch
     */
    if (header.last_offset() < _reader._config.start_offset) {
        return batch_consumer::consume_result::skip_batch;
    }
    if (
      _reader._config.type_filter
      && _reader._config.type_filter != header.type) {
        _reader._config.start_offset = header.last_offset() + model::offset(1);
        return batch_consumer::consume_result::skip_batch;
    }
    if (_reader._config.first_timestamp > header.max_timestamp) {
        // kakfa requires that we return messages >= the timestamp, it is
        // permitted to include a few earlier
        _reader._config.start_offset = header.last_offset() + model::offset(1);
        return batch_consumer::consume_result::skip_batch;
    }
    // we want to consume the batch
    return batch_consumer::consume_result::accept_batch;
}

void skipping_consumer::skip_batch_start(
  model::record_batch_header header,
  size_t /*physical_base_offset*/,
  size_t /*size_on_disk*/) {
    _expected_next_batch = header.last_offset() + model::offset(1);
}

void skipping_consumer::consume_batch_start(
  model::record_batch_header header,
  size_t /*physical_base_offset*/,
  size_t /*size_on_disk*/) {
    _expected_next_batch = header.last_offset() + model::offset(1);
    _header = header;
    _header.ctx.term = _reader._seg.offsets().get_term();
}

void skipping_consumer::consume_records(iobuf&& records) {
    _records = std::move(records);
}

ss::future<batch_consumer::stop_parser> skipping_consumer::consume_batch_end() {
    // Note: This is what keeps the train moving. the `_reader.*` transitively
    // updates the next batch to consume
    _reader.add_one(model::record_batch(
      _header, std::move(_records), model::record_batch::tag_ctor_ng{}));
    // We keep the batch in the buffer so that the reader can be cached.
    if (
      _header.last_offset() >= _reader._seg.offsets().get_stable_offset()
      || _header.last_offset() >= _reader._config.max_offset) {
        co_return stop_parser::yes;
    }
    /*
     * if the very next batch is known to be cached, then stop parsing. the next
     * read will with high probability experience a cache hit.
     */
    if (_next_cached_batch == (_header.last_offset() + model::offset(1))) {
        co_return stop_parser::yes;
    }
    if (
      _reader._config.bytes_consumed >= _reader._config.max_bytes
      || model::timeout_clock::now() >= _timeout) {
        co_return stop_parser::yes;
    }
    _header = {};
    co_return stop_parser(_reader._state.is_full());
}

void skipping_consumer::print(std::ostream& os) const {
    fmt::print(os, "storage::skipping_consumer segment {}", _reader._seg);
}

log_segment_batch_reader::log_segment_batch_reader(
  segment& seg, log_reader_config& config, probe& p) noexcept
  : _seg(seg)
  , _config(config)
  , _probe(p) {}

ss::future<std::unique_ptr<continuous_batch_parser>>
log_segment_batch_reader::initialize(
  model::timeout_clock::time_point timeout,
  std::optional<model::offset> next_cached_batch) {
    auto input = co_await _seg.offset_data_stream(
      _config.start_offset, _config.prio);
    co_return std::make_unique<continuous_batch_parser>(
      std::make_unique<skipping_consumer>(*this, timeout, next_cached_batch),
      std::move(input));
}

ss::future<> log_segment_batch_reader::close() {
    if (_iterator) {
        return _iterator->close();
    }

    return ss::make_ready_future<>();
}

void log_segment_batch_reader::add_one(model::record_batch&& batch) {
    _state.buffer.emplace_back(std::move(batch));
    const auto& b = _state.buffer.back();
    _config.start_offset = b.header().last_offset() + model::offset(1);
    const auto size_bytes = b.header().size_bytes;
    _config.bytes_consumed += size_bytes;
    _state.buffer_size += size_bytes;
    _probe.add_bytes_read(size_bytes);
    if (!_config.skip_batch_cache) {
        _seg.cache_put(b, batch_cache::is_dirty_entry::no);
    }
}
ss::future<result<records_t>>
log_segment_batch_reader::read_some(model::timeout_clock::time_point timeout) {
    /*
     * fetch batches from the cache covering the range [_base, end] where
     * end is either the configured max offset or the end of the segment.
     */
    auto cache_read = _seg.cache_get(
      _config.start_offset,
      _config.max_offset,
      _config.type_filter,
      _config.first_timestamp,
      std::min(max_buffer_size, _config.max_bytes),
      _config.skip_batch_cache);

    // handles cases where the type filter skipped batches. see
    // batch_cache_index::read for more details.
    _config.start_offset = cache_read.next_batch;

    if (
      !cache_read.batches.empty()
      || _config.start_offset > _config.max_offset) {
        _config.bytes_consumed += cache_read.memory_usage;
        _probe.add_bytes_read(cache_read.memory_usage);
        _probe.add_cached_bytes_read(cache_read.memory_usage);
        _probe.add_cached_batches_read(cache_read.batches.size());
        co_return result<records_t>(std::move(cache_read.batches));
    }

    /*
     * the log reader uses dirty offset as an upper limit stop condition in
     * order to make offsets that are in the batch cache but not yet on disk
     * visible to the reader. however, we need to enforce visibility rules for
     * on disk reads which is bound by the stable offset.
     */
    if (_config.start_offset > _seg.offsets().get_stable_offset()) {
        co_return result<records_t>(records_t{});
    }

    if (!_iterator) {
        _iterator = co_await initialize(timeout, cache_read.next_cached_batch);
    }
    auto ptr = _iterator.get();
    co_return co_await ptr->consume()
      .then([this](result<size_t> bytes_consumed) -> result<records_t> {
          if (!bytes_consumed) {
              return bytes_consumed.error();
          }
          auto tmp = std::exchange(_state, {});
          return result<records_t>(std::move(tmp.buffer));
      })
      .handle_exception_type(
        [](const std::system_error& ec) -> ss::future<result<records_t>> {
            if (ec.code().value() == EIO) {
                vassert(false, "I/O error during read!  Disk failure?");
            } else {
                return ss::make_exception_future<result<records_t>>(
                  std::current_exception());
            }
        });
}

log_reader::log_reader(
  std::unique_ptr<lock_manager::lease> l,
  log_reader_config config,
  probe& probe,
  ss::lw_shared_ptr<const storage::offset_translator_state> tr) noexcept
  : _lease(std::move(l))
  , _iterator(_lease->range.begin())
  , _config(config)
  , _expected_next(
      _config.fill_gaps
        ? std::make_optional<model::offset>(_config.start_offset)
        : std::nullopt)
  , _probe(probe)
  , _translator(std::move(tr)) {
    if (config.abort_source) {
        auto op_sub = config.abort_source.value().get().subscribe(
          [this]() noexcept { set_end_of_stream(); });

        if (op_sub) {
            _as_sub = std::move(*op_sub);
        } else {
            // already aborted
            set_end_of_stream();
        }
    }

    if (_iterator.next_seg != _lease->range.end()) {
        _iterator.reader = std::make_unique<log_segment_batch_reader>(
          **_iterator.next_seg, _config, _probe);
    }
}

ss::future<> log_reader::find_next_valid_iterator() {
    if (_config.start_offset <= _iterator.offsets().get_dirty_offset()) {
        return ss::make_ready_future<>();
    }
    std::unique_ptr<log_segment_batch_reader> tmp_reader = nullptr;
    while (_config.start_offset > _iterator.offsets().get_dirty_offset()) {
        _iterator.next_seg++;
        if (!tmp_reader) {
            tmp_reader = std::move(_iterator.reader);
        }
        if (is_end_of_stream()) {
            break;
        }
    }
    if (_iterator.next_seg != _lease->range.end()) {
        _iterator.reader = std::make_unique<log_segment_batch_reader>(
          **_iterator.next_seg, _config, _probe);
        _iterator.current_reader_seg = _iterator.next_seg;
    }
    if (tmp_reader) {
        auto raw = tmp_reader.get();
        return raw->close().finally([r = std::move(tmp_reader)] {});
    }
    return ss::make_ready_future<>();
}

bool log_reader::log_load_slice_depth_warning() const {
    const auto& depth
      = config::shard_local_cfg().debug_load_slice_warning_depth();
    return depth.has_value() && _load_slice_depth >= *depth;
}

void log_reader::maybe_log_load_slice_depth_warning(
  std::string_view context) const {
    if (!log_load_slice_depth_warning()) {
        return;
    }

    vlog(
      stlog.warn,
      "load_slice recursion warning ({}, depth {}). _last_base {} config {}",
      context,
      _load_slice_depth,
      _last_base,
      _config);

    if (_lease->range.empty()) {
        return;
    }

    const auto& segs = _lease->range;
    const auto size = segs.size();
    auto count = 0;
    constexpr auto max_segments = 10;
    for (int i = (int)size - 1; i >= 0; --i) {
        auto& seg = segs[i];
        vlog(
          stlog.warn,
          "load_slice recursion warning. lease range segment {}/{} "
          "empty {}: {}",
          i,
          size,
          seg->empty(),
          seg);
        if (!seg->empty() && ++count > max_segments) {
            // only show the last 10 segments. there could be a ton.
            break;
        }
    }
}

ss::future<log_reader::storage_t>
log_reader::do_load_slice(model::timeout_clock::time_point timeout) {
    _load_slice_depth = 0;
    while (true) {
        _load_slice_depth++;
        if (is_done()) {
            // must keep this function because, the segment might not be done
            // but offsets might have exceeded the read
            set_end_of_stream();
            co_await _iterator.close();
            co_return log_reader::storage_t{};
        }
        if (_last_base == _config.start_offset) {
            set_end_of_stream();
            co_await _iterator.close();
            co_return log_reader::storage_t{};
        }
        /**
         * We do not want to close the reader if we stopped because requested
         * range was read. This way we make it possible to reset configuration
         * and reuse underlying file input stream.
         */
        if (
          _config.start_offset > _config.max_offset
          || _config.bytes_consumed > _config.max_bytes
          || _config.over_budget) {
            set_end_of_stream();
            co_return log_reader::storage_t{};
        }
        maybe_log_load_slice_depth_warning("reading more");
        _last_base = _config.start_offset;
        ss::future<> fut = find_next_valid_iterator();
        if (is_end_of_stream()) {
            co_await std::move(fut);
            co_return log_reader::storage_t{};
        }
        std::exception_ptr e;
        try {
            co_await std::move(fut);
            auto recs = co_await _iterator.reader->read_some(timeout);
            if (!recs) {
                set_end_of_stream();

                if (!_lease->range.empty()) {
                    // Readers do not know their ntp directly: discover
                    // it by checking the segments in our lease
                    auto seg_ptr = *(_lease->range.begin());
                    vlog(
                      stlog.info,
                      "stopped reading stream[{}]: {}",
                      seg_ptr->path().get_ntp(),
                      recs.error().message());
                } else {
                    // Leases should always have a segment, but this is
                    // not a strict invariant at present, so handle the
                    // empty case.
                    vlog(
                      stlog.info,
                      "stopped reading stream: {}",
                      recs.error().message());
                }

                const auto batch_parse_err
                  = recs.error() == parser_errc::header_only_crc_missmatch
                    || recs.error()
                         == parser_errc::input_stream_not_enough_bytes;

                if (batch_parse_err) {
                    _probe.batch_parse_error();
                }
                co_await _iterator.close();
                co_return log_reader::storage_t{};
            }
            if (recs.value().empty()) {
                /*
                 * if no records are returned it may be the case that all of the
                 * batches in the segment were skipped (e.g. all control
                 * batches). thus, returning no records does not imply end of
                 * stream. instead, we continue which will advance the iterator
                 * and check end of stream.
                 */
                maybe_log_load_slice_depth_warning("load next slice");
                continue;
            }
            // Update the probe without the ghost batches.
            _probe.add_batches_read(recs.value().size());

            auto& batches = recs.value();
            if (_config.fill_gaps && _expected_next.has_value()) {
                records_t batches_filled;
                batches_filled.reserve(batches.size());
                for (auto& b : batches) {
                    if (b.base_offset() > _expected_next) {
                        auto gb = make_ghost_batches(
                          _expected_next.value(),
                          model::prev_offset(b.base_offset()),
                          b.term());
                        std::move(
                          gb.begin(),
                          gb.end(),
                          std::back_inserter(batches_filled));
                    }
                    _expected_next = model::next_offset(b.last_offset());
                    batches_filled.emplace_back(std::move(b));
                }
                co_return std::move(batches_filled);
            }
            // To keep things consistent, our internal accounting is all done in
            // untranslated offsets, even if we've been requested to return
            // translated offsets.
            _expected_next = model::next_offset(batches.back().last_offset());

            if (_config.translate_offsets) {
                vassert(
                  _translator, "Expected offset translactor to be initialized");
                for (auto& b : batches) {
                    b.header().base_offset = _translator->from_log_offset(
                      b.base_offset());
                }
            }

            co_return std::move(batches);
        } catch (...) {
            e = std::current_exception();
            set_end_of_stream();
            _probe.batch_parse_error();
        }
        // Non-exceptional cases should have continued or early-returned above.
        vassert(e, "Expected exception");
        co_await _iterator.close();
        std::rethrow_exception(e);
    }
}

static inline bool is_finished_offset(segment_set& s, model::offset o) {
    if (s.empty()) {
        return true;
    }

    for (int i = (int)s.size() - 1; i >= 0; --i) {
        auto& seg = s[i];
        if (!seg->empty()) {
            return o > seg->offsets().get_dirty_offset();
        }
    }
    return true;
}
bool log_reader::is_done() {
    return is_end_of_stream()
           || is_finished_offset(_lease->range, _config.start_offset);
}

timequery_result batch_timequery(
  const model::record_batch& b,
  model::offset min_offset,
  model::timestamp t,
  model::offset max_offset) {
    auto query_interval = model::bounded_offset_interval::checked(
      min_offset, max_offset);

    // If the timestamp matches something mid-batch, then
    // parse into the batch far enough to find it: this
    // happens when we had CreateTime input, such that
    // records in the batch have different timestamps.
    model::offset result_o = b.base_offset();
    model::timestamp result_t = b.header().first_timestamp;
    if (!b.compressed()) {
        b.for_each_record(
          [&result_o, &result_t, &b, query_interval, t](
            const model::record& r) -> ss::stop_iteration {
              auto record_o = model::offset{r.offset_delta()} + b.base_offset();
              auto record_t = model::timestamp(
                b.header().first_timestamp() + r.timestamp_delta());
              if (record_t >= t && query_interval.contains(record_o)) {
                  result_o = record_o;
                  result_t = record_t;
                  return ss::stop_iteration::yes;
              } else {
                  return ss::stop_iteration::no;
              }
          });
    }
    return {result_o, result_t};
}

} // namespace storage
