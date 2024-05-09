/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/remote_segment.h"

#include "bytes/iobuf.h"
#include "bytes/iostream.h"
#include "cloud_storage/cache_service.h"
#include "cloud_storage/download_exception.h"
#include "cloud_storage/logger.h"
#include "cloud_storage/partition_manifest.h"
#include "cloud_storage/remote_segment_index.h"
#include "cloud_storage/segment_chunk_data_source.h"
#include "cloud_storage/tx_range_manifest.h"
#include "cloud_storage/types.h"
#include "config/configuration.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/record_batch_types.h"
#include "raft/consensus.h"
#include "resource_mgmt/io_priority.h"
#include "ssx/future-util.h"
#include "ssx/sformat.h"
#include "ssx/watchdog.h"
#include "storage/parser.h"
#include "storage/segment_index.h"
#include "utils/retry_chain_node.h"
#include "utils/stream_utils.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/circular_buffer.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/io_priority_class.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/queue.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/timed_out_error.hh>
#include <seastar/core/when_all.hh>
#include <seastar/coroutine/all.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/log.hh>

#include <fmt/core.h>

#include <exception>

namespace {
class bounded_stream final : public ss::data_source_impl {
public:
    bounded_stream(ss::input_stream<char>& stream, size_t upto)
      : _stream{stream}
      , _upto{upto} {}

    ss::future<ss::temporary_buffer<char>> get() override {
        auto buf = co_await _stream.read_up_to(_upto);
        _upto -= buf.size();
        co_return buf;
    }

private:
    ss::input_stream<char>& _stream;
    size_t _upto;
};

} // namespace

namespace cloud_storage {

std::filesystem::path
generate_index_path(const cloud_storage::remote_segment_path& p) {
    return fmt::format("{}.index", p().native());
}

using namespace std::chrono_literals;

static constexpr size_t max_consume_size = 128_KiB;

inline void expiry_handler_impl(ss::promise<ss::file>& pr) {
    pr.set_exception(ss::timed_out_error());
}

static ss::sstring
generate_log_prefix(const segment_meta& meta, const model::ntp& ntp) {
    return ssx::sformat(
      "{} [{}:{}]", ntp.path(), meta.base_offset, meta.committed_offset);
}

remote_segment::remote_segment(
  remote& r,
  cache& c,
  cloud_storage_clients::bucket_name bucket,
  const remote_segment_path& path,
  const model::ntp& ntp,
  const segment_meta& meta,
  retry_chain_node& parent,
  partition_probe& probe,
  ts_read_path_probe& ts_probe)
  : _api(r)
  , _cache(c)
  , _bucket(std::move(bucket))
  , _ntp(ntp)
  , _path(path)
  , _index_path(generate_index_path(path))
  , _chunk_root(fmt::format("{}_chunks", _path().native()))
  , _term(meta.segment_term)
  , _base_rp_offset(meta.base_offset)
  , _base_offset_delta(std::clamp(
      meta.delta_offset, model::offset_delta(0), model::offset_delta::max()))
  , _max_rp_offset(meta.committed_offset)
  , _base_timestamp(meta.base_timestamp)
  , _size(meta.size_bytes)
  , _rtc(&parent)
  , _ctxlog(cst_log, _rtc, generate_log_prefix(meta, ntp))
  , _wait_list(expiry_handler_impl)
  , _cache_backoff_jitter(cache_thrash_backoff)
  , _compacted(meta.is_compacted)
  , _sname_format(meta.sname_format)
  , _metadata_size_hint(meta.metadata_size_hint)
  , _chunk_size(config::shard_local_cfg().cloud_storage_cache_chunk_size())
  // The max hydrated chunks per segment are either 0.5 of total number of
  // chunks possible, or in case of small segments the total number of chunks
  // possible. In the second case we should be able to hydrate the entire
  // segment in chunks at the same time. In the first case roughly half the
  // segment may be hydrated at a time.
  , _chunks_in_segment(
      std::max(static_cast<uint64_t>(ceil(_size / _chunk_size)), 1UL))
  , _probe(probe)
  , _ts_probe(ts_probe) {
    vassert(_chunk_size != 0, "cloud_storage_cache_chunk_size should not be 0");

    if (
      _chunks_in_segment <= config::shard_local_cfg()
                              .cloud_storage_min_chunks_per_segment_threshold) {
        _max_hydrated_chunks = _chunks_in_segment;
    } else {
        _max_hydrated_chunks = std::max(
          static_cast<uint64_t>(
            static_cast<double>(_chunks_in_segment)
            * config::shard_local_cfg()
                .cloud_storage_hydrated_chunks_per_segment_ratio),
          uint64_t{1});
    }

    vassert(
      _max_hydrated_chunks > 0 && _max_hydrated_chunks <= _chunks_in_segment,
      "invalid max hydrated chunks: {}, chunks in segment: {}, chunk size: {}, "
      "segment size: {}",
      _max_hydrated_chunks,
      _chunks_in_segment,
      _chunk_size,
      _size);

    vlog(
      _ctxlog.debug,
      "total {} chunks in segment of size {}, max hydrated chunks at a time: "
      "{}, chunk size: {}",
      _chunks_in_segment,
      _size,
      _max_hydrated_chunks,
      _chunk_size);

    _chunks_api.emplace(*this, _max_hydrated_chunks);

    if (config::shard_local_cfg().cloud_storage_disable_chunk_reads) {
        vlog(_ctxlog.debug, "fallback mode enabled");
        _fallback_mode = fallback_mode::yes;
    }

    // run hydration loop in the background
    _hydration_loop_running = true;
    ssx::background = run_hydrate_bg();
}

const model::ntp& remote_segment::get_ntp() const { return _ntp; }

const model::offset remote_segment::get_max_rp_offset() const {
    return _max_rp_offset;
}

const model::offset_delta remote_segment::get_base_offset_delta() const {
    return _base_offset_delta;
}

const model::offset remote_segment::get_base_rp_offset() const {
    return _base_rp_offset;
}

const kafka::offset remote_segment::get_base_kafka_offset() const {
    return _base_rp_offset - _base_offset_delta;
}

size_t remote_segment::get_segment_size() const { return _size; }

const model::term_id remote_segment::get_term() const { return _term; }

ss::future<> remote_segment::stop() {
    if (_stopped) {
        vlog(_ctxlog.warn, "remote segment {} already stopped", _path);
        co_return;
    }

    watchdog wd(300s, [path = _path] {
        vlog(cst_log.error, "remote_segment {} stop operation stuck", path);
    });

    vlog(_ctxlog.debug, "remote segment stop: gate closing");
    _bg_cvar.broken();
    co_await _gate.close();
    vlog(_ctxlog.debug, "remote segment stop: gate closed");
    if (_data_file) {
        co_await _data_file.close().handle_exception(
          [this](std::exception_ptr err) {
              vlog(
                _ctxlog.error, "Error '{}' while closing the '{}'", err, _path);
          });
    }

    if (_chunks_api) {
        vlog(_ctxlog.debug, "waiting for chunk api to stop");
        co_await _chunks_api->stop();
        vlog(_ctxlog.debug, "chunk api stopped");
    }
    _stopped = true;
}

ss::future<storage::segment_reader_handle>
remote_segment::data_stream(size_t pos, ss::io_priority_class io_priority) {
    vlog(_ctxlog.debug, "remote segment file input stream at {}", pos);
    ss::gate::holder g(_gate);
    co_await hydrate();
    ss::file_input_stream_options options{};
    options.buffer_size = config::shard_local_cfg().storage_read_buffer_size();
    options.read_ahead
      = config::shard_local_cfg().storage_read_readahead_count();
    options.io_priority_class = io_priority;
    auto data_stream = ss::make_file_input_stream(
      _data_file, pos, std::move(options));
    co_return storage::segment_reader_handle(std::move(data_stream));
}

ss::future<remote_segment::input_stream_with_offsets>
remote_segment::offset_data_stream(
  kafka::offset start,
  kafka::offset end,
  std::optional<model::timestamp> first_timestamp,
  ss::io_priority_class io_priority,
  storage::opt_abort_source_t as) {
    vlog(_ctxlog.debug, "remote segment file input stream at offset {}", start);
    ss::gate::holder g(_gate);

    co_await hydrate(as);

    std::optional<offset_index::find_result> indexed_pos;
    std::optional<uint16_t> prefetch_override = std::nullopt;

    // Perform index lookup by timestamp or offset. This reduces the number
    // of hydrated chunks required to serve the request.
    if (first_timestamp) {
        // The dominant cost of a time query on a remote partition is promoting
        // the chunks into our local cache: once they're here, the cost of a
        // scan is comparatively small.

        prefetch_override = 0;
        indexed_pos = maybe_get_offsets(*first_timestamp);
    } else {
        indexed_pos = maybe_get_offsets(start);
    }

    // If the index lookup failed, scan the entire segement starting from the
    // first chunk.
    offset_index::find_result pos = indexed_pos.value_or(
      offset_index::find_result{
        .rp_offset = _base_rp_offset,
        .kaf_offset = _base_rp_offset - _base_offset_delta,
        .file_pos = 0,
      });

    vlog(
      _ctxlog.debug,
      "Offset data stream start reading at {}, log offset {}, delta {}",
      pos.file_pos,
      pos.rp_offset,
      pos.rp_offset - pos.kaf_offset);
    ss::file_input_stream_options options{};
    options.buffer_size = config::shard_local_cfg().storage_read_buffer_size();
    options.read_ahead
      = config::shard_local_cfg().storage_read_readahead_count();
    options.io_priority_class = io_priority;

    ss::input_stream<char> data_stream;
    if (is_legacy_mode_engaged()) {
        data_stream = ss::make_file_input_stream(
          _data_file, pos.file_pos, std::move(options));
    } else {
        auto chunk_ds = std::make_unique<chunk_data_source_impl>(
          _chunks_api.value(),
          *this,
          pos.kaf_offset,
          end,
          pos.file_pos,
          std::move(options),
          prefetch_override);
        data_stream = ss::input_stream<char>{
          ss::data_source{std::move(chunk_ds)}};
    }

    co_return input_stream_with_offsets{
      .stream = std::move(data_stream),
      .rp_offset = pos.rp_offset,
      .kafka_offset = pos.kaf_offset,
    };
}

std::optional<offset_index::find_result>
remote_segment::maybe_get_offsets(kafka::offset kafka_offset) {
    if (!_index) {
        return {};
    }
    auto pos = _index->find_kaf_offset(kafka_offset);
    if (!pos) {
        return {};
    }
    vlog(
      _ctxlog.debug,
      "Using index to locate Kafka offset {}, the result is rp-offset: {}, "
      "kafka-offset: "
      "{}, file-pos: {}",
      kafka_offset,
      pos->rp_offset,
      pos->kaf_offset,
      pos->file_pos);
    return pos;
}

size_t remote_segment::estimate_memory_use() const {
    // NOTE: this is imprecise and doesn't account for certain
    // things (e.g. chunks api)
    size_t res = sizeof(remote_segment);
    if (_index) {
        res += _index->estimate_memory_use();
    }
    return res;
}

std::optional<offset_index::find_result>
remote_segment::maybe_get_offsets(model::timestamp ts) {
    if (!_index) {
        return {};
    }
    auto pos = _index->find_timestamp(ts);
    if (!pos) {
        return {};
    }
    vlog(
      _ctxlog.debug,
      "Using index to locate timestamp {}, the result is rp-offset: {}, "
      "kafka-offset: "
      "{}, file-pos: {}",
      ts,
      pos->rp_offset,
      pos->kaf_offset,
      pos->file_pos);
    return pos;
}

/**
 * Called by do_hydrate_segment on the stream the S3 remote creates for
 * a GET response: pass the dat through into the cache.
 */
ss::future<uint64_t> remote_segment::put_segment_in_cache_and_create_index(
  uint64_t size_bytes,
  space_reservation_guard& reservation,
  ss::input_stream<char> s) {
    offset_index tmpidx(
      get_base_rp_offset(),
      get_base_kafka_offset(),
      0,
      remote_segment_sampling_step_bytes,
      _base_timestamp);
    auto [sparse, sput] = input_stream_fanout<2>(std::move(s), 1);
    auto parser = make_remote_segment_index_builder(
      get_ntp(),
      std::move(sparse),
      tmpidx,
      _base_offset_delta,
      remote_segment_sampling_step_bytes);
    auto fparse = parser->consume().finally(
      [parser] { return parser->close(); });
    auto fput
      = _cache.put(_path, sput, reservation).finally([sref = std::ref(sput)] {
            return sref.get().close();
        });
    auto [rparse, rput] = co_await ss::when_all(
      std::move(fparse), std::move(fput));
    bool index_prepared = true;
    if (rparse.failed()) {
        auto parse_exception = rparse.get_exception();
        vlog(
          _ctxlog.warn,
          "Failed to build a remote_segment index, error: {}",
          parse_exception);
        index_prepared = false;
    }
    if (rput.failed()) {
        auto put_exception = rput.get_exception();
        vlog(
          _ctxlog.warn,
          "Failed to write a segment file to cache, error: {}",
          put_exception);
        std::rethrow_exception(put_exception);
    }
    if (index_prepared) {
        auto index_reservation = co_await _cache.reserve_space(
          storage::segment_index::estimate_size(size_bytes), 1);
        auto index_stream = make_iobuf_input_stream(tmpidx.to_iobuf());
        co_await _cache.put(
          generate_index_path(_path), index_stream, index_reservation);
        _index = std::move(tmpidx);
    }
    co_return size_bytes;
}

ss::future<uint64_t> remote_segment::put_segment_in_cache(
  uint64_t size_bytes,
  space_reservation_guard& reservation,
  ss::input_stream<char> s) {
    try {
        co_await _cache.put(_path, s, reservation).finally([&s] {
            return s.close();
        });
    } catch (...) {
        auto put_exception = std::current_exception();
        vlog(
          _ctxlog.warn,
          "Failed to write a segment file to cache, error: {}",
          put_exception);
        std::rethrow_exception(put_exception);
    }

    co_return size_bytes;
}

ss::future<> remote_segment::put_chunk_in_cache(
  space_reservation_guard& reservation,
  ss::input_stream<char> stream,
  chunk_start_offset_t chunk_start) {
    try {
        co_await _cache.put(get_path_to_chunk(chunk_start), stream, reservation)
          .finally([&stream] { return stream.close(); });
    } catch (...) {
        auto put_exception = std::current_exception();
        vlog(
          _ctxlog.warn,
          "Failed to write segment chunk {} to cache, error: {}",
          chunk_start,
          put_exception);
        std::rethrow_exception(put_exception);
    }
}

ss::future<> remote_segment::do_hydrate_segment() {
    retry_chain_node local_rtc(
      cache_hydration_timeout, cache_hydration_backoff, &_rtc);

    // RAII reservation object represents the disk space this put will consume
    auto reservation = co_await _cache.reserve_space(
      _size + storage::segment_index::estimate_size(_size), 1);

    track_hydration t{_ts_probe};
    auto res = co_await _api.download_segment(
      _bucket,
      _path,
      [this, &reservation](uint64_t size_bytes, ss::input_stream<char> s) {
          // Always create the index because we are in legacy mode if we ended
          // up hydrating the segment. Legacy mode indicates a missing index, so
          // we create it here on the fly using the downloaded segment.
          return put_segment_in_cache_and_create_index(
            size_bytes, reservation, std::move(s));
      },
      local_rtc);

    if (res != download_result::success) {
        vlog(
          _ctxlog.debug,
          "Failed to hydrating a segment {}, {} waiter will be "
          "invoked",
          _path,
          _wait_list.size());
        throw download_exception(res, _path);
    }
}

ss::future<> remote_segment::do_hydrate_index() {
    retry_chain_node local_rtc(
      cache_hydration_timeout, cache_hydration_backoff, &_rtc);

    offset_index ix(
      _base_rp_offset,
      _base_rp_offset - _base_offset_delta,
      0,
      remote_segment_sampling_step_bytes,
      _base_timestamp);

    auto result = co_await _api.download_index(
      _bucket, remote_segment_path{_index_path}, ix, local_rtc);

    if (result != download_result::success) {
        throw download_exception(result, _index_path);
    }

    _index = std::move(ix);
    _coarse_index.emplace(
      _index->build_coarse_index(_chunk_size, _index_path.native()));
    co_await _chunks_api->start();
    auto buf = _index->to_iobuf();

    auto reservation = co_await _cache.reserve_space(buf.size_bytes(), 1);
    auto str = make_iobuf_input_stream(std::move(buf));
    co_await _cache.put(_index_path, str, reservation).finally([&str] {
        return str.close();
    });
}

ss::future<> remote_segment::do_hydrate_txrange() {
    ss::gate::holder guard(_gate);
    retry_chain_node local_rtc(
      cache_hydration_timeout, cache_hydration_backoff, &_rtc);
    if (_sname_format == segment_name_format::v3 && _metadata_size_hint == 0) {
        // The tx-manifest is empty, no need to download it, and
        // avoid putting this empty manifest into the cache.
        _tx_range.emplace();
        co_return;
    }

    tx_range_manifest manifest(_path);

    if (!_compacted) {
        // Record barches generated by the aborted transactions are removed
        // during compaction process. We don't need to try to download them
        // but we need to write empty tx manifest to the cache.
        auto res = co_await _api.maybe_download_manifest(
          _bucket, manifest.get_manifest_path(), manifest, local_rtc);

        vlog(
          _ctxlog.debug,
          "hydrate tx_range {}, {}, {} waiters will be invoked",
          manifest.get_manifest_path(),
          res,
          _wait_list.size());

        if (
          res != download_result::success && res != download_result::notfound) {
            throw download_exception(res, _path);
        }

        auto [stream, size] = co_await manifest.serialize();
        auto reservation = co_await _cache.reserve_space(size, 1);
        co_await _cache.put(manifest.get_manifest_path(), stream, reservation)
          .finally([&s = stream]() mutable { return s.close(); });
    }

    _tx_range = std::move(manifest).get_tx_range();
}

ss::future<bool> remote_segment::do_materialize_segment() {
    if (_data_file) {
        co_return true;
    }
    auto maybe_file = co_await _cache.get(_path);
    if (!maybe_file) {
        // We could got here because the cache check returned
        // 'cache_element_status::available' but right after
        // that the file was evicted from cache. It's also
        // possible (but very unlikely) that we got here after
        // successful hydration which was immediately followed
        // by eviction. In any case we should just re-hydrate
        // the segment.
        vlog(
          _ctxlog.info,
          "Segment {} was deleted from cache and need to be "
          "re-hydrated, {} waiter are pending",
          _path,
          _wait_list.size());

        // If we got here, the cache is in a stressed state: it has
        // evicted an object that we probably only just promoted.  We can
        // live-lock if many readers are all trying to promote their objects
        // concurrently and none of them is getting all their objects in
        // the cache at the same time: reduce chance of this by backing off.
        // TODO: this should be a sleep_abortable, but remote_segment does not
        // have an abort source.
        co_await ss::sleep(_cache_backoff_jitter.next_duration());

        co_return false;
    }
    _data_file = maybe_file->body;
    if (!_index) {
        // Materialize index state if it's not materialized yet.
        // If do_hydrate_segment was called _index will be populated
        // and this branch won't be triggered. If the segment was
        // available on disk then this branch will read it and populate
        // the _index.
        co_await maybe_materialize_index();
    }
    co_return true;
}

ss::future<bool> remote_segment::do_materialize_txrange() {
    if (_tx_range) {
        vlog(
          _ctxlog.debug,
          "materialize tx_range, {} transactions available",
          _tx_range->size());
        co_return true;
    }
    if (_compacted) {
        // construct empty tx-range vector for compacted segments
        // since they don't have any aborted transactions by design
        _tx_range.emplace();
        co_return true;
    }
    auto path = generate_remote_tx_path(_path);
    if (auto cache_item = co_await _cache.get(path); cache_item.has_value()) {
        // The cache item is expected to be present if the this method is
        // called.
        vlog(_ctxlog.debug, "Trying to materialize tx_range '{}'", path);
        tx_range_manifest manifest(_path);
        try {
            ss::file_input_stream_options options{};
            options.buffer_size
              = config::shard_local_cfg().storage_read_buffer_size;
            options.read_ahead
              = config::shard_local_cfg().storage_read_readahead_count;
            options.io_priority_class
              = priority_manager::local().shadow_indexing_priority();
            auto inp_stream = ss::make_file_input_stream(
              cache_item->body, options);
            co_await manifest.update(std::move(inp_stream));
            _tx_range = std::move(manifest).get_tx_range();
            vlog(
              _ctxlog.debug,
              "materialize tx_range, {} transactions materialized",
              _tx_range->size());
        } catch (...) {
            vlog(
              _ctxlog.warn,
              "Failed to materialize tx_range '{}'. Error: {}",
              path,
              std::current_exception());
        }
        co_await cache_item->body.close();
    } else {
        vlog(
          _ctxlog.debug,
          "tx_range '{}' is not available in cache, retrying",
          path);
        co_return false;
    }
    co_return true;
}

ss::future<bool> remote_segment::maybe_materialize_index() {
    if (_index) {
        vlog(_ctxlog.debug, "index already materialized");
        co_return true;
    }

    ss::gate::holder guard(_gate);
    auto path = generate_index_path(_path);
    offset_index ix(
      _base_rp_offset,
      _base_rp_offset - _base_offset_delta,
      0,
      remote_segment_sampling_step_bytes,
      _base_timestamp);
    if (auto cache_item = co_await _cache.get(path); cache_item.has_value()) {
        // The cache item is expected to be present if the segment is present
        // so it's very unlikely that we will call this method if there is no
        // segment. This can only happen due to race condition between the
        // remote_segment materialization and cache eviction.
        vlog(_ctxlog.info, "Trying to materialize index '{}'", path);
        try {
            ss::file_input_stream_options options{};
            options.buffer_size
              = config::shard_local_cfg().storage_read_buffer_size;
            options.read_ahead
              = config::shard_local_cfg().storage_read_readahead_count;
            options.io_priority_class
              = priority_manager::local().shadow_indexing_priority();
            auto inp_stream = ss::make_file_input_stream(
              cache_item->body, options);
            iobuf state;
            auto out_stream = make_iobuf_ref_output_stream(state);
            co_await ss::copy(inp_stream, out_stream).finally([&inp_stream] {
                return inp_stream.close();
            });
            ix.from_iobuf(std::move(state));
            _index = std::move(ix);
            _coarse_index.emplace(
              _index->build_coarse_index(_chunk_size, _index_path.native()));
            co_await _chunks_api->start();
        } catch (...) {
            // In case of any failure during index materialization just continue
            // without the index.
            vlog(
              _ctxlog.warn,
              "Failed to materialize index '{}'. Error: {}",
              path,
              std::current_exception());
            co_return false;
        }
        co_await cache_item->body.close();
        co_return true;
    } else {
        vlog(_ctxlog.info, "Index '{}' is not available", path);
        co_return false;
    }
}

void remote_segment::set_waiter_errors(const std::exception_ptr& err) {
    while (!_wait_list.empty()) {
        auto& p = _wait_list.front();
        p.set_exception(err);
        _wait_list.pop_front();
    }

    fragmented_vector<chunk_request> chunk_waiters;
    chunk_waiters.swap(_chunk_waiters);
    for (auto& w : chunk_waiters) {
        w.promise.set_exception(err);
    }
};

bool remote_segment::is_legacy_mode_engaged() const {
    return _fallback_mode || _sname_format <= segment_name_format::v2;
}

void remote_segment::switch_to_legacy_mode() {
    _fallback_mode = fallback_mode::yes;
    for (auto& waiter : _chunk_waiters) {
        waiter.promise.set_exception(
          std::runtime_error{"chunk download aborted"});
    }
}

bool remote_segment::is_state_materialized() const {
    if (is_legacy_mode_engaged()) {
        return bool(_data_file);
    } else {
        return _index.has_value();
    }
}

ss::future<> remote_segment::run_hydrate_bg() {
    ss::gate::holder guard(_gate);

    // Track whether we have seen our objects in the cache during the loop
    // below, so that we can detect regression: one object falling out the
    // cache while the other is read.  We will back off on regression.

    hydration_loop_state hydration{_cache, _path, _ctxlog};
    auto tx_path = generate_remote_tx_path(_path)();

    hydration.add_request(
      tx_path,
      [this] { return do_hydrate_txrange(); },
      [this] { return do_materialize_txrange(); },
      hydration_request::kind::tx);

    if (!is_legacy_mode_engaged()) {
        hydration.add_request(
          generate_index_path(_path),
          [this] { return do_hydrate_index(); },
          [this] { return maybe_materialize_index(); },
          hydration_request::kind::index);
    }

    while (!_gate.is_closed()) {
        try {
            co_await _bg_cvar.wait([this] {
                return !_wait_list.empty() || !_chunk_waiters.empty()
                       || _gate.is_closed();
            });

            if (is_legacy_mode_engaged()) {
                vlog(
                  _ctxlog.debug, "adding full segment to hydrate paths list");
                hydration.add_request(
                  _path,
                  [this] { return do_hydrate_segment(); },
                  [this] { return do_materialize_segment(); },
                  hydration_request::kind::segment);
                vlog(_ctxlog.debug, "removing index from hydrate paths list");
                hydration.remove_request(_index_path);
            }

            vlog(
              _ctxlog.debug,
              "Segment {} requested, {} consumers are awaiting, data file is "
              "{}",
              _path,
              _wait_list.size(),
              _data_file ? "available" : "not available");
            std::exception_ptr err;
            if (!is_state_materialized() || !_tx_range) {
                co_await hydration.update_current_path_states();
                if (co_await hydration.is_cache_thrashing()) {
                    co_await ss::sleep(_cache_backoff_jitter.next_duration());
                }

                co_await hydration.hydrate(_wait_list.size());
                err = hydration.current_error();
                if (!err) {
                    if (auto mat_res = co_await hydration.materialize();
                        !mat_res) {
                        continue;
                    }
                }
            }
            // Invariant: here we should have segment state materialized or
            // error to be set. If the hydration failed we will have 'err' set
            // to some value. The error needs to be propagated further.
            // Otherwise we will always have state materialized because if we
            // don't we will retry the hydration earlier.
            vassert(
              is_state_materialized() || err,
              "Segment hydration succeded but file isn't available");

            // When operating in index-only (v3 or above version of segment
            // meta), the waiter does not need the data file to be present, only
            // the index is required. This promise is still set to the data file
            // but it will be absent and unused.
            while (!_wait_list.empty()) {
                auto& p = _wait_list.front();
                if (err) {
                    p.set_exception(err);
                } else {
                    p.set_value(_data_file);
                }
                _wait_list.pop_front();
            }

            // Only download chunks if we are not in legacy mode and the index
            // is available.
            if (
              !is_legacy_mode_engaged() && is_state_materialized()
              && !_chunk_waiters.empty()) {
                vlog(
                  _ctxlog.debug,
                  "Processing {} chunk download request(s)",
                  _chunk_waiters.size());
                auto failed_requests = co_await service_chunk_requests();
                for (auto& r : failed_requests) {
                    _chunk_waiters.emplace_back(std::move(r));
                }
            }
        } catch (...) {
            const auto err = std::current_exception();
            set_waiter_errors(err);

            if (ssx::is_shutdown_exception(err)) {
                vlog(_ctxlog.debug, "Hydration loop shut down");
                break;
            } else {
                vlog(_ctxlog.error, "Error in hydration loop: {}", err);
            }
        }
    }

    // If any new download requests got queued up while we were downloading
    // chunks before the gate closed, cancel them so that the gate close does
    // not get stuck during segment stop.
    if (!_chunk_waiters.empty() && _gate.is_closed()) {
        vlog(
          _ctxlog.debug,
          "Cancelling {} pending chunk downloads during segment stop",
          _chunk_waiters.size());
        for (auto& w : _chunk_waiters) {
            w.promise.set_exception(ss::gate_closed_exception{});
        }
    }

    _hydration_loop_running = false;
}

ss::future<fragmented_vector<remote_segment::chunk_request>>
remote_segment::service_chunk_requests() {
    auto g = _gate.hold();
    fragmented_vector<ss::future<ss::file>> chunk_op_results;
    chunk_op_results.reserve(_chunk_waiters.size());

    fragmented_vector<chunk_request> requests;
    requests.swap(_chunk_waiters);

    std::ranges::transform(
      requests,
      std::back_inserter(chunk_op_results),
      [this](const auto& request) {
          vlog(_ctxlog.debug, "Downloading chunk {}", request.start);
          return hydrate_and_materialize_chunk(request.start);
      });

    auto results = co_await ss::when_all(
      chunk_op_results.begin(), chunk_op_results.end());

    fragmented_vector<chunk_request> failed;
    for (size_t i = 0; i < results.size(); ++i) {
        auto request = std::move(requests[i]);
        auto current_result = std::move(results[i]);

        ss::file materialized_handle;
        std::exception_ptr err;

        if (current_result.failed()) {
            err = current_result.get_exception();
        } else {
            materialized_handle = current_result.get();
        }

        // Materialization may fail due to cache eviction. In this case the
        // materialized handle is default initialized. Re-queue waiter which
        // will be processed again on the next iteration of run_hydrate_bg loop.
        // Continue with other requests in queue.
        if (!err && !materialized_handle) {
            vlog(
              _ctxlog.debug,
              "Failed to materialize chunk start {}, retrying",
              request.start);
            failed.emplace_back(std::move(request));
            continue;
        }

        if (err) {
            request.promise.set_exception(err);
        } else {
            request.promise.set_value(materialized_handle);
        }
    }
    co_return failed;
}

ss::future<ss::file> remote_segment::hydrate_and_materialize_chunk(
  chunk_start_offset_t start_offset) {
    auto g = _gate.hold();
    vlog(_ctxlog.debug, "Hydrating chunk {}", start_offset);
    co_await hydrate_chunk(start_offset);
    vlog(_ctxlog.debug, "Materializing chunk {}", start_offset);
    co_return co_await materialize_chunk(start_offset);
}

ss::future<ss::file>
remote_segment::download_chunk(chunk_start_offset_t chunk_start) {
    auto g = _gate.hold();
    ss::promise<ss::file> p;
    auto fut = p.get_future();
    _chunk_waiters.push_back({chunk_start, std::move(p)});
    _bg_cvar.signal();
    co_return co_await std::move(fut);
}

namespace {
void log_hydration_abort_cause(
  const retry_chain_logger& logger,
  const ss::lowres_clock::time_point& deadline,
  storage::opt_abort_source_t as) {
    if (ss::lowres_clock::now() > deadline) {
        vlog(logger.warn, "timed out while waiting for hydration");
    } else if (as.has_value() && as->get().abort_requested()) {
        // TODO it might be useful to be able to log the client info here
        // from log reader config.
        vlog(logger.debug, "consumer disconnected during hydration");
    }
}

} // namespace

ss::future<> remote_segment::do_hydrate(
  ss::abort_source& as, ss::lowres_clock::time_point deadline) {
    ss::promise<ss::file> p;
    auto fut = ssx::with_timeout_abortable(p.get_future(), deadline, as);

    _wait_list.push_back(std::move(p), ss::lowres_clock::time_point::max());
    _bg_cvar.signal();

    return fut
      .handle_exception_type(
        [this, deadline, &as](const ss::timed_out_error& ex) {
            log_hydration_abort_cause(_ctxlog, deadline, as);
            return ss::make_exception_future<ss::file>(ex);
        })
      .handle_exception_type(
        [this, deadline, &as](const ss::abort_requested_exception& ex) {
            log_hydration_abort_cause(_ctxlog, deadline, as);
            return ss::make_exception_future<ss::file>(ex);
        })
      .handle_exception_type(
        [this, deadline, &as](const download_exception& ex) {
            // If we are working with an index-only format, and index
            // download failed, we may not be able to progress. So we
            // fallback to old format where the full segment was downloaded,
            // and try to hydrate again.
            if (ex.path == _index_path && !_fallback_mode) {
                vlog(
                  _ctxlog.info,
                  "failed to download index with error [{}], switching to "
                  "fallback mode and retrying hydration.",
                  ex);
                switch_to_legacy_mode();
                return do_hydrate(as, deadline).then([] {
                    // This is an empty file to match the type returned by
                    // `fut`. The result is discarded immediately so it is
                    // unused.
                    return ss::file{};
                });
            }

            // If the download failure was something other than the index,
            // OR if we are in the fallback mode already or if we are
            // working with old format, rethrow the exception and let the
            // upper layer handle it.
            return ss::make_exception_future<ss::file>(ex);
        })
      .discard_result();
}

ss::future<> remote_segment::hydrate(storage::opt_abort_source_t as) {
    if (!_hydration_loop_running) {
        vlog(
          _ctxlog.error,
          "Segment {} hydration requested, but the hydration loop is not "
          "running",
          _path);

        return ss::make_exception_future<>(std::runtime_error(
          fmt::format("Hydration loop is not running for segment: {}", _path)));
    }

    auto g = _gate.hold();
    vlog(_ctxlog.debug, "segment {} hydration requested", _path);

    // A no-op abort source is created on heap so that `do_hydrate` can call
    // `with_timeout_abortable` if we do not have an input abort source.
    return ss::do_with(
             ss::abort_source{},
             [this, as](ss::abort_source& noop) {
                 return do_hydrate(
                   as.value_or(noop),
                   ss::lowres_clock::now()
                     + config::shard_local_cfg()
                         .cloud_storage_hydration_timeout_ms());
             })
      .finally([holder = std::move(g)] {});
}

ss::future<> remote_segment::hydrate_chunk(chunk_start_offset_t start_offset) {
    const auto path_to_start = get_path_to_chunk(start_offset);
    if (const auto status = co_await _cache.is_cached(path_to_start);
        status == cache_element_status::available) {
        vlog(
          _ctxlog.debug,
          "skipping chunk hydration for chunk path {}, it is already in "
          "cache",
          path_to_start);
        co_return;
    }

    retry_chain_node rtc{
      cache_hydration_timeout, cache_hydration_backoff, &_rtc};

    auto byte_range = _chunks_api->get_byte_range_for_chunk(
      start_offset, _size - 1);

    const auto space_required = byte_range.second - byte_range.first + 1;
    auto reserved = co_await _cache.reserve_space(space_required, 1);

    auto measurement = _ts_probe.chunk_hydration_latency();
    track_hydration t{_ts_probe};

    auto res = co_await _api.download_segment(
      _bucket,
      _path,
      [this, start_offset, &reserved](
        auto size, auto stream) -> ss::future<unsigned long> {
          return put_chunk_in_cache(reserved, std::move(stream), start_offset)
            .then([size] { return size; });
      },
      rtc,
      std::move(byte_range));

    if (res != download_result::success) {
        measurement->cancel();
        throw download_exception{res, _path};
    }

    _probe.chunk_size(space_required);
    _ts_probe.on_chunks_hydration(1);
}

ss::future<ss::file>
remote_segment::materialize_chunk(chunk_start_offset_t chunk_start) {
    auto res = co_await _cache.get(get_path_to_chunk(chunk_start));
    if (!res.has_value()) {
        co_return ss::file{};
    }
    co_return res.value().body;
}

ss::future<std::vector<model::tx_range>>
remote_segment::aborted_transactions(model::offset from, model::offset to) {
    auto g = _gate.hold();
    co_await hydrate();
    std::vector<model::tx_range> result;
    if (!_tx_range) {
        // We got NoSuchKey when we tried to download the
        // tx-manifest. This means that segment doesn't have
        // any record batches which belong to aborted transactions.
        vlog(_ctxlog.debug, "no tx-metadata available");
        co_return result;
    }
    for (const auto& it : *_tx_range) {
        if (it.last < from) {
            continue;
        }
        if (it.first > to) {
            continue;
        }
        result.push_back(it);
    }
    vlog(
      _ctxlog.debug,
      "found {} aborted transactions for {}-{} offset range in this segment",
      result.size(),
      from,
      to);
    co_return result;
}

uint64_t remote_segment::max_hydrated_chunks() const {
    return _max_hydrated_chunks;
}

chunk_start_offset_t
remote_segment::get_chunk_start_for_kafka_offset(kafka::offset koff) const {
    vassert(
      _coarse_index.has_value(),
      "cannot find byte range for kafka offset {} when coarse index is not "
      "initialized.",
      koff);

    vlog(_ctxlog.trace, "get_chunk_start_for_kafka_offset {}", koff);

    if (unlikely(_coarse_index->empty())) {
        return 0;
    }

    // TODO (abhijat) assert that koff >= segment base kafka && koff <= segment
    // end kafka
    auto it = _coarse_index->upper_bound(koff);
    // The kafka offset lies in the first chunk of the file.
    if (it == _coarse_index->begin()) {
        return 0;
    }

    it = std::prev(it);
    return static_cast<uint64_t>(it->second);
}

const offset_index::coarse_index_t& remote_segment::get_coarse_index() const {
    vassert(_coarse_index.has_value(), "coarse index is not initialized");
    return _coarse_index.value();
}

/// Batch consumer that connects to remote_segment_batch_reader.
/// It also does offset translation based on incomplete data in
/// manifests.
/// The implementation assumes that the config has kafka offsets
/// and does conversion based on that.
/// The problem is that we don't have full information regarding
/// offset translation in manifests. Because of that we can only
/// translate base_offset of every segment precisely. All other
/// offsets have to rely on state that this batch consumer maintains
/// while scanning the segment. This is not a problem since we
/// always have to scan the segments from the begining in shadow
/// indexing (the indexing to be implemented in the future). So
/// we will always be reusing an existing segment reader (with
/// data necessary for offset translation already present) or we
/// will start from the begining of the segment.
///
/// This consumer expects config.start_offset/max_offset to be
/// kafka offsets. It also returns batches with kafka offsets.
/// The log output always contains redpanda offsets unless the
/// annotation is added.
///
/// Note that the state that this consumer has can only be used
/// to translate current record batch.
class remote_segment_batch_consumer : public storage::batch_consumer {
public:
    using consume_result = storage::batch_consumer::consume_result;
    using stop_parser = storage::batch_consumer::stop_parser;

    remote_segment_batch_consumer(
      storage::log_reader_config& conf,
      remote_segment_batch_reader& parent,
      model::term_id term,
      const model::ntp& ntp,
      retry_chain_node& rtc)
      : _config(conf)
      , _seg_reader(parent)
      , _term(term)
      , _rtc(&rtc)
      , _ctxlog(cst_log, _rtc, ntp.path())
      , _filtered_types(raft::offset_translator_batch_types(ntp)) {}

    /// Translate redpanda offset to kafka offset
    ///
    /// \note this can only be applied to current record batch
    kafka::offset rp_to_kafka(model::offset k) const noexcept {
        vassert(
          k() >= _seg_reader._cur_delta(),
          "Redpanda offset {} is smaller than the delta {}",
          k,
          _seg_reader._cur_delta);
        return k - _seg_reader._cur_delta;
    }

    /// Point config.start_offset to the next record batch
    ///
    /// \param header is a record batch header with redpanda offset
    /// \note this can only be applied to current record batch
    void
    advance_config_offsets(const model::record_batch_header& header) noexcept {
        _seg_reader._cur_rp_offset = header.last_offset() + model::offset{1};

        if (header.type == model::record_batch_type::raft_data) {
            auto next = rp_to_kafka(header.last_offset()) + model::offset(1);
            if (kafka::offset_cast(next) > _config.start_offset) {
                _config.start_offset = kafka::offset_cast(next);
            }
        }
    }

    consume_result accept_batch_start(
      const model::record_batch_header& header) const override {
        vlog(
          _ctxlog.trace,
          "[{}] accept_batch_start {}, current delta: {}",
          _config.client_address,
          header,
          _seg_reader._cur_delta);

        if (
          rp_to_kafka(header.base_offset)
          > model::offset_cast(_config.max_offset)) {
            vlog(
              _ctxlog.debug,
              "[{}] accept_batch_start stop parser because {} > {}(kafka "
              "offset)",
              _config.client_address,
              header.base_offset(),
              _config.max_offset);
            return batch_consumer::consume_result::stop_parser;
        }

        // Ignore filter and always return only raft_data since there is only
        // one usecase for this reader and the offset translation logic can only
        // handle this scenario anyway.
        if (model::record_batch_type::raft_data != header.type) {
            vlog(
              _ctxlog.debug,
              "[{}] accept_batch_start skip because record batch type is {}",
              _config.client_address,
              header.type);
            _seg_reader._probe.add_bytes_skip(header.size_bytes);
            return batch_consumer::consume_result::skip_batch;
        }

        // The segment can be scanned from the begining so we should skip
        // irrelevant batches.
        if (unlikely(
              rp_to_kafka(header.last_offset())
              < model::offset_cast(_config.start_offset))) {
            vlog(
              _ctxlog.debug,
              "[{}] accept_batch_start skip because "
              "last_kafka_offset {} (last_rp_offset: {}) < "
              "config.start_offset: {}",
              _config.client_address,
              rp_to_kafka(header.last_offset()),
              header.last_offset(),
              _config.start_offset);
            _seg_reader._probe.add_bytes_skip(header.size_bytes);
            return batch_consumer::consume_result::skip_batch;
        }

        if (
          (_config.strict_max_bytes || _config.bytes_consumed)
          && (_config.bytes_consumed + header.size_bytes) > _config.max_bytes) {
            vlog(
              _ctxlog.debug,
              "[{}] accept_batch_start stop because overbudget",
              _config.client_address);
            _config.over_budget = true;
            return batch_consumer::consume_result::stop_parser;
        }

        if (_config.first_timestamp > header.max_timestamp) {
            vlog(
              _ctxlog.debug,
              "[{}] accept_batch_start skip because header timestamp is {}",
              _config.client_address,
              header.first_timestamp);
            _seg_reader._probe.add_bytes_skip(header.size_bytes);
            return batch_consumer::consume_result::skip_batch;
        }
        _seg_reader._probe.add_bytes_accept(header.size_bytes);
        // we want to consume the batch
        return batch_consumer::consume_result::accept_batch;
    }

    /// Consume batch start
    void consume_batch_start(
      model::record_batch_header header,
      size_t /*physical_base_offset*/,
      size_t /*size_on_disk*/) override {
        vlog(
          _ctxlog.trace,
          "[{}] consume_batch_start called for {}",
          _config.client_address,
          header.base_offset);
        _header = header;
        _header.ctx.term = _term;
    }

    /// Skip batch (called if accept_batch_start returned 'skip')
    void skip_batch_start(
      model::record_batch_header header,
      size_t /*physical_base_offset*/,
      size_t /*size_on_disk*/) override {
        // NOTE: that advance_config_start_offset should be called before
        // changing the _cur_delta. The _cur_delta that is be used for current
        // record batch can only account record batches in all previous batches.
        vlog(
          _ctxlog.debug,
          "[{}] skip_batch_start called for {}",
          _config.client_address,
          header.base_offset);
        advance_config_offsets(header);
        if (
          std::count(
            _filtered_types.begin(), _filtered_types.end(), header.type)
          > 0) {
            vassert(
              _seg_reader._cur_ot_state,
              "ntp {}: offset translator state for "
              "remote_segment_batch_consumer not initialized",
              _seg_reader._seg->get_ntp());

            vlog(
              _ctxlog.debug,
              "added offset translation gap [{}-{}], current state: {}",
              header.base_offset,
              header.last_offset(),
              _seg_reader._cur_ot_state);

            _seg_reader._cur_ot_state->get().add_gap(
              header.base_offset, header.last_offset());
            _seg_reader._cur_delta += header.last_offset_delta
                                      + model::offset(1);
        }
    }

    void consume_records(iobuf&& ib) override { _records = std::move(ib); }

    /// Produce batch if within memory limits
    ss::future<stop_parser> consume_batch_end() override {
        auto batch = model::record_batch{
          _header, std::move(_records), model::record_batch::tag_ctor_ng{}};

        _config.bytes_consumed += batch.size_bytes();
        advance_config_offsets(batch.header());

        // NOTE: we need to translate offset of the batch after we updated
        // start offset of the config since it assumes that the header has
        // redpanda offset.
        batch.header().base_offset = kafka::offset_cast(
          rp_to_kafka(batch.base_offset()));
        // since base offset isn't accounted into Kafka crc we need to only
        // update header_crc
        batch.header().header_crc = model::internal_header_only_crc(
          batch.header());

        size_t sz = _seg_reader.produce(std::move(batch));

        if (_config.over_budget) {
            co_return stop_parser::yes;
        }

        if (sz > max_consume_size) {
            co_return stop_parser::yes;
        }

        co_return stop_parser::no;
    }

    void print(std::ostream& o) const override {
        o << "remote_segment_batch_consumer";
    }

private:
    storage::log_reader_config& _config;
    remote_segment_batch_reader& _seg_reader;
    model::record_batch_header _header;
    iobuf _records;
    model::term_id _term;
    retry_chain_node _rtc;
    retry_chain_logger _ctxlog;
    std::vector<model::record_batch_type> _filtered_types;
};

remote_segment_batch_reader::remote_segment_batch_reader(
  ss::lw_shared_ptr<remote_segment> s,
  const storage::log_reader_config& config,
  partition_probe& probe,
  ts_read_path_probe& ts_probe,
  ssx::semaphore_units units) noexcept
  : _seg(std::move(s))
  , _config(config)
  , _probe(probe)
  , _ts_probe(ts_probe)
  , _rtc(_seg->get_retry_chain_node())
  , _ctxlog(cst_log, _rtc, _seg->get_ntp().path())
  , _cur_rp_offset(_seg->get_base_rp_offset())
  , _cur_delta(_seg->get_base_offset_delta())
  , _units(std::move(units)) {
    _ts_probe.segment_reader_created();
}

ss::future<result<ss::circular_buffer<model::record_batch>>>
remote_segment_batch_reader::read_some(
  model::timeout_clock::time_point,
  storage::offset_translator_state& ot_state) {
    ss::gate::holder h(_gate);
    if (_ringbuf.empty()) {
        if (!_parser) {
            // remote_segment_batch_reader shouldn't be used concurrently
            _parser = co_await init_parser();
        }

        if (ot_state.add_absolute_delta(_cur_rp_offset, _cur_delta)) {
            vlog(
              _ctxlog.debug,
              "offset translation: add_absolute_delta at offset {}, "
              "delta {}, current state: {}",
              _cur_rp_offset,
              _cur_delta,
              ot_state);
        }

        _cur_ot_state = ot_state;
        auto deferred = ss::defer([this] { _cur_ot_state = std::nullopt; });
        auto new_bytes_consumed = co_await _parser->consume();
        if (!new_bytes_consumed) {
            co_return new_bytes_consumed.error();
        }
        if (
          _bytes_consumed != 0 && _bytes_consumed == new_bytes_consumed.value()
          && !_config.over_budget) {
            const auto msg = fmt::format(
              "segment_reader is stuck, segment ntp: {}, _cur_rp_offset: "
              "{}, "
              "_bytes_consumed: {}, parser error state: {}, client: {}",
              _seg->get_ntp(),
              _cur_rp_offset,
              _bytes_consumed,
              _parser->error(),
              _config.client_address);
            if (_parser->error() == storage::parser_errc::end_of_stream) {
                vlog(_ctxlog.info, "{}", msg);
            } else {
                vlog(_ctxlog.error, "{}", msg);
            }
            _is_unexpected_eof = true;
            co_return ss::circular_buffer<model::record_batch>{};
        }
        _bytes_consumed = new_bytes_consumed.value();
    }
    _total_size = 0;
    co_return std::move(_ringbuf);
}

ss::future<std::unique_ptr<storage::continuous_batch_parser>>
remote_segment_batch_reader::init_parser() {
    ss::gate::holder h(_gate);
    vlog(
      _ctxlog.debug,
      "remote_segment_batch_reader::init_parser (creating stream), "
      "start_offset: {}, client: {}",
      _config.start_offset,
      _config.client_address);

    auto stream_off = co_await _seg->offset_data_stream(
      model::offset_cast(_config.start_offset),
      model::offset_cast(_config.max_offset),
      _config.first_timestamp,
      priority_manager::local().shadow_indexing_priority(),
      _config.abort_source);

    vlog(
      _ctxlog.debug,
      "remote_segment_batch_reader::init_parser (stream created), "
      "start_offset: {}, client: {}",
      _config.start_offset,
      _config.client_address);

    auto parser = std::make_unique<storage::continuous_batch_parser>(
      std::make_unique<remote_segment_batch_consumer>(
        _config, *this, _seg->get_term(), _seg->get_ntp(), _rtc),
      storage::segment_reader_handle(std::move(stream_off.stream)));
    _cur_rp_offset = stream_off.rp_offset;
    _cur_delta = stream_off.rp_offset - stream_off.kafka_offset;
    co_return parser;
}

size_t remote_segment_batch_reader::produce(model::record_batch batch) {
    ss::gate::holder h(_gate);
    vlog(_ctxlog.debug, "remote_segment_batch_reader::produce");
    _total_size += batch.size_bytes();
    _ringbuf.push_back(std::move(batch));
    return _total_size;
}

ss::future<> remote_segment_batch_reader::stop() {
    if (_stopped) {
        vlog(
          _ctxlog.warn,
          "remote_segment_batch_reader::stop called when reader already "
          "stopped");
        co_return;
    }

    watchdog wd(300s, [path = _seg->get_segment_path()] {
        vlog(
          cst_log.error,
          "remote_segment_batch_reader {} stop operation stuck",
          path);
    });

    vlog(
      _ctxlog.debug,
      "[{}] remote_segment_batch_reader::stop",
      _config.client_address);
    co_await _gate.close();
    if (_parser) {
        vlog(
          _ctxlog.debug,
          "[{}] remote_segment_batch_reader::stop - parser-close",
          _config.client_address);
        co_await _parser->close();
        _parser.reset();
    }
    _stopped = true;
}

remote_segment_batch_reader::~remote_segment_batch_reader() noexcept {
    vassert(_stopped, "Destroyed without stopping");
    _ts_probe.segment_reader_destroyed();
}

std::ostream& operator<<(std::ostream& os, hydration_request::kind kind) {
    switch (kind) {
    case hydration_request::kind::segment:
        return os << "segment";
    case hydration_request::kind::tx:
        return os << "tx-range";
    case hydration_request::kind::index:
        return os << "index";
    }
}

hydration_loop_state::hydration_loop_state(
  cache& c, remote_segment_path root, retry_chain_logger& ctxlog)
  : _cache{c}
  , _root{std::move(root)}
  , _ctxlog{ctxlog} {}

void hydration_loop_state::add_request(
  const std::filesystem::path& p,
  hydrate_action_t h,
  materialize_action_t m,
  hydration_request::kind path_kind) {
    // Do not re-add the path. A path may be added conditionally in a
    // loop, we should only add it the first time it is requested.
    if (auto it = std::find_if(
          _states.cbegin(),
          _states.cend(),
          [&p](const auto& st) { return st.path == p; });
        it != _states.end()) {
        return;
    }

    _states.push_back(
      {.path = p,
       .was_cached = false,
       .hydrate_action = std::move(h),
       .materialize_action = std::move(m),
       .path_kind = path_kind});
}

void hydration_loop_state::remove_request(const std::filesystem::path& p) {
    std::erase_if(_states, [&p](const auto& state) { return state.path == p; });
}

ss::future<> hydration_loop_state::update_current_path_states() {
    for (auto& path_state : _states) {
        path_state.current_status = co_await _cache.is_cached(path_state.path);
    }
}

ss::future<bool> hydration_loop_state::is_cache_thrashing() {
    for (auto& path_state : _states) {
        bool available = path_state.current_status
                         != cache_element_status::not_available;
        path_state.was_cached |= available;
        if (!available && path_state.was_cached) {
            vlog(
              _ctxlog.debug,
              "Cache thrashing detected while downloading {} {}, path "
              "{}, backing off",
              path_state.path_kind,
              _root,
              path_state.path);
            co_return true;
        }
    }
    co_return false;
}

ss::future<> hydration_loop_state::hydrate(size_t wait_list_size) {
    _current_error = std::exception_ptr{};
    std::vector<ss::future<>> fs;
    fs.reserve(_states.size());

    for (const auto& state : _states) {
        switch (state.current_status) {
        case cache_element_status::available:
            break;
        case cache_element_status::not_available:
            vlog(
              _ctxlog.info,
              "Hydrating {} {}, {} waiters",
              state.path_kind,
              state.path,
              wait_list_size);
            fs.push_back(state.hydrate_action());
            break;
        case cache_element_status::in_progress:
            vassert(false, "{} is already in progress", state.path);
        }
    }

    try {
        auto rs = co_await ss::when_all(fs.begin(), fs.end());
        for (size_t i = 0; i < rs.size(); ++i) {
            auto& r = rs[i];
            if (r.failed()) {
                _current_error = r.get_exception();
                vlog(
                  _ctxlog.warn,
                  "Failed to hydrate {} {}: {}",
                  _states[i].path_kind,
                  _states[i].path.native(),
                  _current_error);
            }
        }
    } catch (...) {
        _current_error = std::current_exception();
    }
}

ss::future<bool> hydration_loop_state::materialize() {
    if (_current_error) {
        co_return false;
    }

    std::vector<ss::future<bool>> fs;
    fs.reserve(_states.size());
    for (const auto& state : _states) {
        fs.push_back(state.materialize_action());
    }

    auto rs = co_await ss::when_all(fs.begin(), fs.end());
    bool accum = true;
    for (size_t i = 0; i < rs.size(); ++i) {
        auto& r = rs[i];
        if (r.failed()) {
            vlog(
              _ctxlog.warn,
              "Failed to materialize {} {}: {}",
              _states[i].path_kind,
              _states[i].path,
              r.get_exception());
            accum &= false;
        } else {
            accum &= r.get();
        }
    }

    co_return accum;
}

std::exception_ptr hydration_loop_state::current_error() {
    return _current_error;
}

std::pair<size_t, bool> remote_segment::min_cache_cost() const {
    if (is_legacy_mode_engaged()) {
        return std::make_pair(_size, false);
    } else {
        return std::make_pair(_chunk_size, true);
    }
}

} // namespace cloud_storage
