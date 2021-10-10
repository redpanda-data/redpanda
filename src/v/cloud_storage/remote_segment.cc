/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/remote_segment.h"

#include "cloud_storage/logger.h"
#include "cloud_storage/types.h"
#include "model/fundamental.h"
#include "storage/parser.h"
#include "utils/gate_guard.h"
#include "utils/retry_chain_node.h"
#include "utils/stream_utils.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/circular_buffer.hh>
#include <seastar/core/io_priority_class.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/queue.hh>
#include <seastar/core/temporary_buffer.hh>

#include <exception>

namespace cloud_storage {

static constexpr size_t max_consume_size = 128_KiB;

download_exception::download_exception(
  download_result r, std::filesystem::path p)
  : result(r)
  , path(std::move(p)) {
    vassert(
      r != download_result::success,
      "Exception created with successful error code");
}

const char* download_exception::what() const noexcept {
    switch (result) {
    case download_result::failed:
        return "Failed";
    case download_result::notfound:
        return "NotFound";
    case download_result::timedout:
        return "TimedOut";
    case download_result::success:
        return "Success";
    }
    __builtin_unreachable();
}

inline ss::sstring manifest_key_to_string(const manifest::key& name) {
    ss::sstring result;
    try {
        if (std::holds_alternative<segment_name>(name)) {
            result = std::get<segment_name>(name)();
        } else if (std::holds_alternative<remote_segment_path>(name)) {
            auto tmp = std::get<remote_segment_path>(name)();
            result = tmp.string();
        }
    } catch (const std::bad_variant_access& e) {
        vlog(cst_log.error, "Can't decode manifest key");
        result = "N/A";
    }
    return result;
}

remote_segment::remote_segment(
  remote& r,
  cache& c,
  s3::bucket_name bucket,
  const manifest& m,
  manifest::key path,
  retry_chain_node& parent)
  : _api(r)
  , _cache(c)
  , _bucket(std::move(bucket))
  , _manifest(m)
  , _path(std::move(path))
  , _rtc(&parent)
  , _ctxlog(cst_log, _rtc, get_ntp().path()) {}

const model::ntp& remote_segment::get_ntp() const {
    return _manifest.get_ntp();
}

const model::offset remote_segment::get_max_offset() const {
    return _manifest.get(_path)->committed_offset;
}

const model::offset remote_segment::get_base_offset() const {
    return _manifest.get(_path)->base_offset;
}

const model::term_id remote_segment::get_term() const {
    std::filesystem::path p;
    if (std::holds_alternative<segment_name>(_path)) {
        p = std::get<segment_name>(_path)();
    } else {
        p = std::get<remote_segment_path>(_path)();
    }
    auto [_, term, success] = parse_segment_name(p);
    vassert(success, "Can't parse segment name");
    return term;
}

ss::future<> remote_segment::stop() {
    vlog(_ctxlog.debug, "remote segment stop");
    return _gate.close();
}

gate_guard remote_segment::get_gate_guard() { return gate_guard(_gate); }

ss::future<ss::input_stream<char>>
remote_segment::data_stream(size_t pos, const ss::io_priority_class&) {
    vlog(_ctxlog.debug, "remote segment file input stream at {}", pos);
    gate_guard g(_gate);
    // Hydrate segment on disk
    auto full_path = co_await hydrate();
    // Create a file stream
    auto opt = co_await _cache.get(full_path, pos);
    if (opt) {
        co_return std::move(opt->body);
    }
    throw remote_segment_exception("Segment already evicted");
}

// TODO: retun cache element lease
ss::future<std::filesystem::path> remote_segment::hydrate() {
    gate_guard g(_gate);
    auto full_path = _manifest.get_remote_segment_path(_path);
    if (co_await _cache.is_cached(full_path)) {
        vlog(_ctxlog.debug, "{} is in the cache already", full_path);
        co_return full_path;
    }
    vlog(_ctxlog.debug, "Hydrating a segment {}", full_path);
    // TODO: acquire lease on cache item
    auto callback = [this, full_path](
                      uint64_t size_bytes,
                      ss::input_stream<char> s) -> ss::future<uint64_t> {
        co_await _cache.put(full_path, s).finally([&s] { return s.close(); });
        co_return size_bytes;
    };
    retry_chain_node local_rtc(100s, 250ms, &_rtc);
    auto res = co_await _api.download_segment(
      _bucket, _path, _manifest, callback, local_rtc);
    if (res != download_result::success) {
        throw download_exception(res, full_path);
    }
    co_return full_path;
}

class remote_segment_batch_consumer : public storage::batch_consumer {
public:
    using consume_result = storage::batch_consumer::consume_result;
    using stop_parser = storage::batch_consumer::stop_parser;

    remote_segment_batch_consumer(
      storage::log_reader_config& conf,
      remote_segment_batch_reader& parent,
      model::term_id term)
      : _config(conf)
      , _parent(parent)
      , _term(term) {}

    consume_result accept_batch_start(
      const model::record_batch_header& header) const override {
        vlog(
          cst_log.debug,
          "remote_segment_batch_consumer::accept_batch_start {}",
          header);
        if (header.base_offset() > _config.max_offset) {
            vlog(
              cst_log.debug,
              "remote_segment_batch_consumer::accept_batch_start stop parser "
              "because "
              "{} > {}",
              header.base_offset(),
              _config.max_offset);
            return batch_consumer::consume_result::stop_parser;
        }

        // The segment can be scanned from the begining so we should skip
        // irrelevant batches.
        if (unlikely(header.last_offset() < _config.start_offset)) {
            vlog(
              cst_log.debug,
              "remote_segment_batch_consumer::accept_batch_start skip becuse "
              "{} < {}",
              header.last_offset(),
              _config.start_offset);
            return batch_consumer::consume_result::skip_batch;
        }

        if (
          (_config.strict_max_bytes || _config.bytes_consumed)
          && (_config.bytes_consumed + header.size_bytes) > _config.max_bytes) {
            vlog(
              cst_log.debug,
              "remote_segment_batch_consumer::accept_batch_start stop because "
              "overbudget");
            // signal to log reader to stop (see log_reader::is_done)
            _config.over_budget = true;
            return batch_consumer::consume_result::stop_parser;
        }

        if (_config.type_filter && _config.type_filter != header.type) {
            vlog(
              cst_log.debug,
              "remote_segment_batch_consumer::accept_batch_start skip because "
              "of filter");
            _config.start_offset = header.last_offset() + model::offset(1);
            return batch_consumer::consume_result::skip_batch;
        }

        if (_config.first_timestamp > header.first_timestamp) {
            // kakfa needs to guarantee that the returned record is >=
            // first_timestamp
            vlog(
              cst_log.debug,
              "remote_segment_batch_consumer::accept_batch_start skip because "
              "of timestamp");
            _config.start_offset = header.last_offset() + model::offset(1);
            return batch_consumer::consume_result::skip_batch;
        }
        // we want to consume the batch
        return batch_consumer::consume_result::accept_batch;
    }

    /**
     * unconditionally consumes batch start
     */
    void consume_batch_start(
      model::record_batch_header header,
      size_t /*physical_base_offset*/,
      size_t /*size_on_disk*/) override {
        _header = header;
        _header.ctx.term = _term;
    }

    /**
     * unconditionally skip batch
     */
    void skip_batch_start(
      model::record_batch_header,
      size_t /*physical_base_offset*/,
      size_t /*size_on_disk*/) override {
        // TODO: use this to check invariant
    }

    void consume_records(iobuf&& ib) override { _records = std::move(ib); }

    stop_parser consume_batch_end() override {
        // Note: This is what keeps the train moving. the `_reader.*`
        // transitively updates the next batch to consume
        auto batch = model::record_batch{
          _header, std::move(_records), model::record_batch::tag_ctor_ng{}};
        _config.start_offset = batch.last_offset() + model::offset(1);
        _config.bytes_consumed += batch.size_bytes();
        size_t sz = _parent.produce(std::move(batch));

        if (_config.over_budget) {
            return stop_parser::yes;
        }

        if (sz > max_consume_size) {
            return stop_parser::yes;
        }

        return stop_parser::no;
    }

    void print(std::ostream& o) const override {
        o << "remote_segment_batch_consumer";
    }

private:
    storage::log_reader_config& _config;
    remote_segment_batch_reader& _parent;
    model::record_batch_header _header;
    iobuf _records;
    model::term_id _term;
};

remote_segment_batch_reader::remote_segment_batch_reader(
  remote_segment& s,
  storage::log_reader_config& config,
  model::term_id term) noexcept
  : _seg(s)
  //   , _guard(_seg.get_gate_guard())
  , _config(config)
  , _term(term) {}

ss::future<result<ss::circular_buffer<model::record_batch>>>
remote_segment_batch_reader::read_some(
  model::timeout_clock::time_point deadline) {
    vlog(
      cst_log.debug,
      "remote_segment_batch_reader::read_some(1) - done={}, ringbuf size={}",
      _done,
      _ringbuf.size());
    if (_done) {
        co_return storage::parser_errc::end_of_stream;
    }
    if (_ringbuf.empty()) {
        if (!_parser && !_done) {
            _parser = co_await init_parser();
        }
        auto bytes_consumed = co_await _parser->consume();
        if (!bytes_consumed) {
            co_return bytes_consumed.error();
        }
        if (bytes_consumed.value() == 0) {
            _done = true;
        }
    }
    vlog(
      cst_log.debug,
      "remote_segment_batch_reader::read_some(2) - done={}, ringbuf size={}",
      _done,
      _ringbuf.size());
    _total_size = 0;
    co_return std::move(_ringbuf);
}

ss::future<std::unique_ptr<storage::continuous_batch_parser>>
remote_segment_batch_reader::init_parser() {
    vlog(cst_log.debug, "remote_segment_batch_reader::init_parser");
    auto stream = co_await _seg.data_stream(0, ss::default_priority_class());
    auto parser = std::make_unique<storage::continuous_batch_parser>(
      std::make_unique<remote_segment_batch_consumer>(_config, *this, _term),
      std::move(stream));
    co_return parser;
}

size_t remote_segment_batch_reader::produce(model::record_batch batch) {
    vlog(cst_log.debug, "remote_segment_batch_reader::produce");
    _total_size += batch.size_bytes();
    _ringbuf.push_back(std::move(batch));
    return _total_size;
}

ss::future<> remote_segment_batch_reader::close() {
    vlog(cst_log.debug, "remote_segment_batch_reader::close");
    if (_parser) {
        vlog(
          cst_log.debug, "remote_segment_batch_reader::close - parser-close");
        return _parser->close();
    }
    // _guard = std::nullopt;
    return ss::now();
}

} // namespace cloud_storage
