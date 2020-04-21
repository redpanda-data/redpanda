#include "storage/log_reader.h"

#include "bytes/iobuf.h"
#include "model/record.h"
#include "storage/logger.h"
#include "vassert.h"
#include "vlog.h"

#include <seastar/core/circular_buffer.hh>

#include <fmt/ostream.h>

namespace storage {
using records_t = ss::circular_buffer<model::record_batch>;

batch_consumer::consume_result skipping_consumer::consume_batch_start(
  model::record_batch_header header,
  size_t /*physical_base_offset*/,
  size_t /*size_on_disk*/) {
    // check for holes in the offset range on disk
    if (unlikely(
          _expected_next_batch() >= 0
          && header.base_offset != _expected_next_batch)) {
        throw std::runtime_error(fmt::format(
          "hole encountered reading from disk log: "
          "expected batch offset {} (actual {})",
          _expected_next_batch,
          header.base_offset()));
    }
    _expected_next_batch = header.last_offset() + model::offset(1);

    if (header.last_offset() < _reader._config.start_offset) {
        return skip_batch::yes;
    }
    if (header.base_offset() > _reader._config.max_offset) {
        return stop_parser::yes;
    }
    if (
      _reader._config.type_filter
      && _reader._config.type_filter != header.type) {
        _reader._config.start_offset = header.last_offset() + model::offset(1);
        return skip_batch::yes;
    }
    if (
      _reader._config.first_timestamp
      && _reader._config.first_timestamp < header.first_timestamp) {
        // kakfa needs to guarantee that the returned record is >=
        // first_timestamp
        _reader._config.start_offset = header.last_offset() + model::offset(1);
        return skip_batch::yes;
    }
    if (header.attrs.compression() == model::compression::none) {
        // Reset the variant.
        auto r = model::record_batch::uncompressed_records();
        r.reserve(header.record_count);
        _records = std::move(r);
    }
    _header = header;
    _header.ctx.term = _reader._seg.reader().term();
    return skip_batch::no;
}

batch_consumer::consume_result
skipping_consumer::consume_record(model::record r) {
    std::get<model::record_batch::uncompressed_records>(_records).emplace_back(
      std::move(r));
    return skip_batch::no;
}

void skipping_consumer::consume_compressed_records(iobuf&& records) {
    _records = std::move(records);
}

batch_consumer::stop_parser skipping_consumer::consume_batch_end() {
    // Note: This is what keeps the train moving. the `_reader.*` transitively
    // updates the next batch to consume
    _reader.add_one(model::record_batch(_header, std::exchange(_records, {})));
    // We keep the batch in the buffer so that the reader can be cached.
    if (
      _header.last_offset() >= _reader._seg.committed_offset()
      || _header.last_offset() >= _reader._config.max_offset) {
        return stop_parser::yes;
    }
    /*
     * if the very next batch is known to be cached, then stop parsing. the next
     * read will with high probability experience a cache hit.
     */
    if (_next_cached_batch == (_header.last_offset() + model::offset(1))) {
        return stop_parser::yes;
    }
    if (
      _reader._config.bytes_consumed >= _reader._config.max_bytes
      || model::timeout_clock::now() >= _timeout) {
        return stop_parser::yes;
    }
    _header = {};
    return stop_parser(_reader._state.is_full());
}

log_segment_batch_reader::log_segment_batch_reader(
  segment& seg, log_reader_config& config, probe& p) noexcept
  : _seg(seg)
  , _config(config)
  , _probe(p) {}

std::unique_ptr<continuous_batch_parser> log_segment_batch_reader::initialize(
  model::timeout_clock::time_point timeout,
  std::optional<model::offset> next_cached_batch) {
    auto input = _seg.offset_data_stream(_config.start_offset, _config.prio);
    return std::make_unique<continuous_batch_parser>(
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
    _config.bytes_consumed += b.header().size_bytes;
    _state.buffer_size += b.header().size_bytes;
    _seg.cache_put(b);
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
      max_buffer_size);

    // handles cases where the type filter skipped batches. see
    // batch_cache_index::read for more details.
    _config.start_offset = cache_read.next_batch;

    if (
      !cache_read.batches.empty()
      || _config.start_offset > _config.max_offset) {
        _config.bytes_consumed += cache_read.memory_usage;
        _probe.add_bytes_read(cache_read.memory_usage);
        return ss::make_ready_future<result<records_t>>(
          std::move(cache_read.batches));
    }
    if (!_iterator) {
        _iterator = initialize(timeout, cache_read.next_cached_batch);
    }
    auto ptr = _iterator.get();
    return ptr->consume().then(
      [this](result<size_t> bytes_consumed) -> result<records_t> {
          if (!bytes_consumed) {
              return bytes_consumed.error();
          }
          _probe.add_bytes_read(bytes_consumed.value());
          auto tmp = std::exchange(_state, {});
          return result<records_t>(std::move(tmp.buffer));
      });
}

log_reader::log_reader(
  std::unique_ptr<lock_manager::lease> l,
  log_reader_config config,
  probe& probe) noexcept
  : _lease(std::move(l))
  , _iterator(_lease->range.begin())
  , _config(config)
  , _probe(probe) {
    if (_iterator.next_seg != _lease->range.end()) {
        _iterator.reader = std::make_unique<log_segment_batch_reader>(
          **_iterator.next_seg, _config, _probe);
    }
}

ss::future<> log_reader::next_iterator() {
    if (_config.start_offset <= (**_iterator.next_seg).committed_offset()) {
        return ss::make_ready_future<>();
    }
    std::unique_ptr<log_segment_batch_reader> tmp_reader = nullptr;
    while (_config.start_offset > (**_iterator.next_seg).committed_offset()
           && !is_end_of_stream()) {
        _iterator.next_seg++;
        if (!tmp_reader) {
            tmp_reader = std::move(_iterator.reader);
        }
    }
    if (_iterator.next_seg != _lease->range.end()) {
        _iterator.reader = std::make_unique<log_segment_batch_reader>(
          **_iterator.next_seg, _config, _probe);
    }
    if (tmp_reader) {
        auto raw = tmp_reader.get();
        return raw->close().finally([r = std::move(tmp_reader)] {});
    }
    return ss::make_ready_future<>();
}

ss::future<log_reader::storage_t>
log_reader::do_load_slice(model::timeout_clock::time_point timeout) {
    if (is_done()) {
        // must keep this function because, the segment might not be done
        // but offsets might have exceeded the read
        set_end_of_stream();
        return _iterator.close().then(
          [] { return ss::make_ready_future<storage_t>(); });
    }
    if (_last_base == _config.start_offset) {
        set_end_of_stream();
        return _iterator.close().then(
          [] { return ss::make_ready_future<storage_t>(); });
    }
    _last_base = _config.start_offset;
    ss::future<> fut = next_iterator();
    if (is_end_of_stream()) {
        return fut.then([] { return ss::make_ready_future<storage_t>(); });
    }
    return fut
      .then([this, timeout] { return _iterator.reader->read_some(timeout); })
      .then([this](result<records_t> recs) -> ss::future<storage_t> {
          if (!recs) {
              set_end_of_stream();
              vlog(
                stlog.info,
                "stopped reading stream: {}",
                recs.error().message());
              return _iterator.close().then(
                [] { return ss::make_ready_future<storage_t>(); });
          }
          if (recs.value().empty()) {
              set_end_of_stream();
              return _iterator.close().then(
                [] { return ss::make_ready_future<storage_t>(); });
          }
          _probe.add_batches_read(recs.value().size());
          return ss::make_ready_future<storage_t>(std::move(recs.value()));
      })
      .handle_exception([this](std::exception_ptr e) {
          set_end_of_stream();
          _probe.batch_parse_error();
          return _iterator.close().then(
            [e] { return ss::make_exception_future<storage_t>(e); });
      });
}

static inline bool is_finished_offset(segment_set& s, model::offset o) {
    if (s.empty()) {
        return true;
    }

    for (int i = (int)s.size() - 1; i >= 0; --i) {
        auto& seg = s[i];
        if (!seg->empty()) {
            return o > seg->committed_offset();
        }
    }
    return true;
}
bool log_reader::is_done() {
    return is_end_of_stream()
           || is_finished_offset(_lease->range, _config.start_offset)
           || _config.start_offset > _config.max_offset
           || _config.bytes_consumed > _config.max_bytes;
}

} // namespace storage
