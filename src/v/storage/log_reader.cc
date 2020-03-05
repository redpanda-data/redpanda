#include "storage/log_reader.h"

#include "bytes/iobuf.h"
#include "model/record.h"
#include "storage/logger.h"
#include "vassert.h"
#include "vlog.h"

#include <seastar/core/circular_buffer.hh>

#include <fmt/ostream.h>

namespace storage {
static constexpr size_t max_segment_size = static_cast<size_t>(
  std::numeric_limits<uint32_t>::max());

batch_consumer::consume_result skipping_consumer::consume_batch_start(
  model::record_batch_header header,
  size_t physical_base_offset,
  size_t size_on_disk) {
    const auto filesize = _reader._seg.reader()->file_size();
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
    if (unlikely(
          header.size_bytes != size_on_disk
          || size_on_disk > max_segment_size)) {
        vlog(
          stlog.info,
          "Invalid record size:{} > max_segment_size:{}. Possible corruption",
          size_on_disk,
          max_segment_size);
        return stop_parser::yes;
    }
    if (unlikely((header.size_bytes + physical_base_offset) > filesize)) {
        vlog(
          stlog.info,
          "offset + batch_size:{} exceeds filesize:{}. Possible corruption",
          (header.size_bytes + physical_base_offset),
          filesize);
        return stop_parser::yes;
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
    _header.ctx.term = _reader._seg.reader()->term();
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
    _reader._buffer.emplace_back(
      model::record_batch(_header, std::exchange(_records, {})));
    _reader._config.start_offset = _header.last_offset() + model::offset(1);
    _reader._config.bytes_consumed += _header.size_bytes;
    _reader._buffer_size += _header.size_bytes;
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
    return stop_parser(_reader.is_buffer_full());
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

bool log_segment_batch_reader::is_buffer_full() const {
    return _buffer_size >= max_buffer_size;
}

ss::future<ss::circular_buffer<model::record_batch>>
log_segment_batch_reader::read(model::timeout_clock::time_point timeout) {
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
        return ss::make_ready_future<ss::circular_buffer<model::record_batch>>(
          std::move(cache_read.batches));
    }
    auto parser = initialize(timeout, cache_read.next_cached_batch);
    auto ptr = parser.get();
    return ptr->consume()
      .then([this](size_t bytes_consumed) {
          _probe.add_bytes_read(bytes_consumed);
          // insert batches from disk into the cache
          for (auto& b : _buffer) {
              _seg.cache_put(b.share());
          }
          return std::move(_buffer);
      })
      .finally([this, p = std::move(parser)] {});
}

log_reader::log_reader(
  log_set& seg_set, log_reader_config config, probe& probe) noexcept
  : _set(seg_set)
  , _config(config)
  , _probe(probe) {
    _end_of_stream = seg_set.empty();
}

static inline segment* find_in_set(log_set& s, model::offset o) {
    vassert(o() >= 0, "cannot find negative logical offsets");
    segment* ret = nullptr;
    if (auto it = s.lower_bound(o); it != s.end()) {
        ret = it->get();
    }
    return ret;
}

ss::future<ss::circular_buffer<model::record_batch>>
log_reader::do_load_slice(model::timeout_clock::time_point timeout) {
    if (is_done()) {
        _end_of_stream = true;
        return ss::make_ready_future<
          ss::circular_buffer<model::record_batch>>();
    }
    if (_last_base == _config.start_offset) {
        _end_of_stream = true;
        return ss::make_ready_future<
          ss::circular_buffer<model::record_batch>>();
    }
    _last_base = _config.start_offset;
    segment* seg = find_in_set(_set, _config.start_offset);
    if (!seg) {
        _end_of_stream = true;
        return ss::make_ready_future<
          ss::circular_buffer<model::record_batch>>();
    }
    auto segc = std::make_unique<log_segment_batch_reader>(
      *seg, _config, _probe);
    auto ptr = segc.get();
    return ptr->read(timeout)
      .handle_exception([this](std::exception_ptr e) {
          _probe.batch_parse_error();
          return ss::make_exception_future<
            ss::circular_buffer<model::record_batch>>(e);
      })
      .then([this, ptr](ss::circular_buffer<model::record_batch> recs) {
          if (recs.empty()) {
              return ss::make_ready_future<
                ss::circular_buffer<model::record_batch>>();
          }
          _probe.add_batches_read(recs.size());
          return ss::make_ready_future<
            ss::circular_buffer<model::record_batch>>(std::move(recs));
      })
      .finally([sc = std::move(segc)] {});
}

static inline bool is_finished_offset(log_set& s, model::offset o) {
    if (s.empty()) {
        return true;
    }
    for (auto it = s.rbegin(); it != s.rend(); it++) {
        if (!(*it)->empty()) {
            return o > (*it)->committed_offset();
        }
    }
    return true;
}
bool log_reader::is_done() {
    return _end_of_stream || is_finished_offset(_set, _config.start_offset)
           || _config.start_offset > _config.max_offset
           || _config.bytes_consumed > _config.max_bytes;
}

} // namespace storage
