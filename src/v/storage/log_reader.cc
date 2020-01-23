#include "storage/log_reader.h"

#include "bytes/iobuf.h"
#include "storage/logger.h"
#include "vassert.h"

#include <fmt/ostream.h>

namespace storage {

bool skipping_consumer::skip_batch_type(model::record_batch_type type) {
    auto& type_filter = _reader._config.type_filter;
    if (type_filter.empty()) {
        return false;
    }
    auto it = std::find(std::cbegin(type_filter), std::cend(type_filter), type);
    return it == std::cend(type_filter);
}

batch_consumer::consume_result skipping_consumer::consume_batch_start(
  model::record_batch_header header,
  size_t num_records,
  size_t /*physical_base_offset*/,
  size_t /*bytes_on_disk*/) {
    if (header.last_offset() < _start_offset) {
        return skip_batch::yes;
    }
    if (header.base_offset() > _reader._config.max_offset) {
        return skip_batch::yes;
    }
    if (skip_batch_type(header.type)) {
        return skip_batch::yes;
    }
    if (header.attrs.compression() == model::compression::none) {
        // Reset the variant.
        auto r = model::record_batch::uncompressed_records();
        r.reserve(num_records);
        _records = std::move(r);
    }
    _header = std::move(header);
    _num_records = num_records;
    return skip_batch::no;
}

batch_consumer::consume_result skipping_consumer::consume_record_key(
  size_t size_bytes,
  model::record_attributes attributes,
  int32_t timestamp_delta,
  int32_t offset_delta,
  iobuf&& key) {
    _record_size_bytes = size_bytes;
    _record_attributes = attributes;
    _record_timestamp_delta = timestamp_delta;
    _record_offset_delta = offset_delta;
    _record_key = std::move(key);
    return skip_batch::no;
}

void skipping_consumer::consume_record_value(iobuf&& value_and_headers) {
    std::get<model::record_batch::uncompressed_records>(_records).emplace_back(
      _record_size_bytes,
      _record_attributes,
      _record_timestamp_delta,
      _record_offset_delta,
      std::move(_record_key),
      std::move(value_and_headers));
}

void skipping_consumer::consume_compressed_records(iobuf&& records) {
    _records = model::record_batch::compressed_records(
      _num_records, std::move(records));
}

batch_consumer::stop_parser skipping_consumer::consume_batch_end() {
    auto& batch = _reader._buffer.emplace_back(
      std::move(_header), std::move(_records));
    auto mem = batch.memory_usage();
    _reader._bytes_read += mem;
    _reader._buffer_size += mem;
    // We keep the batch in the buffer so that the reader can be cached.
    if (
      batch.base_offset() > _reader._seg.committed_offset()
      || batch.last_offset() > _reader._config.max_offset) {
        _reader._end_of_stream = true;
        return stop_parser::yes;
    }
    if (
      _reader._bytes_read >= _reader._config.max_bytes
      || model::timeout_clock::now() >= _timeout) {
        _reader._end_of_stream = true;
        return stop_parser::yes;
    }
    return stop_parser(_reader.is_buffer_full());
}

log_segment_batch_reader::log_segment_batch_reader(
  segment& seg,
  model::offset base,
  log_reader_config config,
  probe& probe) noexcept
  : model::record_batch_reader::impl()
  , _seg(seg)
  , _base(base)
  , _config(std::move(config))
  , _consumer(skipping_consumer(*this, config.start_offset))
  , _probe(probe) {
    std::sort(std::begin(_config.type_filter), std::end(_config.type_filter));
}

bool log_segment_batch_reader::is_initialized() const { return bool(_parser); }

ss::future<> log_segment_batch_reader::initialize() {
    _input = _seg.offset_data_stream(_base, _config.prio);
    _parser = continuous_batch_parser(_consumer, _input);
    return ss::make_ready_future<>();
}

bool log_segment_batch_reader::is_buffer_full() const {
    return _buffer_size >= max_buffer_size;
}

ss::future<log_segment_batch_reader::span>
log_segment_batch_reader::do_load_slice(
  model::timeout_clock::time_point timeout) {
    if (_end_of_stream) {
        return ss::make_ready_future<span>();
    }
    if (!is_initialized()) {
        return initialize().then(
          [this, timeout] { return do_load_slice(timeout); });
    }
    _buffer_size = 0;
    _buffer.clear();
    _consumer.set_timeout(timeout);
    return _parser->consume()
      .handle_exception([this](std::exception_ptr e) {
          _probe.batch_parse_error();
          return ss::make_exception_future<size_t>(e);
      })
      .then([this](size_t bytes_consumed) {
          _probe.add_bytes_read(bytes_consumed);
          auto f = ss::make_ready_future<>();
          if (_input.eof() || end_of_stream()) {
              _end_of_stream = true;
              f = _input.close();
          }
          return f.then([this] {
              if (_buffer.empty()) {
                  return span();
              }
              _probe.add_batches_read(int32_t(_buffer.size()));
              return span{&_buffer[0], int32_t(_buffer.size())};
          });
      });
}

log_reader::log_reader(
  log_set& seg_set, log_reader_config config, probe& probe) noexcept
  : _set(seg_set)
  , _config(std::move(config))
  , _probe(probe) {
    _end_of_stream = seg_set.empty();
}

ss::future<log_reader::span>
log_reader::do_load_slice(model::timeout_clock::time_point timeout) {
    if (is_done()) {
        return ss::make_ready_future<span>();
    }
    return _current_reader->do_load_slice(timeout);
}

bool log_reader::is_done() {
    return _end_of_stream || !maybe_create_segment_reader();
}

static inline segment* find_in_set(log_set& s, model::offset o) {
    vassert(o() >= 0, "cannot find negative logical offsets");
    segment* ret = nullptr;
    if (auto it = s.lower_bound(o); it != s.end()) {
        ret = &(*it);
    }
    return ret;
}

static inline bool is_finished_offset(log_set& s, model::offset o) {
    if (s.empty()) {
        return true;
    }
    for (auto it = s.rbegin(); it != s.rend(); it++) {
        if (!it->empty()) {
            stlog.info("offset: {}, dirty:{}", o, it->dirty_offset());
            return o > it->dirty_offset();
        }
    }
    return true;
}

log_reader::reader_available log_reader::maybe_create_segment_reader() {
    if (_current_reader && !_current_reader->end_of_stream()) {
        return reader_available::yes;
    }
    model::offset base = _config.start_offset;
    if (_current_reader) {
        auto bytes_read = _current_reader->bytes_read();
        // The max offset is inclusive, we have to select next segment
        base = _current_reader->_seg.committed_offset() + model::offset(1);
        if (
          bytes_read >= _config.max_bytes || base > _config.max_offset
          || is_finished_offset(_set, base)) {
            _end_of_stream = true;
            return reader_available::no;
        }
        _config.max_bytes -= bytes_read;
        _config.min_bytes -= std::min(bytes_read, _config.min_bytes);
    }
    segment* seg = find_in_set(_set, base);
    if (!seg) {
        _end_of_stream = true;
        return reader_available::no;
    }
    _current_reader.emplace(*seg, base, _config, _probe);
    return reader_available::yes;
}

} // namespace storage
