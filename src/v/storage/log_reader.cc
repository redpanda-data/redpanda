#include "storage/log_reader.h"

#include "utils/fragbuf.h"

namespace storage {

batch_consumer::skip skipping_consumer::consume_batch_start(
  model::record_batch_header header, size_t num_records) {
    if (header.last_offset() < _start_offset) {
        return skip::yes;
    }
    if (header.attrs.compression() == model::compression::none) {
        // Reset the variant.
        auto r = model::record_batch::uncompressed_records();
        r.reserve(num_records);
        _records = std::move(r);
    }
    _header = std::move(header);
    _num_records = num_records;
    return skip::no;
}

batch_consumer::skip skipping_consumer::consume_record_key(
  size_t size_bytes,
  model::record_attributes attributes,
  int32_t timestamp_delta,
  int32_t offset_delta,
  fragbuf&& key) {
    _record_size_bytes = size_bytes;
    _record_attributes = attributes;
    _record_timestamp_delta = timestamp_delta;
    _record_offset_delta = offset_delta;
    _record_key = std::move(key);
    return skip::no;
}

void skipping_consumer::consume_record_value(fragbuf&& value_and_headers) {
    std::get<model::record_batch::uncompressed_records>(_records).emplace_back(
      _record_size_bytes,
      _record_attributes,
      _record_timestamp_delta,
      _record_offset_delta,
      std::move(_record_key),
      std::move(value_and_headers));
}

void skipping_consumer::consume_compressed_records(fragbuf&& records) {
    _records = model::record_batch::compressed_records(
      _num_records, std::move(records));
}

stop_iteration skipping_consumer::consume_batch_end() {
    auto& batch = _reader._buffer.emplace_back(
      std::move(_header), std::move(_records));
    auto mem = batch.memory_usage();
    _reader._bytes_read += mem;
    _reader._buffer_size += mem;
    // We keep the batch in the buffer so that the reader can be cached.
    if (batch.base_offset() > _reader._tracker.committed_offset()) {
        _reader._end_of_stream = true;
        _reader._over_committed_offset = true;
        return stop_iteration::yes;
    }
    if (
      _reader._bytes_read >= _reader._config.max_bytes
      || model::timeout_clock::now() >= _timeout) {
        _reader._end_of_stream = true;
        return stop_iteration::yes;
    }
    return stop_iteration(_reader.is_buffer_full());
}

log_segment_reader::log_segment_reader(
  log_segment_ptr seg,
  offset_tracker& tracker,
  log_reader_config config,
  probe& probe) noexcept
  : model::record_batch_reader::impl()
  , _seg(std::move(seg))
  , _tracker(tracker)
  , _config(std::move(config))
  , _consumer(skipping_consumer(*this))
  , _probe(probe) {
}

bool log_segment_reader::is_initialized() const {
    return bool(_parser);
}

future<> log_segment_reader::initialize() {
    // FIXME: Read from offset index
    _input = _seg->data_stream(0, _config.prio);
    _parser = continuous_batch_parser(_consumer, _input);
    return make_ready_future<>();
}

bool log_segment_reader::is_buffer_full() const {
    return _buffer_size >= max_buffer_size;
}

// Called for cached readers.
void log_segment_reader::reset_state() {
    if (__builtin_expect(_over_committed_offset, false)) {
        _buffer_size = _buffer.back().memory_usage();
        if (_buffer.back().last_offset() > _tracker.committed_offset()) {
            return;
        }
        _over_committed_offset = false;
    }
    _end_of_stream = false;
}

future<log_segment_reader::span>
log_segment_reader::do_load_slice(model::timeout_clock::time_point timeout) {
    if (_end_of_stream || _over_committed_offset) {
        return make_ready_future<span>();
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
          return make_exception_future<size_t>(e);
      })
      .then([this](size_t bytes_consumed) {
          _probe.add_bytes_read(bytes_consumed);
          auto f = make_ready_future<>();
          if (_input.eof() || end_of_stream()) {
              _end_of_stream = true;
              f = _input.close();
          }
          return f.then([this] {
              if (_buffer.empty()) {
                  return span();
              }
              _probe.add_batches_read(int32_t(_buffer.size()));
              return span{&_buffer[0],
                          int32_t(_buffer.size()) - _over_committed_offset};
          });
      });
}

log_reader::log_reader(
  log_set& seg_set,
  offset_tracker& tracker,
  log_reader_config config,
  probe& probe) noexcept
  : _selector(seg_set)
  , _offset_tracker(tracker)
  , _config(std::move(config))
  , _probe(probe) {
}

future<log_reader::span>
log_reader::do_load_slice(model::timeout_clock::time_point timeout) {
    if (is_done()) {
        return make_ready_future<span>();
    }
    return _current_reader->do_load_slice(timeout);
}

bool log_reader::is_done() {
    return _end_of_stream || !maybe_create_segment_reader();
}

log_reader::reader_available log_reader::maybe_create_segment_reader() {
    if (_current_reader && !_current_reader->end_of_stream()) {
        return reader_available::yes;
    }
    log_segment_ptr seg;
    if (_current_reader) {
        auto bytes_read = _current_reader->bytes_read();
        if (
          bytes_read >= _config.max_bytes
          || _current_reader->_over_committed_offset) {
            _end_of_stream = true;
            return reader_available::no;
        }
        _config.max_bytes -= bytes_read;
        _config.min_bytes -= std::min(bytes_read, _config.min_bytes);
        seg = _selector.select(_current_reader->_seg->max_offset());
    } else {
        seg = _selector.select(_config.start_offset);
    }
    if (!seg) {
        _end_of_stream = true;
        return reader_available::no;
    }
    _current_reader.emplace(std::move(seg), _offset_tracker, _config, _probe);
    return reader_available::yes;
}

} // namespace storage
