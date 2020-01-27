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
  size_t physical_base_offset,
  size_t bytes_on_disk) {
    if (header.last_offset() < _reader._config.start_offset) {
        return skip_batch::yes;
    }
    if (header.base_offset() > _reader._config.max_offset) {
        return stop_parser::yes;
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
    _header = header;
    _num_records = num_records;
    _header.ctx.term = _reader._seg.reader()->term();
    return skip_batch::no;
}

batch_consumer::consume_result skipping_consumer::consume_record(
  size_t size_bytes,
  model::record_attributes attributes,
  int32_t timestamp_delta,
  int32_t offset_delta,
  iobuf&& key,
  iobuf&& value_and_headers) {
    std::get<model::record_batch::uncompressed_records>(_records).emplace_back(
      size_bytes,
      attributes,
      timestamp_delta,
      offset_delta,
      std::move(key),
      std::move(value_and_headers));
    return skip_batch::no;
}

void skipping_consumer::consume_compressed_records(iobuf&& records) {
    _records = model::record_batch::compressed_records(
      _num_records, std::move(records));
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
    if (
      _reader._config.bytes_consumed >= _reader._config.max_bytes
      || model::timeout_clock::now() >= _timeout) {
        return stop_parser::yes;
    }
    _header = {};
    _num_records = 0;
    return stop_parser(_reader.is_buffer_full());
}

log_segment_batch_reader::log_segment_batch_reader(
  segment& seg,
  log_reader_config& config,
  std::vector<model::record_batch>& recs) noexcept
  : _seg(seg)
  , _config(config)
  , _buffer(recs) {
    std::sort(std::begin(_config.type_filter), std::end(_config.type_filter));
}

std::unique_ptr<continuous_batch_parser>
log_segment_batch_reader::initialize(model::timeout_clock::time_point timeout) {
    auto input = _seg.offset_data_stream(_config.start_offset, _config.prio);
    return std::make_unique<continuous_batch_parser>(
      std::make_unique<skipping_consumer>(*this, timeout), std::move(input));
}

bool log_segment_batch_reader::is_buffer_full() const {
    return _buffer_size >= max_buffer_size;
}

ss::future<size_t>
log_segment_batch_reader::read(model::timeout_clock::time_point timeout) {
    _buffer_size = 0;
    _buffer.clear();
    auto parser = initialize(timeout);
    auto ptr = parser.get();
    return parser->consume().finally([this, p = std::move(parser)] {});
}

log_reader::log_reader(
  log_set& seg_set, log_reader_config config, probe& probe) noexcept
  : _set(seg_set)
  , _config(std::move(config))
  , _probe(probe) {
    _end_of_stream = seg_set.empty();
}

static inline segment* find_in_set(log_set& s, model::offset o) {
    vassert(o() >= 0, "cannot find negative logical offsets");
    segment* ret = nullptr;
    if (auto it = s.lower_bound(o); it != s.end()) {
        ret = &(*it);
    }
    return ret;
}

ss::future<log_reader::span>
log_reader::do_load_slice(model::timeout_clock::time_point timeout) {
    if (is_done()) {
        _end_of_stream = true;
        return ss::make_ready_future<span>();
    }
    if (_last_base == _config.start_offset) {
        _end_of_stream = true;
        return ss::make_ready_future<span>();
    }
    _last_base = _config.start_offset;
    segment* seg = find_in_set(_set, _config.start_offset);
    if (!seg) {
        _end_of_stream = true;
        return ss::make_ready_future<span>();
    }
    auto segc = std::make_unique<log_segment_batch_reader>(
      *seg, _config, _batches);
    auto ptr = segc.get();
    return ptr->read(timeout)
      .handle_exception([this](std::exception_ptr e) {
          _probe.batch_parse_error();
          return ss::make_exception_future<size_t>(e);
      })
      .then([this, ptr](size_t bytes_consumed) {
          _probe.add_bytes_read(bytes_consumed);
          if (_batches.empty()) {
              return span();
          }
          _probe.add_batches_read(int32_t(_batches.size()));
          return span{&_batches[0], int32_t(_batches.size())};
      })
      .finally([sc = std::move(segc)] {});
}

static inline bool is_finished_offset(log_set& s, model::offset o) {
    if (s.empty()) {
        return true;
    }
    for (auto it = s.rbegin(); it != s.rend(); it++) {
        if (!it->empty()) {
            return o > it->committed_offset();
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
