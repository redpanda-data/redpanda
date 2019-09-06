#include "storage/parser.h"

#include "model/fundamental.h"
#include "storage/constants.h"

#include <seastar/core/byteorder.hh>

#include <algorithm>
#include <cstdint>
#include <stdexcept>

namespace storage {

future<consumption_result<char>> continuous_batch_parser::
operator()(temporary_buffer<char> data) {
    auto orig_data_size = data.size();
    auto result = process(data);
    return seastar::visit(
      result,
      [this, orig_data_size, &data](stop_iteration stop) {
          if (stop) {
              return make_ready_future<consumption_result<char>>(
                stop_consuming<char>(std::move(data)));
          }
          if (!orig_data_size) {
              // End of file
              ensure_valid_end_state();
              return make_ready_future<consumption_result<char>>(
                stop_consuming<char>(std::move(data)));
          }
          return make_ready_future<consumption_result<char>>(
            continue_consuming{});
      },
      [this, &data](skip_bytes skip) {
          auto n = skip.get_value();
          auto skip_buf = std::min(n, data.size());
          data.trim_front(skip_buf);
          n -= skip_buf;
          if (!n) {
              return make_ready_future<consumption_result<char>>(
                stop_consuming<char>(std::move(data)));
          }
          return make_ready_future<consumption_result<char>>(skip_bytes(n));
      });
}

parse_result continuous_batch_parser::process(temporary_buffer<char>& data) {
    while (data || non_consuming()) {
        process_sliced_data(data);
        // If _prestate is set to something other than prestate::none
        // after process_buffer was called, it means that data wasn't
        // enough to complete the prestate.
        if (__builtin_expect(_prestate != prestate::none, false)) {
            if (data.size()) {
                throw std::logic_error(
                  "Expected all data from the buffer to have been consumed.");
            }
            return stop_iteration::no;
        }
        auto ret = do_process(data);
        if (__builtin_expect(ret != stop_iteration::no, false)) {
            return ret;
        }
    }
    return stop_iteration::no;
}

parse_result continuous_batch_parser::do_process(temporary_buffer<char>& data) {
    switch (_state) {
    case state::batch_start: {
        if (read_vint(data) != read_status::ready) {
            _state = state::base_offset;
            break;
        }
    }
    case state::base_offset: {
        _header.size_bytes = _64;
        if (read_int<int64_t>(data) != read_status::ready) {
            _state = state::crc;
            break;
        }
    }
    case state::crc: {
        _header.base_offset = model::offset(_64);
        if (read_int<int32_t>(data) != read_status::ready) {
            _state = state::attributes;
            break;
        }
    }
    case state::attributes: {
        _header.crc = _32;
        if (read_int<int16_t>(data) != read_status::ready) {
            _state = state::last_offset_delta;
            break;
        }
    }
    case state::last_offset_delta: {
        _header.attrs = model::record_batch_attributes(_16);
        if (read_int<int32_t>(data) != read_status::ready) {
            _state = state::first_timestamp;
            break;
        }
    }
    case state::first_timestamp: {
        _header.last_offset_delta = _32;
        if (read_int<int64_t>(data) != read_status::ready) {
            _state = state::max_timestamp;
            break;
        }
    }
    case state::max_timestamp: {
        _header.first_timestamp = model::timestamp(_64);
        if (read_int<int64_t>(data) != read_status::ready) {
            _state = state::record_count;
            break;
        }
    }
    case state::record_count: {
        _header.max_timestamp = model::timestamp(_64);
        if (read_int<int32_t>(data) != read_status::ready) {
            _state = state::header_done;
            break;
        }
    }
    case state::header_done: {
        _num_records = _32;
        auto remaining_batch_bytes = _header.size_bytes - packed_header_size;
        _compressed_batch = _header.attrs.compression()
                            != model::compression::none;
        auto should_skip = _consumer->consume_batch_start(
          std::move(_header), _num_records);
        if (__builtin_expect(bool(should_skip), false)) {
            _state = state::batch_start;
            return skip_bytes(remaining_batch_bytes);
        }
        if (_compressed_batch) {
            _record_size = remaining_batch_bytes;
            _state = state::compressed_records_start;
            break;
        }
    }
    case state::record_start: {
        if (read_vint(data) != read_status::ready) {
            _state = state::timestamp_delta;
            break;
        }
    }
    case state::timestamp_delta: {
        _record_size = _64;
        _value_and_headers_size = _64;
        if (read_vint(data) != read_status::ready) {
            _state = state::offset_delta;
            break;
        }
    }
    case state::offset_delta: {
        _timestamp_delta = static_cast<int32_t>(_64);
        _value_and_headers_size -= _varint_size;
        if (read_vint(data) != read_status::ready) {
            _state = state::key_length;
            break;
        }
    }
    case state::key_length: {
        _offset_delta = static_cast<int32_t>(_64);
        _value_and_headers_size -= _varint_size;
        if (read_vint(data) != read_status::ready) {
            _state = state::key_bytes;
            break;
        }
    }
    case state::key_bytes: {
        _value_and_headers_size -= _varint_size;
        _value_and_headers_size -= _64;
        if (read_fragmented_bytes(data, _64) != read_status::ready) {
            _state = state::key_done;
            break;
        }
    }
    case state::key_done: {
        auto should_skip = _consumer->consume_record_key(
          _record_size,
          _timestamp_delta,
          _offset_delta,
          fragmented_temporary_buffer(std::exchange(_read_bytes, {}), _64));
        if (should_skip) {
            if (--_num_records) {
                _state = state::record_start;
            } else {
                _state = state::batch_end;
            }
            return skip_bytes(_value_and_headers_size);
        }
    }
    case state::value_and_headers: {
        if (
          read_fragmented_bytes(data, _value_and_headers_size)
          != read_status::ready) {
            _state = state::record_end;
            break;
        }
    }
    case state::record_end: {
        auto vhs = fragmented_temporary_buffer(
          std::exchange(_read_bytes, {}), _value_and_headers_size);
        _consumer->consume_record_value(std::move(vhs));
        if (--_num_records) {
            _state = state::record_start;
        } else {
            _state = state::batch_end;
        }
        break;
    }
    case state::compressed_records_start: {
        if (read_fragmented_bytes(data, _record_size) != read_status::ready) {
            _state = state::compressed_records_end;
            break;
        }
    }
    case state::compressed_records_end: {
        auto record = fragmented_temporary_buffer(
          std::exchange(_read_bytes, {}), _record_size);
        _consumer->consume_compressed_records(std::move(record));
    }
    case state::batch_end:
        _state = state::batch_start;
        return _consumer->consume_batch_end();
    }
    return stop_iteration::no;
}

void continuous_batch_parser::process_sliced_data(
  temporary_buffer<char>& data) {
    if (__builtin_expect(_prestate != prestate::none, false)) {
        // We're in the middle of reading a basic type, which crossed
        // an input buffer. Resume that read before continuing to
        // handle the current state:
        switch (_prestate) {
        case prestate::none: {
            __builtin_unreachable();
            break;
        }
        case prestate::reading_vint: {
            process_vint(data);
            break;
        }
        case prestate::reading_8: {
            if (process_int(data, sizeof(int8_t))) {
                _8 = _read_int.int8;
                _prestate = prestate::none;
            }
            break;
        }
        case prestate::reading_16: {
            if (process_int(data, sizeof(int16_t))) {
                _16 = be_to_cpu(_read_int.int16);
                _prestate = prestate::none;
            }
            break;
        }
        case prestate::reading_32: {
            if (process_int(data, sizeof(int32_t))) {
                _32 = be_to_cpu(_read_int.int32);
                _prestate = prestate::none;
            }
            break;
        }
        case prestate::reading_64: {
            if (process_int(data, sizeof(int64_t))) {
                _64 = be_to_cpu(_read_int.int64);
                _prestate = prestate::none;
            }
            break;
        }
        case prestate::reading_bytes: {
            if (_pos >= _ftb_size) {
                throw malformed_batch_stream_exception(
                  "Overrun the amount of bytes to read");
            }
            auto n = std::min(size_t(_ftb_size - _pos), data.size());
            _read_bytes.push_back(data.share(0, n));
            data.trim_front(n);
            _pos += n;
            if (_pos == _ftb_size) {
                _prestate = prestate::none;
            }
            break;
        }
        }
    }
}

continuous_batch_parser::read_status
continuous_batch_parser::read_vint(temporary_buffer<char>& data) {
    if (data.size() >= vint::max_length) {
        auto [val, bytes_read] = vint::deserialize(data);
        data.trim_front(bytes_read);
        _64 = val;
        _varint_size = bytes_read;
        return read_status::ready;
    }
    _pos = 0;
    _prestate = prestate::reading_vint;
    return read_status::waiting;
}

void continuous_batch_parser::process_vint(temporary_buffer<char>& data) {
    bool finished = false;
    auto it = data.begin();
    for (; it != data.end(); ++it) {
        if (finished = !vint::has_more_bytes(*it); finished) {
            ++it; // Increment so the iterator encompasses the last vint byte.
            break;
        }
    }
    std::copy(data.begin(), it, _read_int.bytes + _pos);
    auto n = std::distance(data.begin(), it);
    data.trim_front(n);
    _pos += n;
    if (finished) {
        auto range = boost::make_iterator_range(
          _read_int.bytes, _read_int.bytes + vint::max_length);
        auto [val, bytes_read] = vint::deserialize(range);
        _64 = val;
        _varint_size = bytes_read;
        _prestate = prestate::none;
    }
}

// Reads bytes belonging to an integer of size len. Returns true
// if a full integer is now available.
bool continuous_batch_parser::process_int(
  temporary_buffer<char>& data, size_t len) {
    if (_pos >= len) {
        throw malformed_batch_stream_exception(
          "Overrun the amount of bytes to read");
    }
    auto n = std::min((size_t)(len - _pos), data.size());
    std::copy(data.begin(), data.begin() + n, _read_int.bytes + _pos);
    data.trim_front(n);
    _pos += n;
    return _pos == len;
}

continuous_batch_parser::read_status
continuous_batch_parser::read_fragmented_bytes(
  temporary_buffer<char>& data, size_t len) {
    _read_bytes.push_back(data.share(0, std::min(len, data.size())));
    if (data.size() >= len) {
        data.trim_front(len);
        return read_status::ready;
    }
    _ftb_size = len;
    _pos = data.size();
    data.trim(0);
    _prestate = prestate::reading_bytes;
    return read_status::waiting;
}

void continuous_batch_parser::ensure_valid_end_state() {
    if (_state != state::batch_start) {
        throw malformed_batch_stream_exception(
          "end of input, but not end of batch");
    }
}

} // namespace storage
