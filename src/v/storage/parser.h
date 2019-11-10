#pragma once

#include "model/record.h"
#include "seastarx.h"
#include "storage/exceptions.h"
#include "storage/failure_probes.h"
#include "bytes/iobuf.h"
#include "utils/vint.h"

#include <seastar/core/byteorder.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/iostream.hh>

#include <variant>

namespace storage {

class batch_consumer {
public:
    using skip = bool_class<class skip_tag>;

    virtual ~batch_consumer() = default;

    virtual skip
    consume_batch_start(model::record_batch_header, size_t num_records)
      = 0;

    virtual skip consume_record_key(
      size_t size_bytes,
      model::record_attributes attributes,
      int32_t timestamp_delta,
      int32_t offset_delta,
      iobuf&& key)
      = 0;

    virtual void consume_record_value(iobuf&& value_and_headers) = 0;

    virtual void consume_compressed_records(iobuf&&) = 0;

    virtual stop_iteration consume_batch_end() = 0;
};

namespace detail {

template<typename T>
static inline T consume_be(temporary_buffer<char>& p) {
    T i = read_be<T>(p.get());
    p.trim_front(sizeof(T));
    return i;
}

} // namespace detail

using parse_result = std::variant<stop_iteration, skip_bytes>;

inline bool operator==(const parse_result& result, stop_iteration value) {
    auto* p = std::get_if<stop_iteration>(&result);
    return p != nullptr && *p == value;
}

inline bool operator!=(const parse_result& result, stop_iteration value) {
    return !(result == value);
}

// Reads batches from a log segment.
// Currently, only the initial file format
// is supported.
class continuous_batch_parser {
    enum class prestate {
        none,
        reading_vint,
        reading_8,
        reading_16,
        reading_32,
        reading_64,
        reading_bytes,
    };

    enum class state {
        batch_start,
        base_offset,
        batch_type,
        crc,
        attributes,
        last_offset_delta,
        first_timestamp,
        max_timestamp,
        header_done,
        record_start,
        record_count,
        record_attributes,
        timestamp_delta,
        offset_delta,
        key_length,
        key_bytes,
        key_done,
        value_and_headers,
        record_end,
        compressed_records_start,
        compressed_records_end,
        batch_end,
    };

    bool non_consuming() const {
        return _prestate == prestate::none
               && (_state == state::header_done || _state == state::key_done || _state == state::record_end || _state == state::batch_end);
    }
    using failure_probes = parser_failure_probes;

public:
    continuous_batch_parser(
      batch_consumer& consumer, input_stream<char>& input) noexcept
      : _consumer(&consumer)
      , _input(&input) {
        finjector::shard_local_badger().register_probe(
          failure_probes::name(), &_fprobe);
    }
    continuous_batch_parser(continuous_batch_parser&&) = default;
    continuous_batch_parser& operator=(continuous_batch_parser&&) = default;
    ~continuous_batch_parser() {
        finjector::shard_local_badger().deregister_probe(
          failure_probes::name());
    }

    [[gnu::always_inline]] future<size_t> consume() {
        return _fprobe.consume().then([this] { return do_consume(); });
    }

    // Called by input_stream::consume().
    future<consumption_result<char>> operator()(temporary_buffer<char>);

private:
    parse_result process(temporary_buffer<char>&);
    void process_sliced_data(temporary_buffer<char>&);
    parse_result do_process(temporary_buffer<char>&);

    enum class read_status {
        ready,
        waiting,
    };

    future<size_t> do_consume() {
        return _input->consume(*this).then([this] { return _bytes_consumed; });
    }
    // Read an integer. If the buffer doesn't contain the whole thing,
    // remember what we have in the buffer and continue later by using
    // a "prestate".
    template<typename Integer>
    read_status read_int(temporary_buffer<char>& data) {
        static_assert(std::is_signed_v<Integer>);
        if (__builtin_expect(data.size() >= sizeof(Integer), true)) {
            if constexpr (sizeof(Integer) == 1) {
                _8 = detail::consume_be<Integer>(data);
            } else if constexpr (sizeof(Integer) == 2) {
                _16 = detail::consume_be<Integer>(data);
            } else if constexpr (sizeof(Integer) == 4) {
                _32 = detail::consume_be<Integer>(data);
            } else if constexpr (sizeof(Integer) == 8) {
                _64 = detail::consume_be<Integer>(data);
            }
            return read_status::ready;
        }
        _pos = 0;
        if constexpr (sizeof(Integer) == 1) {
            _prestate = prestate::reading_8;
        } else if constexpr (sizeof(Integer) == 2) {
            _prestate = prestate::reading_16;
        } else if constexpr (sizeof(Integer) == 4) {
            _prestate = prestate::reading_32;
        } else if constexpr (sizeof(Integer) == 8) {
            _prestate = prestate::reading_64;
        }
        return read_status::waiting;
    }

    read_status read_vint(temporary_buffer<char>&);
    read_status read_fragmented_bytes(temporary_buffer<char>&, size_t len);

    bool process_int(temporary_buffer<char>&, size_t len);
    void process_vint(temporary_buffer<char>&);

    void ensure_valid_end_state();

    continuous_batch_parser() = default;

    explicit operator bool() const noexcept {
        return bool(_consumer);
    }

    friend class optimized_optional<continuous_batch_parser>;

private:
    batch_consumer* _consumer = nullptr;
    input_stream<char>* _input = nullptr;
    prestate _prestate = prestate::none;
    // state for non-NONE prestates
    uint32_t _pos;
    // state for the integer pre-state, in host byte order.
    int8_t _8;
    int16_t _16;
    int32_t _32;
    int64_t _64;
    size_t _varint_size;
    // state for reading fragmented ints, in big-endian.
    union {
        char bytes[vint::max_length];
        int64_t int64;
        int32_t int32;
        int16_t int16;
        int8_t int8;
    } _read_int;
    // state for reading fragmented bytes
    size_t _ftb_size;
    std::vector<temporary_buffer<char>> _read_bytes;
    // state for reading batches
    state _state = state::batch_start;
    model::record_batch_header _header;
    bool _compressed_batch = false;
    size_t _num_records;
    size_t _record_size;
    model::record_attributes _record_attributes;
    int32_t _timestamp_delta;
    int32_t _offset_delta;
    size_t _value_and_headers_size;
    size_t _bytes_consumed;
    failure_probes _fprobe;
};

using continuous_batch_parser_opt = optimized_optional<continuous_batch_parser>;

} // namespace storage
