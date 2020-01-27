#pragma once

#include "bytes/iobuf.h"
#include "seastarx.h"
#include "utils/utf8.h"
#include "utils/vint.h"

#include <seastar/core/sstring.hh>

/// simple parser inspired by the kafka::request_reader
class iobuf_parser {
public:
    explicit iobuf_parser(iobuf buf)
      : _io(std::move(buf))
      , _in(_io.cbegin(), _io.cend())
      , _original_size(_io.size_bytes()) {}
    size_t bytes_left() const { return _original_size - _in.bytes_consumed(); }
    size_t bytes_consumed() const { return _in.bytes_consumed(); }

    std::pair<int64_t, uint8_t> read_varlong() {
        auto [val, length_size] = vint::deserialize(_in);
        _in.skip(length_size);
        return {val, length_size};
    }
    ss::sstring read_string(size_t len) {
        ss::sstring str(ss::sstring::initialized_later(), len);
        _in.consume_to(str.size(), str.begin());
        validate_utf8(str);
        return str;
    }
    bool read_bool() { return bool(consume_type<int8_t>()); }
    template<typename T>
    T consume_type() {
        return _in.consume_type<T>();
    }
    iobuf share(size_t len) {
        auto ret = _io.share(_in.bytes_consumed(), len);
        _in.skip(len);
        return ret;
    }

private:
    iobuf _io;
    iobuf::iterator_consumer _in;
    /// needed to compute bytes_left()
    size_t _original_size;
};
