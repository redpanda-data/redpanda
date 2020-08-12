#pragma once

#include "bytes/bytes.h"
#include "bytes/iobuf.h"
#include "seastarx.h"
#include "utils/utf8.h"
#include "utils/vint.h"

#include <seastar/core/sstring.hh>

#include <memory>

class iobuf_parser final {
public:
    explicit iobuf_parser(iobuf buf)
      : _state(std::make_unique<parser_state>(std::move(buf)))
      , _original_size(_state->io.size_bytes()) {}

    size_t bytes_left() const {
        return _original_size - _state->in.bytes_consumed();
    }

    size_t bytes_consumed() const { return _state->in.bytes_consumed(); }

    std::pair<int64_t, uint8_t> read_varlong() {
        auto [val, length_size] = vint::deserialize(_state->in);
        _state->in.skip(length_size);
        return {val, length_size};
    }

    ss::sstring read_string(size_t len) {
        ss::sstring str = ss::uninitialized_string(len);
        _state->in.consume_to(str.size(), str.begin());
        validate_utf8(str);
        return str;
    }

    bytes read_bytes(size_t n) {
        auto b = ss::uninitialized_string<bytes>(n);
        _state->in.consume_to(n, b.begin());
        return b;
    }

    bool read_bool() { return bool(consume_type<int8_t>()); }

    template<typename T>
    T consume_type() {
        return _state->in.consume_type<T>();
    }

    template<typename T>
    T consume_be_type() {
        return _state->in.consume_be_type<T>();
    }

    void skip(size_t n) { _state->in.skip(n); }

    iobuf share(size_t len) {
        auto ret = _state->io.share(_state->in.bytes_consumed(), len);
        _state->in.skip(len);
        return ret;
    }

    // clang-format off
    template<typename Consumer>
    CONCEPT(requires requires(Consumer c, const char* src, size_t max) {
        { c(src, max) } -> ss::stop_iteration;
    })
    // clang-format on
    size_t consume(const size_t n, Consumer&& f) {
        return _state->in.consume(n, std::forward<Consumer>(f));
    }

private:
    // See io_iterator_consumer for notes on iterator validity.
    struct parser_state {
        iobuf io;
        iobuf::iterator_consumer in;

        explicit parser_state(iobuf&& buf)
          : io(std::move(buf))
          , in(io.cbegin(), io.cend()) {}
    };

    std::unique_ptr<parser_state> _state;
    /// needed to compute bytes_left()
    size_t _original_size;
};

inline std::ostream& operator<<(std::ostream& o, const iobuf_parser& p) {
    return o << "{bytes_left:" << p.bytes_left()
             << ", bytes_consumed:" << p.bytes_consumed() << "}";
}
