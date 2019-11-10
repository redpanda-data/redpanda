#pragma once

#include "bytes/iobuf.h"
#include "hashing/xx.h"

#include <seastar/core/do_with.hh>
#include <seastar/core/iostream.hh>

#include <functional>

namespace rpc {

class source {
public:
    explicit source(input_stream<char>& s)
      : _source(std::ref(s)) {
    }
    source(const source&) = delete;
    source(source&&) noexcept = default;

    future<temporary_buffer<char>> read_exactly(size_t i) {
        return _source.get().read_exactly(i).then(
          [this](temporary_buffer<char> b) {
              if (b.size() > 0) {
                  _size += b.size();
                  _hash.update(b.get(), b.size());
              }
              return make_ready_future<temporary_buffer<char>>(std::move(b));
          });
    }
    future<iobuf> read_iobuf(size_t i) {
        return read_iobuf_exactly(_source.get(), i).then([this](iobuf b) {
            auto in = iobuf::iterator_consumer(b.cbegin(), b.cend());
            in.consume(b.size_bytes(), [this](const char* src, size_t sz) {
                _hash.update(src, sz);
                return stop_iteration::no;
            });
            _size += b.size_bytes();
            return std::move(b);
        });
    }
    size_t size_bytes() const {
        return _size;
    }
    uint64_t checksum() const {
        return _hash.digest();
    }

private:
    std::reference_wrapper<input_stream<char>> _source;
    mutable incremental_xxhash64 _hash{};
    size_t _size = 0;
};

} // namespace rpc
