#pragma once

#include "hashing/xx.h"
#include "utils/fragmented_temporary_buffer.h"

#include <seastar/core/do_with.hh>
#include <seastar/core/iostream.hh>
namespace rpc {

class source {
public:
    virtual future<temporary_buffer<char>> read_exactly(size_t) = 0;
    virtual future<fragmented_temporary_buffer>
      read_fragmented_temporary_buffer(size_t) = 0;
    virtual ~source() = default;
};

class default_source final : public source {
public:
    explicit default_source(input_stream<char>& s)
      : _source(s) {
    }
    future<temporary_buffer<char>> read_exactly(size_t i) final {
        return _source.read_exactly(i);
    }
    future<fragmented_temporary_buffer>
    read_fragmented_temporary_buffer(size_t i) final {
        return _frag.read_exactly(_source, i);
    }

    default_source(const default_source&) = delete;
    ~default_source() noexcept = default;

private:
    input_stream<char>& _source;
    fragmented_temporary_buffer::reader _frag;
};

class checksum_source final : public source {
public:
    using ftb = fragmented_temporary_buffer;
    explicit checksum_source(input_stream<char>& s)
      : _source(s) {
    }
    future<temporary_buffer<char>> read_exactly(size_t i) final {
        return _source.read_exactly(i).then([this](temporary_buffer<char> b) {
            if (b.size() > 0) {
                _hash.update(b.get(), b.size());
            }
            return make_ready_future<temporary_buffer<char>>(std::move(b));
        });
    }
    future<fragmented_temporary_buffer>
    read_fragmented_temporary_buffer(size_t i) final {
        return _frag.read_exactly(_source, i).then([this](ftb b) {
            const size_t byte_size = b.size_bytes();
            if (byte_size > 0) {
                auto vec = std::move(b).release();
                for (auto& buf : vec) {
                    _hash.update(buf.get(), buf.size());
                }
                return make_ready_future<ftb>(ftb(std::move(vec), byte_size));
            }
            return make_ready_future<ftb>(std::move(b));
        });
    }
    uint64_t checksum() {
        return _hash.digest();
    }
    checksum_source(const checksum_source&) = delete;
    ~checksum_source() noexcept = default;

private:
    input_stream<char>& _source;
    incremental_xxhash64 _hash;
    ftb::reader _frag;
};

} // namespace rpc
