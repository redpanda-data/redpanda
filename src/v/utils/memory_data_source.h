#pragma once

#include <seastar/core/iostream.hh>

#include <numeric>
#include <vector>

class memory_data_source final : public data_source_impl {
public:
    using value_type = temporary_buffer<char>;
    using vector_type = std::vector<value_type>;
    explicit memory_data_source(vector_type buffers)
      : capacity(std::accumulate(
        buffers.begin(),
        buffers.end(),
        size_t(0),
        [](size_t acc, auto& it) { return acc + it.size(); }))
      , _buffers(std::move(buffers)) {}
    future<value_type> skip(uint64_t n) final {
        _byte_offset = std::min(_byte_offset + n, capacity);
        return get();
    }
    future<value_type> get() final {
        if (_byte_offset >= capacity) {
            return make_ready_future<value_type>();
        }
        int64_t offset = _byte_offset;
        for (auto& _buffer : _buffers) {
            const int64_t end = _buffer.size();
            if (offset - end < 0) {
                _byte_offset += end - offset;
                return make_ready_future<value_type>(
                  _buffer.share(offset, end));
            }
            offset -= end;
        }
        return make_ready_future<value_type>();
    }

    const size_t capacity;

private:
    vector_type _buffers;
    size_t _byte_offset = 0;
};
