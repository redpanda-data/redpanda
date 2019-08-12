#pragma once

#include "bytes/bytes_ostream.h"
#include "utils/memory_data_source.h"

namespace rpc {
inline input_stream<char> make_input_stream(bytes_ostream&& o) {
    auto frags = std::move(o).release();
    std::vector<temporary_buffer<char>> vec;
    vec.reserve(frags.size());
    for (auto& f : frags) {
        vec.push_back(std::move(f).release());
    }
    auto ds = data_source(std::make_unique<memory_data_source>(std::move(vec)));
    return input_stream<char>(std::move(ds));
}
} // namespace rpc
