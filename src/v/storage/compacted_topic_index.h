#pragma once
#include "bytes/bytes.h"
#include "model/fundamental.h"

#include <seastar/core/file.hh>
#include <seastar/core/future.hh>

#include <cstdint>

namespace storage {

/// format on file is:
/// key_size | key    | offset
/// -------- | ------ | ------
/// vint     | []byte | vint
/// vint     | []byte | vint
/// vint     | []byte | vint
/// ...
/// footer - in little endian
class compacted_topic_index {
public:
    static constexpr size_t footer_size = 13;
    struct footer {
        uint32_t size{0};
        uint32_t keys{0};
        uint32_t crc{0}; // crc32
        // version *must* be the last value
        int8_t version{0};
    };

    struct impl {
        virtual ~impl() noexcept = default;
        virtual ss::future<> write_key(bytes_view, model::offset) = 0;
        virtual ss::future<> close() = 0;
    };

    explicit compacted_topic_index(std::unique_ptr<impl> i)
      : _impl(std::move(i)) {}

    ss::future<> write_key(bytes_view, model::offset);
    ss::future<> close();
    std::unique_ptr<impl> release() &&;

private:
    std::unique_ptr<impl> _impl;
};

inline std::unique_ptr<compacted_topic_index::impl>
compacted_topic_index::release() && {
    return std::move(_impl);
}

inline ss::future<>
compacted_topic_index::write_key(bytes_view b, model::offset o) {
    return _impl->write_key(b, o);
}

inline ss::future<> compacted_topic_index::close() { return _impl->close(); }

compacted_topic_index
make_file_backed_compacted_index(ss::file, ss::io_priority_class p);

} // namespace storage
