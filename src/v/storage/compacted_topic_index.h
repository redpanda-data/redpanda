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
/// footer
class compacted_topic_index {
public:
    static constexpr size_t footer_size = 16;
    struct footer {
        int8_t version{0};
        int8_t unused_padding_1{0};
        int16_t unused_padding_2{0};
        uint32_t size{0};
        uint32_t crc{0};
        uint32_t keys{0};
    };
    static_assert(sizeof(footer) == footer_size, "needed for lazy reader");

    struct impl {
        virtual ~impl() noexcept = default;
        virtual ss::future<> write_key(const bytes&, model::offset) = 0;
        virtual ss::future<> close() = 0;
    };

    explicit compacted_topic_index(std::unique_ptr<impl> i)
      : _impl(std::move(i)) {}

    ss::future<> write_key(const bytes&, model::offset);
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
compacted_topic_index::write_key(const bytes& b, model::offset o) {
    return _impl->write_key(b, o);
}

inline ss::future<> compacted_topic_index::close() { return _impl->close(); }

compacted_topic_index
make_file_backed_compacted_index(ss::file, ss::io_priority_class p);

} // namespace storage
