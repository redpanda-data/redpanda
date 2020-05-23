#pragma once
#include "bytes/bytes.h"
#include "model/fundamental.h"

#include <seastar/core/file.hh>
#include <seastar/core/future.hh>

#include <bits/stdint-intn.h>

#include <cstdint>

namespace storage {

/** format on file is:
    TYPE PAYLOAD
    TYPE PAYLOAD
    TYPE PAYLOAD
    FOOTER

Details:

    TYPE::KEY
    VINT   // byte size of key
    []BYTE // actual key (in truncate events we use 'truncation')
    VINT   // batch-base-offset
    VINT   // record-offset-delta


footer - in little endian
*/
class compacted_topic_index {
public:
    static constexpr size_t footer_size = 13;
    enum class entry_type : uint8_t {
        key,
        /// \brief because of raft truncations, we write a truncation, for
        /// the recovery thread to compact up to key-point on the index.
        truncation,
    };
    // bitflags for index
    enum class footer_flags : uint32_t {
        none = 0,
        /// needed for truncation events in the same raft-term
        truncation = 1U,
    };
    struct footer {
        uint32_t size{0};
        uint32_t keys{0};
        footer_flags flags{0};
        uint32_t crc{0}; // crc32
        // version *must* be the last value
        int8_t version{0};
    };
    class impl {
    public:
        explicit impl(ss::sstring filename) noexcept
          : _name(std::move(filename)) {}
        virtual ~impl() noexcept = default;
        impl(impl&&) noexcept = default;
        impl& operator=(impl&&) noexcept = default;
        impl(const impl&) = delete;
        impl& operator=(const impl&) = delete;

        virtual ss::future<> index(
          bytes_view, // convert from bytes which is the key-type in map
          model::offset base_offset,
          int32_t offset_delta)
          = 0;

        virtual ss::future<> index(
          const iobuf& key, // default format in record batch
          model::offset base_offset,
          int32_t offset_delta)
          = 0;

        virtual ss::future<> truncate(model::offset) = 0;

        virtual ss::future<> close() = 0;
        const ss::sstring& filename() const { return _name; }

    private:
        ss::sstring _name;
    };

    explicit compacted_topic_index(std::unique_ptr<impl> i)
      : _impl(std::move(i)) {}

    ss::future<> index(bytes_view, model::offset, int32_t);
    ss::future<> index(const iobuf& key, model::offset, int32_t);
    ss::future<> truncate(model::offset);
    ss::future<> close();
    const ss::sstring& filename() const;
    std::unique_ptr<impl> release() &&;

private:
    std::unique_ptr<impl> _impl;
};

inline const ss::sstring& compacted_topic_index::filename() const {
    return _impl->filename();
}
inline std::unique_ptr<compacted_topic_index::impl>
compacted_topic_index::release() && {
    return std::move(_impl);
}
inline ss::future<> compacted_topic_index::index(
  const iobuf& b, model::offset base_offset, int32_t delta) {
    return _impl->index(b, base_offset, delta);
}
inline ss::future<> compacted_topic_index::index(
  bytes_view b, model::offset base_offset, int32_t delta) {
    return _impl->index(b, base_offset, delta);
}
inline ss::future<> compacted_topic_index::truncate(model::offset o) {
    return _impl->truncate(o);
}
inline ss::future<> compacted_topic_index::close() { return _impl->close(); }

inline compacted_topic_index::footer_flags operator|(
  compacted_topic_index::footer_flags a,
  compacted_topic_index::footer_flags b) {
    return compacted_topic_index::footer_flags(
      std::underlying_type_t<compacted_topic_index::footer_flags>(a)
      | std::underlying_type_t<compacted_topic_index::footer_flags>(b));
}

inline void operator|=(
  compacted_topic_index::footer_flags& a,
  compacted_topic_index::footer_flags b) {
    a = (a | b);
}

inline compacted_topic_index::footer_flags
operator~(compacted_topic_index::footer_flags a) {
    return compacted_topic_index::footer_flags(
      ~std::underlying_type_t<compacted_topic_index::footer_flags>(a));
}

inline compacted_topic_index::footer_flags operator&(
  compacted_topic_index::footer_flags a,
  compacted_topic_index::footer_flags b) {
    return compacted_topic_index::footer_flags(
      std::underlying_type_t<compacted_topic_index::footer_flags>(a)
      & std::underlying_type_t<compacted_topic_index::footer_flags>(b));
}

inline void operator&=(
  compacted_topic_index::footer_flags& a,
  compacted_topic_index::footer_flags b) {
    a = (a & b);
}

inline std::ostream&
operator<<(std::ostream& o, const compacted_topic_index::footer& f) {
    return o << "{size:" << f.size << ", keys:" << f.keys
             << ", flags:" << (uint32_t)f.flags << ", crc:" << f.crc
             << ", version: " << (int)f.version << "}";
}

compacted_topic_index make_file_backed_compacted_index(
  ss::sstring filename, ss::file, ss::io_priority_class p, size_t max_memory);

} // namespace storage
