#pragma once
#include "storage/log_segment_appender.h"
#include "storage/log_segment_reader.h"

namespace storage {
struct segment {
    segment(segment_reader_ptr r, segment_appender_ptr a) noexcept
      : reader(r)
      , appender(std::move(a)) {}
    explicit segment(segment_reader_ptr r) noexcept
      : reader(r) {}
    segment(segment&&) noexcept = default;
    segment& operator=(segment&&) noexcept = default;
    segment(const segment&) = delete;
    segment& operator=(const segment&) = delete;

    ss::future<> close() {
        auto f = reader->close();
        if (appender) {
            f = f.then([this] { return appender->close(); });
        }
        return f;
    }
    ss::future<> flush() {
        if (appender) {
            return appender->flush();
        }
        return ss::make_ready_future<>();
    }

    ss::future<> truncate(model::offset) { return ss::make_ready_future<>(); }

    // data
    segment_reader_ptr reader;
    segment_appender_ptr appender = nullptr;
};

inline std::ostream& operator<<(std::ostream& o, const segment& h) {
    o << "{reader=" << h.reader << ", writer=";
    if (h.appender) {
        o << *h.appender;
    } else {
        o << "nullptr";
    }
    return o << "}";
}

} // namespace storage
