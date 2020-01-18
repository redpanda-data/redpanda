#pragma once
#include "storage/log_segment_appender.h"
#include "storage/log_segment_reader.h"

namespace storage {
class segment {
public:
    segment(segment_reader_ptr, segment_appender_ptr) noexcept;
    explicit segment(segment_reader_ptr) noexcept;
    segment(segment&&) noexcept = default;
    segment& operator=(segment&&) noexcept = default;
    segment(const segment&) = delete;
    segment& operator=(const segment&) = delete;

    ss::future<> close();
    ss::future<> flush();
    ss::future<> truncate(model::offset);

    segment_reader_ptr reader() const { return _reader; }
    segment_appender_ptr& appender() { return _appender; }
    const segment_appender_ptr& appender() const { return _appender; }
    bool has_appender() const { return bool(_appender); }
    operator bool() const { return bool(_reader); }

private:
    // data
    segment_reader_ptr _reader;
    segment_appender_ptr _appender = nullptr;
};

std::ostream& operator<<(std::ostream&, const segment&);
} // namespace storage
