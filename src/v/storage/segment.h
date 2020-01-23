#pragma once

#include "storage/log_segment_appender.h"
#include "storage/log_segment_reader.h"
#include "storage/segment_offset_index.h"
#include "storage/types.h"

#include <seastar/core/file.hh>

namespace storage {
class segment {
public:
    segment(
      segment_reader_ptr,
      segment_offset_index_ptr,
      segment_appender_ptr) noexcept;

    segment(segment&&) noexcept = default;
    segment& operator=(segment&&) noexcept = default;
    segment(const segment&) = delete;
    segment& operator=(const segment&) = delete;

    ss::future<> close();
    ss::future<> flush();
    ss::future<> truncate(model::offset);

    /// main write interface
    /// auto indexes record_batch
    ss::future<append_result> append(model::record_batch);

    /// main read interface
    ss::input_stream<char>
      offset_data_stream(model::offset, ss::io_priority_class);

    bool empty() const { return _reader->empty(); }
    model::offset committed_offset() const { return _reader->max_offset(); }
    model::offset dirty_offset() const {
        if (_appender) {
            return _dirty_offset;
        }
        return committed_offset();
    }
    // low level api's are discouraged and might be deprecated
    // please use higher level API's when possible

    segment_reader_ptr reader() const { return _reader; }
    segment_offset_index_ptr& oindex() { return _oidx; }
    const segment_offset_index_ptr& oindex() const { return _oidx; }
    segment_appender_ptr& appender() { return _appender; }
    const segment_appender_ptr& appender() const { return _appender; }
    bool has_appender() const { return bool(_appender); }
    operator bool() const { return bool(_reader); }

private:
    // last offset of the last batch, i.e.: batch.last_offset()
    model::offset _dirty_offset;
    segment_reader_ptr _reader;
    segment_offset_index_ptr _oidx;
    segment_appender_ptr _appender = nullptr;
};

std::ostream& operator<<(std::ostream&, const segment&);
} // namespace storage
