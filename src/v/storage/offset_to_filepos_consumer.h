#pragma once

#include "model/fundamental.h"
#include "model/record.h"
#include "seastarx.h"
#include "storage/logger.h"

#include <seastar/core/future-util.hh>

#include <fmt/format.h>

#include <optional>

namespace storage::internal {
class offset_to_filepos_consumer {
public:
    using type = std::optional<std::pair<model::offset, size_t>>;
    offset_to_filepos_consumer(model::offset target, size_t initial)
      : _target_last_offset(target)
      , _accumulator(initial)
      , _prev(initial) {}
    ss::future<ss::stop_iteration> operator()(::model::record_batch batch) {
        _prev = _accumulator;
        _accumulator += batch.size_bytes() + sizeof(int32_t) /*see parser.cc*/;
        if (_target_last_offset == batch.base_offset()) {
            _filepos = {batch.base_offset() - model::offset(1), _prev};
            return ss::make_ready_future<ss::stop_iteration>(
              ss::stop_iteration::yes);
        }
        if (_target_last_offset == batch.last_offset()) {
            return ss::make_exception_future<ss::stop_iteration>(fmt::format(
              "offset_to_filepos_consumer can only translate base_offsets. "
              "Current batch's {}-{}. last_offset matches target offset:{}. "
              "accumulated bytes:{}",
              batch.base_offset(),
              batch.last_offset(),
              _target_last_offset,
              _accumulator));
        }
        if (batch.base_offset() > _target_last_offset) {
            return ss::make_exception_future<ss::stop_iteration>(fmt::format(
              "offset_to_filepos_consumer can only translate base_offsets. "
              "Current batch's {}-{}, exceeds target offset:{}. accumulated "
              "bytes:{}",
              batch.base_offset(),
              batch.last_offset(),
              _target_last_offset,
              _accumulator));
        }
        return ss::make_ready_future<ss::stop_iteration>(
          ss::stop_iteration::no);
    }
    type end_of_stream() { return _filepos; }

private:
    type _filepos;
    model::offset _target_last_offset;
    size_t _accumulator;
    size_t _prev;
};

} // namespace storage::internal
