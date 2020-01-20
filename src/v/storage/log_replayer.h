#pragma once

#include "model/fundamental.h"
#include "seastarx.h"
#include "storage/segment.h"

#include <seastar/core/io_queue.hh>
#include <seastar/util/bool_class.hh>

namespace storage {

class log_replayer {
public:
    explicit log_replayer(segment& seg) noexcept
      : _seg(&seg) {}

    class [[nodiscard]] recovered {
        explicit recovered(
          bool good, std::optional<model::offset> offset) noexcept
          : _good(good)
          , _last_valid_offset(std::move(offset)) {}

    public:
        operator bool() const { return _good; }

        std::optional<model::offset> last_valid_offset() const {
            return _last_valid_offset;
        }

    private:
        bool _good;
        std::optional<model::offset> _last_valid_offset;

        friend class log_replayer;
    };

    // Must be called in the context of a ss::thread
    recovered recover_in_thread(const ss::io_priority_class&);

private:
    segment* _seg;
};

} // namespace storage
