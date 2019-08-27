#pragma once

#include "model/fundamental.h"

namespace storage {

class offset_tracker {
public:
    offset_tracker() noexcept = default;

    model::offset committed_offset() const {
        return _committed;
    }

    model::offset dirty_offset() const {
        return _dirty;
    }

private:
    void update_committed_offset(model::offset new_offset) {
        _committed = new_offset;
    }

    void update_dirty_offset(model::offset new_offset) {
        _dirty = new_offset;
    }

private:
    model::offset _committed;
    model::offset _dirty;

    friend class log;
};

} // namespace storage
