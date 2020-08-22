#pragma once

#include "model/fundamental.h"
#include "vassert.h"

#include <bits/stdint-uintn.h>
#include <roaring/roaring.hh>
namespace storage::internal {

/// This class contains batches of *individual* records. That is you take the
/// batch_offset+delta
class compacted_offset_list {
public:
    compacted_offset_list(
      model::offset base_offset, Roaring offset_list) noexcept
      : _base(base_offset)
      , _to_keep(std::move(offset_list)) {}
    compacted_offset_list(compacted_offset_list&&) noexcept = default;
    compacted_offset_list&
    operator=(compacted_offset_list&&) noexcept = default;
    compacted_offset_list(const compacted_offset_list&) = delete;
    compacted_offset_list& operator=(const compacted_offset_list&) = delete;
    ~compacted_offset_list() noexcept = default;

    bool contains(model::offset) const;
    void add(model::offset);

private:
    model::offset _base;
    Roaring _to_keep;
};

inline void compacted_offset_list::add(model::offset o) {
    vassert(
      o >= _base, "Cannot add logical offset:'{}' below base:'{}'", o, _base);
    const uint32_t x = (o - _base)();
    _to_keep.add(x);
}

inline bool compacted_offset_list::contains(model::offset o) const {
    const uint32_t x = (o - _base)();
    return _to_keep.contains(x);
}

} // namespace storage::internal
