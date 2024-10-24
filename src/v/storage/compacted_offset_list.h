/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "base/vassert.h"
#include "model/fundamental.h"

#include <bits/stdint-uintn.h>
#include <roaring/roaring.hh>
namespace storage::internal {

/// This class contains batches of *individual* records. That is you take the
/// batch_offset+delta
class compacted_offset_list {
public:
    compacted_offset_list(
      model::offset base_offset, roaring::Roaring offset_list) noexcept
      : _base(base_offset)
      , _to_keep(std::move(offset_list)) {}
    compacted_offset_list(compacted_offset_list&&) noexcept = default;
    compacted_offset_list& operator=(compacted_offset_list&&) noexcept
      = default;
    compacted_offset_list(const compacted_offset_list&) = delete;
    compacted_offset_list& operator=(const compacted_offset_list&) = delete;
    ~compacted_offset_list() noexcept = default;

    bool contains(model::offset) const;
    void add(model::offset);

private:
    model::offset _base;
    roaring::Roaring _to_keep;

    friend std::ostream&
    operator<<(std::ostream& o, const compacted_offset_list& l) {
        return o << "{base:" << l._base << ", logical_offsets_cardinality: "
                 << l._to_keep.cardinality()
                 << ", offset_mem_bytes: " << l._to_keep.getSizeInBytes()
                 << "}";
    }
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
