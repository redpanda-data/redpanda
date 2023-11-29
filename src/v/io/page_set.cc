/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "io/page_set.h"

namespace experimental::io {

page_set::const_iterator::const_iterator(map_type::const_iterator it)
  : const_iterator::iterator_adaptor(it) {}

page_set::const_iterator::reference
page_set::const_iterator::dereference() const {
    return base_reference()->second;
}

page_set::const_iterator page_set::find(uint64_t offset) const {
    return const_iterator(pages_.find(offset));
}

void page_set::erase(page_set::const_iterator it) {
    pages_.erase(it.base_reference());
}

page_set::const_iterator page_set::begin() const {
    return const_iterator(pages_.begin());
}

page_set::const_iterator page_set::end() const {
    return const_iterator(pages_.end());
}

std::pair<page_set::const_iterator, bool>
page_set::insert(seastar::lw_shared_ptr<page> page) {
    auto offset = page->offset();
    auto size = page->size();
    auto res = pages_.insert({offset, size}, std::move(page));
    return {const_iterator(res.first), res.second};
}

} // namespace experimental::io
