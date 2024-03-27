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
#include "io/page_cache.h"

namespace experimental::io {

void page_cache::insert(page& page) noexcept { cache_.insert(page); }

void page_cache::remove(const page& page) noexcept { cache_.remove(page); }

bool page_cache::evict::operator()(page& page) noexcept {
    if (page.may_evict()) {
        page.clear();
        return true;
    }
    return false;
}

size_t page_cache::cost::operator()(const page& /*page*/) noexcept { return 1; }

} // namespace experimental::io
