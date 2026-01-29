/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "container/chunked_vector.h"
#include "lsm/core/internal/iterator.h"

#include <memory>

namespace lsm::internal {

// Return an iterator that provides the union of data in `children`.
//
// The resulting iterator does not supress duplicates. I.e., if a particular key
// is present in K child iterators, it will be yielded K times.
std::unique_ptr<iterator>
create_merging_iterator(chunked_vector<std::unique_ptr<iterator>> children);

} // namespace lsm::internal
