/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "base/seastarx.h"

#include <seastar/core/cacheline.hh>

namespace ssx {

/// aligned is a wrapper to cache-align the wrapped type in order to reduce the
/// chance of false sharing between the wrapped type and the data around it.
/// This is useful in cases such as when you perform atomic operatons on the
/// wrapped type or when you store an array of the padded type together and
/// access different values of the array from different cores.
template<typename T>
struct alignas(ss::cache_line_size) aligned final : public T {};

} // namespace ssx
