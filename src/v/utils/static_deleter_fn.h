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
#include <cstddef>

// These functions for the gifts from the C language, to us.
// wrappers around C-api free_*

template<typename T, void (*f)(T*)>
struct static_deleter_fn {
    void operator()(T* t) const { f(t); }
};
template<typename T, size_t (*f)(T*)>
struct static_sized_deleter_fn {
    void operator()(T* t) const { (void)f(t); }
};
template<typename T, typename R, R (*f)(T*)>
struct static_retval_deleter_fn {
    void operator()(T* t) const { (void)f(t); }
};

namespace internal {
static inline void static_deleter_noop(void*) {}
} // namespace internal
template<typename T>
using static_deleter_noop
  = static_deleter_fn<T, &internal::static_deleter_noop>;
