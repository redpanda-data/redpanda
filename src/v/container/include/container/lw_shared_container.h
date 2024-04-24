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

#include "base/seastarx.h"

#include <seastar/core/shared_ptr.hh>

#pragma once

template<typename C>
class lw_shared_container {
public:
    using iterator = C::const_iterator;
    using value_type = C::value_type;

    explicit lw_shared_container(C&& c)
      : c_{ss::make_lw_shared<C>(std::move(c))} {}

    iterator begin() const { return c_->begin(); }
    iterator end() const { return c_->end(); }

private:
    ss::lw_shared_ptr<C> c_;
};
