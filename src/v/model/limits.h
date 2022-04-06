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

#include "model/fundamental.h"

namespace model {

template<typename T>
struct model_limits {};

template<>
struct model_limits<offset> {
    static constexpr offset max() {
        return offset(std::numeric_limits<typename offset::type>::max());
    }
    static constexpr offset min() {
        return offset(std::numeric_limits<typename offset::type>::min());
    }
};
template<>
struct model_limits<term_id> {
    static constexpr term_id max() {
        return term_id(std::numeric_limits<typename term_id::type>::max());
    }
    static constexpr term_id min() {
        return term_id(std::numeric_limits<typename term_id::type>::min());
    }
};

} // namespace model
