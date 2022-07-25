// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include <fmt/format.h>

#define XSTR(s) STR(s)
#define STR(s) #s

#define verify(condition, ...)                                                 \
    {                                                                          \
        if (!(condition)) {                                                    \
            throw std::runtime_error{fmt::format(                              \
              "{}:{} verify {} failed: {}",                                    \
              __FILE__,                                                        \
              __LINE__,                                                        \
              XSTR(condition),                                                 \
              fmt::format(__VA_ARGS__))};                                      \
        }                                                                      \
    }

#define verify_equal(x, y)                                                     \
    {                                                                          \
        if (!((x) == (y))) {                                                   \
            throw std::runtime_error{fmt::format(                              \
              "{}:{} not equal: {} vs {}", __FILE__, __LINE__, x, y)};         \
        }                                                                      \
    }
