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
#include <fmt/core.h>
#include <fmt/ostream.h>

#if FMT_VERSION >= 90100
// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define RP_OSTREAM_FMT(type)                                                   \
    template<>                                                                 \
    struct fmt::formatter<type> : fmt::ostream_formatter {};

#define RP_FMT_STREAMED(x) fmt::streamed(x)
#else
#define RP_OSTREAM_FMT(type)
#define RP_FMT_STREAMED(x)
#endif
