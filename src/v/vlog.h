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
#include "utils/source_location.h"

// NOLINTNEXTLINE
#define fmt_with_ctx(method, fmt, args...)                                     \
    method(                                                                    \
      "{}:{} - " fmt,                                                          \
      (const char*)&__FILE__[vlog_internal::log_basename_start<                \
        vlog_internal::basename_index(__FILE__)>::value],                      \
      __LINE__,                                                                \
      ##args)

// NOLINTNEXTLINE
#define vlog(method, fmt, args...) fmt_with_ctx(method, fmt, ##args)

// NOLINTNEXTLINE
#define fmt_with_ctx_level(logger, level, fmt, args...)                        \
    logger.log(                                                                \
      level,                                                                   \
      "{}:{} - " fmt,                                                          \
      (const char*)&__FILE__[vlog_internal::log_basename_start<                \
        vlog_internal::basename_index(__FILE__)>::value],                      \
      __LINE__,                                                                \
      ##args)

// NOLINTNEXTLINE
#define vlogl(logger, level, fmt, args...)                                     \
    fmt_with_ctx_level(logger, level, fmt, ##args)
