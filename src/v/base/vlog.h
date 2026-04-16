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
#include "base/source_location.h"
#include "base/vlog_callsite.h"

#define fmt_with_ctx(method, fmt, args...)                                     \
    method("{} - " fmt, vlog::file_line::current(), ##args)

#define fmt_with_ctx_level(logger, level, fmt, args...)                        \
    logger.log(level, "{} - " fmt, vlog::file_line::current(), ##args)

// Gate a vlog invocation on a static per-callsite enable flag. The flag
// defaults to enabled and is mutated at runtime by vlog::apply_rules. When
// disabled, none of the format arguments are evaluated and the logger is not
// called — the cost of a filtered-out site is one relaxed atomic load and a
// well-predicted branch.
//
// The callsite is a class-template instantiation parameterized on an NTTP
// carrying __FILE__, __LINE__, and the format literal. That lets the
// static instance be constant-initialized at program start, avoiding the
// __cxa_guard once-check that would otherwise run on every macro entry.
#define vlog(method, fmt, args...)                                             \
    do {                                                                       \
        static ::vlog::detail::callsite<::vlog::detail::make_site_nttp(        \
          __FILE__, __LINE__, fmt)>                                            \
          _vlog_cs = {};                                                       \
        if (_vlog_cs.enabled()) {                                              \
            fmt_with_ctx(method, fmt, ##args);                                 \
        }                                                                      \
    } while (0)

#define vlogl(logger, level, fmt, args...)                                     \
    do {                                                                       \
        static ::vlog::detail::callsite<::vlog::detail::make_site_nttp(        \
          __FILE__, __LINE__, fmt)>                                            \
          _vlog_cs = {};                                                       \
        if (_vlog_cs.enabled()) {                                              \
            fmt_with_ctx_level(logger, level, fmt, ##args);                    \
        }                                                                      \
    } while (0)

#define vloglr(logger, level, rate, fmt, args...)                              \
    do {                                                                       \
        static ::vlog::detail::callsite<::vlog::detail::make_site_nttp(        \
          __FILE__, __LINE__, fmt)>                                            \
          _vlog_cs = {};                                                       \
        if (_vlog_cs.enabled()) {                                              \
            logger.log(                                                        \
              level, rate, "{} - " fmt, vlog::file_line::current(), ##args);   \
        }                                                                      \
    } while (0)
