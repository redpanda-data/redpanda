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

#define fmt_with_ctx_force(method, fmt, args...)                               \
    method(                                                                    \
      ::seastar::logger::force,                                                \
      "{} - " fmt,                                                             \
      vlog::file_line::current(),                                              \
      ##args)

#define fmt_with_ctx_level(logger_, level, fmt, args...)                       \
    logger_.log(level, "{} - " fmt, vlog::file_line::current(), ##args)

#define fmt_with_ctx_level_force(logger_, level, fmt, args...)                 \
    logger_.log(                                                               \
      level,                                                                   \
      ::seastar::logger::force,                                                \
      "{} - " fmt,                                                             \
      vlog::file_line::current(),                                              \
      ##args)

// Gate a vlog invocation on the static per-callsite state. default_ goes
// through the logger's configured level gate, force_on bypasses the gate
// via the seastar force_tag overloads, force_off drops the call entirely
// without evaluating any format argument.
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
        switch (_vlog_cs.resolved_state()) {                                   \
        case ::vlog::detail::callsite_base::state::default_:                   \
            fmt_with_ctx(method, fmt, ##args);                                 \
            break;                                                             \
        case ::vlog::detail::callsite_base::state::force_on:                   \
            fmt_with_ctx_force(method, fmt, ##args);                           \
            break;                                                             \
        case ::vlog::detail::callsite_base::state::force_off:                  \
        case ::vlog::detail::callsite_base::state::uninit:                     \
            break;                                                             \
        }                                                                      \
    } while (0)

#define vlogl(logger_, level, fmt, args...)                                    \
    do {                                                                       \
        static ::vlog::detail::callsite<::vlog::detail::make_site_nttp(        \
          __FILE__, __LINE__, fmt)>                                            \
          _vlog_cs = {};                                                       \
        switch (_vlog_cs.resolved_state()) {                                   \
        case ::vlog::detail::callsite_base::state::default_:                   \
            fmt_with_ctx_level(logger_, level, fmt, ##args);                   \
            break;                                                             \
        case ::vlog::detail::callsite_base::state::force_on:                   \
            fmt_with_ctx_level_force(logger_, level, fmt, ##args);             \
            break;                                                             \
        case ::vlog::detail::callsite_base::state::force_off:                  \
        case ::vlog::detail::callsite_base::state::uninit:                     \
            break;                                                             \
        }                                                                      \
    } while (0)

#define vloglr(logger_, level, rate, fmt, args...)                             \
    do {                                                                       \
        static ::vlog::detail::callsite<::vlog::detail::make_site_nttp(        \
          __FILE__, __LINE__, fmt)>                                            \
          _vlog_cs = {};                                                       \
        switch (_vlog_cs.resolved_state()) {                                   \
        case ::vlog::detail::callsite_base::state::default_:                   \
            logger_.log(                                                       \
              level, rate, "{} - " fmt, vlog::file_line::current(), ##args);   \
            break;                                                             \
        case ::vlog::detail::callsite_base::state::force_on:                   \
            logger_.log(                                                       \
              level,                                                           \
              ::seastar::logger::force,                                        \
              rate,                                                            \
              "{} - " fmt,                                                     \
              vlog::file_line::current(),                                      \
              ##args);                                                         \
            break;                                                             \
        case ::vlog::detail::callsite_base::state::force_off:                  \
        case ::vlog::detail::callsite_base::state::uninit:                     \
            break;                                                             \
        }                                                                      \
    } while (0)
