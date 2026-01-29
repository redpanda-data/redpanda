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

// Start ignoring deprecations from this line, should be paired with
// REDPANDA_END_IGNORE_DEPRECATIONS after the region you want to ignore
// Use of this macro is ideally paired with a comment/JIRA reference
// indicating the plan for removing the ignored deprecations.
#define REDPANDA_BEGIN_IGNORE_DEPRECATIONS                                     \
    _Pragma("GCC diagnostic push")                                             \
      _Pragma("GCC diagnostic ignored \"-Wdeprecated-declarations\"")

#define REDPANDA_END_IGNORE_DEPRECATIONS _Pragma("GCC diagnostic pop")
