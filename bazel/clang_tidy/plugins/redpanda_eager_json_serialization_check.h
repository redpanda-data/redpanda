// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include <clang-tidy/ClangTidyCheck.h>

namespace clang::tidy::redpanda {

/**
 * A clang-tidy check that flags construction of
 * `seastar::json::json_return_type` from a container (any range that is not
 * string-like). The converting constructor eagerly serializes the whole range
 * into a single contiguous string via `formatter::to_json`, which for large
 * responses requires an oversized contiguous allocation and can crash the
 * process when memory is fragmented (allocation failure aborts by default).
 *
 * Instead, return `ss::json::stream_range_as_array(...)`, which streams
 * elements to the HTTP output stream with bounded allocations. See
 * src/v/redpanda/admin/usage.cc for an example.
 *
 * Scalars, strings and fixed-size json objects are not flagged: they take the
 * scalar serialization path and their size does not grow with cluster state.
 */
class EagerJsonSerialization : public ClangTidyCheck {
public:
    EagerJsonSerialization(StringRef Name, ClangTidyContext* Context)
      : ClangTidyCheck(Name, Context) {}
    void registerMatchers(ast_matchers::MatchFinder* Finder) override;
    void check(const ast_matchers::MatchFinder::MatchResult& Result) override;
};

} // namespace clang::tidy::redpanda
