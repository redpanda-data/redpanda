// Copyright 2025 Redpanda Data, Inc.
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
 * A clang-tidy check that does nothing.
 *
 * Included for demonstration purposes and so the tool doesn't complain about
 * the 'redpanda-*' glob in .clang-tidy.
 *
 */
class NoopCheck : public ClangTidyCheck {
public:
    NoopCheck(StringRef Name, ClangTidyContext* Context)
      : ClangTidyCheck(Name, Context) {}
};

} // namespace clang::tidy::redpanda
