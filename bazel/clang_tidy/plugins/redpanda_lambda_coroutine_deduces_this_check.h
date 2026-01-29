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
 * A clang-tidy check that ensures that any lambda that is a coroutine passes
 * `this` as the first parameter, thereby forcing captured variables in a lambda
 * to be moved into the coroutine frame, extending their lifetimes and
 * preventing what is known as the "lambda coroutine fiasco".
 * See:
 * https://github.com/scylladb/seastar/blob/master/doc/lambda-coroutine-fiasco.md
 *
 *
 */
class LambdaCoroutineDeducesThis : public ClangTidyCheck {
public:
    LambdaCoroutineDeducesThis(StringRef Name, ClangTidyContext* Context)
      : ClangTidyCheck(Name, Context) {}
    void registerMatchers(ast_matchers::MatchFinder* Finder) override;
    void check(const ast_matchers::MatchFinder::MatchResult& Result) override;
    bool isLanguageVersionSupported(const LangOptions& LangOpts) const override;
};

} // namespace clang::tidy::redpanda
