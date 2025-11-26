#pragma once

#include <clang-tidy/ClangTidy.h>
#include <clang-tidy/ClangTidyCheck.h>

namespace clang::tidy::redpanda {
/**
 * avoid evil optional usage
 */
class AvoidOptionalOperatorStar : public ClangTidyCheck {
public:
    AvoidOptionalOperatorStar(StringRef name, ClangTidyContext* context)
      : ClangTidyCheck(name, context) {}
    void registerMatchers(ast_matchers::MatchFinder*) final;
    void check(const ast_matchers::MatchFinder::MatchResult&) final;
};

class AvoidOptionalOperatorArrow : public ClangTidyCheck {
public:
    AvoidOptionalOperatorArrow(StringRef name, ClangTidyContext* context)
      : ClangTidyCheck(name, context) {}
    void registerMatchers(ast_matchers::MatchFinder*) final;
    void check(const ast_matchers::MatchFinder::MatchResult&) final;
};

} // namespace clang::tidy::redpanda
