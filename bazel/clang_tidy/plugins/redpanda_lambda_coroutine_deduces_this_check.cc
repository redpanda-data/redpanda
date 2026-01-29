// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "redpanda_lambda_coroutine_deduces_this_check.h"

#include <clang-tidy/ClangTidyCheck.h>
#include <clang/AST/ASTContext.h>
#include <clang/ASTMatchers/ASTMatchFinder.h>

namespace clang::tidy::redpanda {

using namespace clang::ast_matchers;

namespace {

AST_MATCHER(LambdaExpr, hasCoroutineBody) {
    const Stmt* Body = Node.getBody();
    return Body != nullptr && CoroutineBodyStmt::classof(Body);
}

AST_MATCHER(LambdaExpr, invalidCaptures) {
    auto empty_captures = Node.capture_size() == 0U;
    if (empty_captures) {
        // Empty capture clause is okay.
        return false;
    } else {
        // Non-empty capture clause requires deducing this to properly extend
        // lifetimes of captured elements.
        // https://github.com/scylladb/seastar/blob/master/doc/lambda-coroutine-fiasco.md#solution-c23-and-up
        const auto* Call = Node.getCallOperator();
        bool deduces_this = Call->isExplicitObjectMemberFunction();
        return !deduces_this;
    }
}

} // namespace

void LambdaCoroutineDeducesThis::registerMatchers(MatchFinder* Finder) {
    Finder->addMatcher(
      lambdaExpr(hasCoroutineBody(), invalidCaptures()).bind("lambda"), this);
}

bool LambdaCoroutineDeducesThis::isLanguageVersionSupported(
  const LangOptions& LangOpts) const {
    // Deducing this is only supported in C++23 and beyond.
    return LangOpts.CPlusPlus23;
}

void LambdaCoroutineDeducesThis::check(const MatchFinder::MatchResult& Result) {
    const auto* MatchedLambda = Result.Nodes.getNodeAs<LambdaExpr>("lambda");
    SourceLocation insert_loc;
    std::string replace;
    auto* Call = MatchedLambda->getCallOperator();
    bool has_params = !Call->param_empty();

    if (has_params) {
        // Lambda already has parameters: insert 'this auto,' before the first
        // parameter
        const ParmVarDecl* first_param = Call->parameters().front();
        insert_loc = first_param->getBeginLoc();
        replace = "this auto, ";
    } else {
        // Lambda has no parameters and may not even have a parameter list:
        // insert either a new argument list ('(this auto)') or an argument
        // `this auto`.
        auto end_of_introducer_range
          = MatchedLambda->getIntroducerRange().getEnd();
        auto maybe_paren_loc = end_of_introducer_range.getLocWithOffset(1);
        auto* SM = Result.SourceManager;
        if (maybe_paren_loc.isValid()) {
            const char* token_start = SM->getCharacterData(maybe_paren_loc);
            if (*token_start == '(') {
                insert_loc = end_of_introducer_range.getLocWithOffset(2);
                replace = "this auto";
            } else {
                insert_loc = maybe_paren_loc;
                replace = "(this auto)";
            }
        }
    }

    diag(
      MatchedLambda->getExprLoc(),
      "coroutine lambda may cause use-after-free, avoid captures or pass "
      "`this` as first parameter to lambda arguments")
      << FixItHint::CreateInsertion(insert_loc, replace);
}

} // namespace clang::tidy::redpanda
