// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "redpanda_eager_json_serialization_check.h"

#include <clang-tidy/ClangTidyCheck.h>
#include <clang/AST/ASTContext.h>
#include <clang/ASTMatchers/ASTMatchFinder.h>

namespace clang::tidy::redpanda {

using namespace clang::ast_matchers;

namespace {

// Mirrors the dispatch in seastar's json formatter: a type is serialized via
// the eager range overload iff it is an input_range that is not string-like.
// We approximate input_range as "has both begin() and end() members", which
// matches every container that reaches formatter::to_json's range overload.
AST_MATCHER(CXXRecordDecl, hasBeginAndEnd) {
    if (!Node.hasDefinition()) {
        return false;
    }
    bool has_begin = false;
    bool has_end = false;
    for (const auto* method : Node.methods()) {
        if (const auto* ident = method->getIdentifier()) {
            if (ident->isStr("begin")) {
                has_begin = true;
            } else if (ident->isStr("end")) {
                has_end = true;
            }
        }
    }
    return has_begin && has_end;
}

} // namespace

void EagerJsonSerialization::registerMatchers(MatchFinder* Finder) {
    // string-like types have begin()/end() too, but seastar serializes them
    // via the scalar path (internal::is_string_like), so they are exempt.
    auto string_like = cxxRecordDecl(hasAnyName(
      "::seastar::basic_sstring",
      "::std::basic_string",
      "::std::basic_string_view"));

    auto container_arg = hasType(hasUnqualifiedDesugaredType(recordType(
      hasDeclaration(cxxRecordDecl(hasBeginAndEnd(), unless(string_like))))));

    Finder->addMatcher(
      cxxConstructExpr(
        hasDeclaration(cxxConstructorDecl(
          isTemplateInstantiation(),
          ofClass(
            cxxRecordDecl(hasName("::seastar::json::json_return_type"))))),
        argumentCountIs(1),
        hasArgument(0, container_arg))
        .bind("ctor"),
      this);
}

void EagerJsonSerialization::check(const MatchFinder::MatchResult& Result) {
    const auto* Ctor = Result.Nodes.getNodeAs<CXXConstructExpr>("ctor");
    diag(
      Ctor->getBeginLoc(),
      "constructing json_return_type from a container serializes the whole "
      "range into one contiguous string, risking an oversized allocation; "
      "return ss::json::stream_range_as_array() instead to stream elements "
      "with bounded allocations");
}

} // namespace clang::tidy::redpanda
