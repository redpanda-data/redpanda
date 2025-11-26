#include "redpanda_optional_check.h"

#include <clang/Lex/Lexer.h>
#include <fmt/format.h>

namespace clang::tidy::redpanda {

void AvoidOptionalOperatorStar::registerMatchers(
  ast_matchers::MatchFinder* finder) {
    if (!getLangOpts().CPlusPlus) {
        return;
    }
    using namespace ast_matchers;

    finder->addMatcher(
      cxxOperatorCallExpr(
        hasOverloadedOperatorName("*"),
        callee(cxxMethodDecl(ofClass(hasName("::std::optional")))))
        .bind("starOperatorCall"),
      this);
}

void AvoidOptionalOperatorStar::check(
  const ast_matchers::MatchFinder::MatchResult& result) {
    const auto* call = result.Nodes.getNodeAs<CXXOperatorCallExpr>(
      "starOperatorCall");
    if (call == nullptr) {
        return;
    }
    const auto* operand = call->getArg(0);
    auto range = clang::CharSourceRange::getTokenRange(
      operand->getSourceRange());
    auto op_text = clang::Lexer::getSourceText(
      range, *result.SourceManager, result.Context->getLangOpts());
    std::string replace = fmt::format("{}.value()", llvm::Twine(op_text).str());

    diag(
      call->getBeginLoc(),
      "bug-prone use of unchecked optional::operator*, prefer checked access "
      "through optional::value()")
      << FixItHint::CreateReplacement(call->getSourceRange(), replace);
}

void AvoidOptionalOperatorArrow::registerMatchers(
  ast_matchers::MatchFinder* finder) {
    if (!getLangOpts().CPlusPlus) {
        return;
    }
    using namespace ast_matchers;

    finder->addMatcher(
      cxxOperatorCallExpr(
        hasOverloadedOperatorName("->"),
        callee(cxxMethodDecl(ofClass(hasName("::std::optional")))))
        .bind("arrowOperatorCall"),
      this);
}

void AvoidOptionalOperatorArrow::check(
  const ast_matchers::MatchFinder::MatchResult& result) {
    const auto* call = result.Nodes.getNodeAs<CXXOperatorCallExpr>(
      "arrowOperatorCall");
    if (call == nullptr) {
        return;
    }
    const auto* operand = call->getArg(0);
    auto range = clang::CharSourceRange::getTokenRange(
      operand->getSourceRange());
    auto op_text = clang::Lexer::getSourceText(
      range, *result.SourceManager, result.Context->getLangOpts());

    std::string replace = fmt::format(
      "{}.value().", llvm::Twine(op_text).str());
    diag(
      call->getExprLoc(),
      "bug-prone use of unchecked optional::operator->, prefer checked access "
      "through optional::value().")
      << FixItHint::CreateReplacement(call->getSourceRange(), replace);
}

} // namespace clang::tidy::redpanda
