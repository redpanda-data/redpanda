#include "redpanda_noop_check.h"
#include "redpanda_optional_check.h"

#include <clang-tidy/ClangTidyModule.h>
#include <clang-tidy/ClangTidyModuleRegistry.h>

namespace clang::tidy {

namespace redpanda {

/**
 * Parent clang-tidy module for redpanda custom checks.
 *
 * Write your custom check in a separate h/cc and register it here.
 *
 * See README.md for detail.
 *
 */
class RedpandaModule : public ClangTidyModule {
public:
    void addCheckFactories(ClangTidyCheckFactories& check_factories) override {
        // register checks here. for example:
        check_factories.registerCheck<NoopCheck>("redpanda-noop");
        check_factories.registerCheck<AvoidOptionalOperatorStar>(
          "redpanda-avoid-optional-op-star");
        check_factories.registerCheck<AvoidOptionalOperatorArrow>(
          "redpanda-avoid-optional-op-arrow");
    }

    // this is where you might set default options for any configurable checks
    // in the module
    ClangTidyOptions getModuleOptions() override {
        ClangTidyOptions options{};
        return options;
    }
};

// Register the module using this statically initialized variable.
static ClangTidyModuleRegistry::Add<RedpandaModule>
  X("redpanda-module", "Adds redpanda custom checks.");

} // namespace redpanda

volatile int RedpandaModuleAnchorSource = 0;

} // namespace clang::tidy
