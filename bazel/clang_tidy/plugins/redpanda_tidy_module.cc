// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "redpanda_lambda_coroutine_deduces_this_check.h"
#include "redpanda_noop_check.h"

#include <clang-tidy/ClangTidyModule.h>

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
        check_factories.registerCheck<LambdaCoroutineDeducesThis>(
          "redpanda-lambda-coroutine-deduces-this");
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
