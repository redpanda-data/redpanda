// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "utils/variant.h"

#include <gtest/gtest.h>

#include <string>
#include <type_traits>
#include <variant>

static_assert(std::is_same_v<
              extend_variant_t<std::variant<int, double>, bool>,
              std::variant<int, double, bool>>);

static_assert(std::is_same_v<
              extend_variant_t<std::variant<int>, double, std::string>,
              std::variant<int, double, std::string>>);

static_assert(std::is_same_v<
              extend_variant_t<std::variant<int, double>>,
              std::variant<int, double>>);

TEST(VariantTest, CompileOnly) {}
