/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "base/seastarx.h"
#include "json/document.h"

#include <seastar/core/future.hh>

#include <filesystem>

namespace compat {

ss::future<> write_corpus(const std::filesystem::path&);
ss::future<json::Document> parse_type(const std::filesystem::path&);
ss::future<> check_type(json::Document);

inline ss::future<> check_type(const std::filesystem::path& path) {
    return parse_type(path).then(
      [](auto doc) { return check_type(std::move(doc)); });
}

} // namespace compat
