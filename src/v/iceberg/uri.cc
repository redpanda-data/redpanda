// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "iceberg/uri.h"

#include "ssx/sformat.h"

#include <regex>

namespace iceberg {
namespace {
static std::regex uri_pattern{R"(^(.*):\/\/(.*?)\/(.*)$)"};
}

std::filesystem::path path_from_uri(const uri& u) {
    std::cmatch match;
    std::regex_match(u().data(), match, uri_pattern);
    if (match.ready() && match.empty()) {
        throw std::invalid_argument(fmt::format("Malformed URI: {}", u));
    }
    return std::filesystem::path(match[3]);
}

uri make_uri(
  const ss::sstring& bucket,
  const std::filesystem::path& path,
  std::string_view scheme) {
    return uri(ssx::sformat("{}://{}/{}", scheme, bucket, path.native()));
}

}; // namespace iceberg
