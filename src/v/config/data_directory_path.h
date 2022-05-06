/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "seastarx.h"

#include <seastar/core/sstring.hh>

#include <fmt/format.h>
#include <yaml-cpp/yaml.h>

#include <cstdlib>
#include <filesystem>

namespace config {

struct data_directory_path {
    std::filesystem::path path;
    ss::sstring as_sstring() const { return path.string(); }

    friend bool
    operator==(const data_directory_path&, const data_directory_path&)
      = default;

    friend std::ostream&
    operator<<(std::ostream& o, const config::data_directory_path& p) {
        return o << "{data_directory=" << p.path << "}";
    }
};

} // namespace config

namespace YAML {
template<>
struct convert<config::data_directory_path> {
    using type = config::data_directory_path;
    static Node encode(const type& rhs) {
        Node node;
        node = rhs.path.c_str();
        return node;
    }
    static bool decode(const Node& node, type& rhs) {
        auto pstr = node.as<std::string>();
        if (pstr[0] == '~') {
            const char* home = std::getenv("HOME");
            if (!home) {
                return false;
            }
            pstr = fmt::format("{}{}", home, pstr.erase(0, 1));
        }
        rhs.path = pstr;
        return true;
    }
};
} // namespace YAML