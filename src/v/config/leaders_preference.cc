// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "config/leaders_preference.h"

#include <boost/algorithm/string.hpp>

namespace config {

std::ostream& operator<<(std::ostream& os, const leaders_preference& lp) {
    switch (lp.type) {
    case leaders_preference::type_t::none:
        os << "none";
        break;
    case leaders_preference::type_t::racks: {
        os << "racks:";
        bool first = true;
        for (const auto& r : lp.racks) {
            if (first) {
                first = false;
            } else {
                os << ",";
            }
            os << r;
        }
        break;
    }
    default:
        throw std::invalid_argument("unknown leaders_preference type");
    }
    return os;
}

leaders_preference leaders_preference::parse(std::string_view s) {
    static constexpr std::string_view racks_prefix = "racks:";
    if (s == "none") {
        return leaders_preference{};
    } else if (s.starts_with(racks_prefix)) {
        std::vector<ss::sstring> tokens;
        boost::algorithm::split(
          tokens, s.substr(racks_prefix.length()), [](char c) {
              return c == ',';
          });

        leaders_preference ret;
        ret.type = leaders_preference::type_t::racks;
        ret.racks.reserve(tokens.size());
        for (auto& tok : tokens) {
            boost::algorithm::trim(tok);
            if (tok.empty()) {
                throw std::runtime_error("couldn't parse leaders_preference: "
                                         "empty rack token");
            }
            ret.racks.emplace_back(std::move(tok));
        }
        ret.type = leaders_preference::type_t::racks;
        return ret;
    } else {
        throw std::runtime_error("couldn't parse leaders_preference: should be "
                                 "\"none\" or start with \"racks:\"");
    }
}

} // namespace config
