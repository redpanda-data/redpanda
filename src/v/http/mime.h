/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include <map>
#include <string>

namespace http {

// Media type is documented in the following RFC
// https://datatracker.ietf.org/doc/html/rfc2045#section-5.1
//
// But we implement only a subset defined in
// https://www.rfc-editor.org/rfc/rfc9110

struct media_type {
    std::string type;
    std::map<std::string, std::string> params;
};

std::string format_media_type(const media_type& m);

media_type parse_media_type(const std::string_view in);

} // namespace http
