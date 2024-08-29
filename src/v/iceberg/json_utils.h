// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "json/document.h"

namespace iceberg {

std::optional<std::reference_wrapper<const json::Value>>
parse_optional(const json::Value& v, const char* member_name);

const json::Value&
parse_required(const json::Value& v, const char* member_name);

ss::sstring parse_required_str(const json::Value& v, const char* member_name);

int32_t parse_required_i32(const json::Value& v, const char* member_name);
int64_t parse_required_i64(const json::Value& v, const char* member_name);

bool parse_required_bool(const json::Value& v, const char* member_name);

} // namespace iceberg
