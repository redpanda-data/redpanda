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

#include "seastarx.h"

#include <seastar/http/request.hh>

#pragma once

namespace admin {

/**
 * Helper for decoding path parameters.
 *
 * It is a slightly modified version of admin::path_decode
 * which only differs in that it correctly does not replace +
 * with ' ' in the input. It only replaces percent encoded values.
 */
bool path_decode(const std::string_view in, ss::sstring& out);

} // namespace admin
