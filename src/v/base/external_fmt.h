/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

/// Formatters for commonly-used external / third-party types that have
/// operator<< but no fmt::formatter specialization.  Include this header
/// whenever you need to format one of these types with {fmt}.

#include <boost/beast/http/status.hpp>
#include <boost/beast/http/verb.hpp>
#include <boost/filesystem/path.hpp>
#include <boost/system/error_code.hpp>
#include <fmt/ostream.h>

template<>
struct fmt::formatter<boost::beast::http::status> : fmt::ostream_formatter {};

template<>
struct fmt::formatter<boost::beast::http::verb> : fmt::ostream_formatter {};

template<>
struct fmt::formatter<boost::system::error_code> : fmt::ostream_formatter {};

template<>
struct fmt::formatter<boost::filesystem::path> : fmt::ostream_formatter {};
