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

#include "base/seastarx.h"

#include <seastar/core/sstring.hh>

#include <exception>
#include <stdexcept>
#include <utility>

class malformed_batch_stream_exception : public std::exception {
public:
    explicit malformed_batch_stream_exception(ss::sstring s)
      : _msg(std::move(s)) {}

    const char* what() const noexcept override { return _msg.c_str(); }

private:
    ss::sstring _msg;
};

class zero_segments_indexed_exception : public std::exception {
public:
    explicit zero_segments_indexed_exception(ss::sstring s)
      : _msg(std::move(s)) {}

    const char* what() const noexcept override { return _msg.c_str(); }

private:
    ss::sstring _msg;
};

class generation_id_mismatch_exception : public std::exception {
public:
    explicit generation_id_mismatch_exception(ss::sstring s)
      : _msg(std::move(s)) {}

    const char* what() const noexcept override { return _msg.c_str(); }

private:
    ss::sstring _msg;
};

/// Thrown by `offset_translator_state` when an offset to translate falls
/// outside the range currently covered by the translator state.
class translation_offset_out_of_range : public std::runtime_error {
public:
    explicit translation_offset_out_of_range(const ss::sstring& s)
      : std::runtime_error(s.c_str()) {}
};
