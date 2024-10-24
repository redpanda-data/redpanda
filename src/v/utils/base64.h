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
#include "bytes/bytes.h"
#include "bytes/iobuf.h"

#include <seastar/core/sstring.hh>

class base64_decoder_exception final : public std::exception {
public:
    const char* what() const noexcept final {
        return "error decoding base64 string";
    }
};

class base64_url_decoder_exception final : public std::exception {
public:
    const char* what() const noexcept final {
        return "error decoding base64url string";
    }
};

// base64 <-> bytes
bytes base64_to_bytes(std::string_view);
ss::sstring bytes_to_base64(bytes_view);

// base64 -> string
ss::sstring base64_to_string(std::string_view);

// base64 -> iobuf
iobuf base64_to_iobuf(const iobuf&);

// base64 <-> iobuf
ss::sstring iobuf_to_base64(const iobuf&);

/// \brief Used to decode URL encoded base64 values
///
/// URL encoded base64 values use '-' and '_' instead of
/// '+' and '/', respectively
/// \throws base64_url_decoder_exception if an invalid URL base64 encoded string
/// is provided
bytes base64url_to_bytes(std::string_view);
