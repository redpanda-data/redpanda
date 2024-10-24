/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "ssl_utils.h"

#include "thirdparty/openssl/err.h"

namespace crypto::internal {
std::string ossl_error::build_error() {
    // Size is defined by OpenSSL documentatiion for ERR_error_string()
    // We are using ERR_error_string_n for safety purposes
    // https://www.openssl.org/docs/man3.0/man3/ERR_error_string_n.html
    static const size_t OPENSSL_ERROR_BUFFER_SIZE = 256;

    std::string msg = "{{";
    std::array<char, OPENSSL_ERROR_BUFFER_SIZE> buf{};
    for (auto code = ERR_get_error(); code != 0; code = ERR_get_error()) {
        ERR_error_string_n(code, buf.data(), buf.size());
        msg += fmt::format("{{{}: {}}}", code, buf.data());
    }
    msg += "}}";
    return msg;
}
} // namespace crypto::internal
