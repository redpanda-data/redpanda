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

#include "net/connection.h"

#include <gtest/gtest.h>

#include <cerrno>
#include <system_error>

namespace {

std::system_error make_system_error(int err) {
    return std::system_error(err, std::generic_category());
}

} // namespace

TEST(IsReconnectError, ReturnsTrue_ForENODATA) {
    auto e = make_system_error(ENODATA);
    EXPECT_TRUE(net::is_reconnect_error(e));
}

TEST(IsReconnectError, ReturnsTrue_ForKnownReconnectErrors) {
    for (auto err : {ECONNREFUSED,
                     ENETUNREACH,
                     ETIMEDOUT,
                     ECONNRESET,
                     ENOTCONN,
                     ECONNABORTED,
                     EAGAIN,
                     EPIPE,
                     EHOSTUNREACH,
                     EHOSTDOWN,
                     ENETRESET,
                     ENETDOWN,
                     ENODATA}) {
        auto e = make_system_error(err);
        EXPECT_TRUE(net::is_reconnect_error(e)) << "errno=" << err;
    }
}

TEST(IsReconnectError, ReturnsFalse_ForUnrelatedErrors) {
    auto e = make_system_error(EISDIR);
    EXPECT_FALSE(net::is_reconnect_error(e));
}
