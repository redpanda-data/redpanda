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

#ifndef IS_GTEST
#define RPTEST_FAIL(m) BOOST_FAIL(m)
#define RPTEST_FAIL_CORO(m) BOOST_FAIL(m)
#else
#define RPTEST_FAIL(m) FAIL() << (m)
#define RPTEST_FAIL_CORO(m) ASSERT_TRUE_CORO(false) << (m)
#endif
