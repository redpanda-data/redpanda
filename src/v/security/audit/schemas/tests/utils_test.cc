/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "kafka/protocol/messages.h"
#include "security/audit/schemas/utils.h"

#include <gtest/gtest.h>

template<typename T>
class KafkaToOcsfTest : public ::testing::Test {
public:
    kafka::api_key api_key = T::key;
};

template<typename T>
struct ToGTestTypes;

template<template<typename...> typename ListTemplate, typename... Ts>
struct ToGTestTypes<ListTemplate<Ts...>> {
    using type = ::testing::Types<Ts...>;
};

template<typename T>
using ToGTestTypes_T = typename ToGTestTypes<T>::type;

using test_types = ToGTestTypes_T<kafka::request_types>;

TYPED_TEST_SUITE(KafkaToOcsfTest, test_types);

TYPED_TEST(KafkaToOcsfTest, EnsureAllAPIsMapped) {
    EXPECT_NE(
      security::audit::kafka_api_to_event_type(this->api_key),
      security::audit::event_type::unknown);
}
