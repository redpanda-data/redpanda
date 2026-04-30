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

#include "cluster/sloth_mail/tests/backend_fixture.h"
#include "cluster/sloth_mail/tests/config.h"
#include "container/chunked_vector.h"
#include "model/fundamental.h"
#include "test_utils/test.h"

#include <sys/select.h>

#include <cstddef>
#include <unordered_set>
#include <variant>

using namespace cluster::sloth_mail::tests;

TEST_F_CORO(backend_fixture, test_overfill) {
    add_node(model::node_id{1});
    add_node(model::node_id{2});

    auto test_start = ss::lowres_clock::now();

    // 9 requests should be buffered, nothing shipped yet
    for (int i = 0; i < 9; i++) {
        backend_instance.dispatch<kinds::test_kind_0>(
          model::node_id{1},
          kinds::test_kind_0::key_t{.key = i},
          kinds::test_kind_0::value_t{.value = fmt::format("v{}", i)},
          test_start + 1h);
    }
    auto requests_shipped = shipper.detach_recent_requests();
    GTEST_ASSERT_EQ_CORO(requests_shipped.size(), 0);

    // dispatching with existing key won't trigger overfill
    backend_instance.dispatch<kinds::test_kind_0>(
      model::node_id{1},
      kinds::test_kind_0::key_t{.key = 5},
      kinds::test_kind_0::value_t{.value = "v5n"},
      test_start + 1h);
    requests_shipped = shipper.detach_recent_requests();
    GTEST_ASSERT_EQ_CORO(requests_shipped.size(), 0);

    // dispatching to a different node won't trigger overfill
    backend_instance.dispatch<kinds::test_kind_0>(
      model::node_id{2},
      kinds::test_kind_0::key_t{.key = 1},
      kinds::test_kind_0::value_t{.value = "v1"},
      test_start + 1h);
    requests_shipped = shipper.detach_recent_requests();
    GTEST_ASSERT_EQ_CORO(requests_shipped.size(), 0);

    // now overfill, should trigger dispatch
    backend_instance.dispatch<kinds::test_kind_0>(
      model::node_id{1},
      kinds::test_kind_0::key_t{.key = 9},
      kinds::test_kind_0::value_t{.value = "v9"},
      test_start + 1h);
    requests_shipped = shipper.detach_recent_requests();
    GTEST_ASSERT_EQ_CORO(requests_shipped.size(), 1);
    const auto& request = requests_shipped[0];
    GTEST_ASSERT_EQ_CORO(request.destination, model::node_id{1});
    GTEST_ASSERT_EQ_CORO(request.data.data.size(), 1);
    const auto& kind_and_data = *request.data.data.begin();
    GTEST_ASSERT_EQ_CORO(kind_and_data.first, kinds::test_kind_0::id);
    validate_pairs(
      std::get<pairs_vector_of_kind<kinds::test_kind_0>>(kind_and_data.second),
      {{.key = {.key = 0}, .value = {.value = "v0"}},
       {.key = {.key = 1}, .value = {.value = "v1"}},
       {.key = {.key = 2}, .value = {.value = "v2"}},
       {.key = {.key = 3}, .value = {.value = "v3"}},
       {.key = {.key = 4}, .value = {.value = "v4"}},
       {.key = {.key = 5}, .value = {.value = "v5 and v5n"}},
       {.key = {.key = 6}, .value = {.value = "v6"}},
       {.key = {.key = 7}, .value = {.value = "v7"}},
       {.key = {.key = 8}, .value = {.value = "v8"}},
       {.key = {.key = 9}, .value = {.value = "v9"}}});

    // just in case make sure no more requests have been shipped
    requests_shipped = shipper.detach_recent_requests();
    GTEST_ASSERT_EQ_CORO(requests_shipped.size(), 0);
}

TEST_F_CORO(backend_fixture, test_deadline) {
    add_node(model::node_id{1});
    add_node(model::node_id{2});

    auto test_start = ss::lowres_clock::now();
    backend_instance.dispatch<kinds::test_kind_0>(
      model::node_id{1},
      kinds::test_kind_0::key_t{.key = 1},
      kinds::test_kind_0::value_t{.value = "v1"},
      test_start + 1h);
    backend_instance.dispatch<kinds::test_kind_1>(
      model::node_id{1},
      kinds::test_kind_1::key_t{.key = "k1"},
      kinds::test_kind_1::value_t{.value = 1},
      test_start + 1h);
    backend_instance.dispatch<kinds::test_kind_0>(
      model::node_id{2},
      kinds::test_kind_0::key_t{.key = 1},
      kinds::test_kind_0::value_t{.value = "v1"},
      test_start + 1h);
    co_await ss::sleep(50ms);
    // nothing shipped yet
    auto requests_shipped = shipper.detach_recent_requests();
    GTEST_ASSERT_EQ_CORO(requests_shipped.size(), 0);

    // dispatch to node 1 with near deadline should trigger shipping
    backend_instance.dispatch<kinds::test_kind_0>(
      model::node_id{1},
      kinds::test_kind_0::key_t{.key = 2},
      kinds::test_kind_0::value_t{.value = "v2"},
      ss::lowres_clock::now() + 10ms);
    co_await ss::sleep(50ms);
    requests_shipped = shipper.detach_recent_requests();
    GTEST_ASSERT_EQ_CORO(requests_shipped.size(), 1);
    GTEST_ASSERT_EQ_CORO(requests_shipped[0].destination, model::node_id{1});
    const auto& request_data = requests_shipped[0].data.data;
    GTEST_ASSERT_EQ_CORO(request_data.size(), 2);
    auto it = std::ranges::find_if(request_data, [](const auto& pair) {
        return pair.first == kinds::test_kind_0::id;
    });
    GTEST_ASSERT_NE_CORO(it, request_data.end());
    validate_pairs(
      std::get<pairs_vector_of_kind<kinds::test_kind_0>>(it->second),
      {{.key = {.key = 1}, .value = {.value = "v1"}},
       {.key = {.key = 2}, .value = {.value = "v2"}}});
    it = std::ranges::find_if(request_data, [](const auto& pair) {
        return pair.first == kinds::test_kind_1::id;
    });
    GTEST_ASSERT_NE_CORO(it, request_data.end());
    validate_pairs(
      std::get<pairs_vector_of_kind<kinds::test_kind_1>>(it->second),
      {{.key = {.key = "k1"}, .value = {.value = 1}}});

    // just in case make sure no more requests have been shipped
    requests_shipped = shipper.detach_recent_requests();
    GTEST_ASSERT_EQ_CORO(requests_shipped.size(), 0);
}

TEST_F_CORO(backend_fixture, test_recipient_absent) {
    // no node 1
    backend_instance.dispatch<kinds::test_kind_0>(
      model::node_id{1},
      kinds::test_kind_0::key_t{.key = 1},
      kinds::test_kind_0::value_t{.value = "v1"},
      ss::lowres_clock::now());

    co_await ss::sleep(10ms);
    // nothing shipped
    auto requests_shipped = shipper.detach_recent_requests();
    GTEST_ASSERT_EQ_CORO(requests_shipped.size(), 0);
}

TEST_F_CORO(backend_fixture, test_recipient_gone) {
    add_node(model::node_id{1});

    backend_instance.dispatch<kinds::test_kind_0>(
      model::node_id{1},
      kinds::test_kind_0::key_t{.key = 1},
      kinds::test_kind_0::value_t{.value = "v1"},
      ss::lowres_clock::now() + 10ms);

    remove_node(model::node_id{1});

    // backend will fail to ship because node is gone
    co_await ss::sleep(50ms);

    // nothing shipped
    auto requests_shipped = shipper.detach_recent_requests();
    GTEST_ASSERT_EQ_CORO(requests_shipped.size(), 0);
}

TEST_F_CORO(backend_fixture, test_deadline_while_retrying) {
    add_node(model::node_id{1});

    backend_instance.dispatch<kinds::test_kind_0>(
      model::node_id{1},
      kinds::test_kind_0::key_t{.key = 0},
      kinds::test_kind_0::value_t{.value = "v0"},
      ss::lowres_clock::now() + 10ms);

    // in 10ms backend will fail to ship and will retry in 100ms
    shipper.fail_next_n_requests(1);
    co_await ss::sleep(20ms);

    // nothing shipped
    auto requests_shipped = shipper.detach_recent_requests();
    GTEST_ASSERT_EQ_CORO(requests_shipped.size(), 0);

    // add one more request with near deadline to trigger shipping while the
    // old one waits to retry
    backend_instance.dispatch<kinds::test_kind_0>(
      model::node_id{1},
      kinds::test_kind_0::key_t{.key = 1},
      kinds::test_kind_0::value_t{.value = "v1"},
      ss::lowres_clock::now() + 10ms);

    // enough for both to complete
    co_await ss::sleep(150ms);

    requests_shipped = shipper.detach_recent_requests();
    GTEST_ASSERT_EQ_CORO(requests_shipped.size(), 2);
    for (auto i : std::views::iota(0, 2)) {
        GTEST_ASSERT_EQ_CORO(
          requests_shipped[i].destination, model::node_id{1});
        GTEST_ASSERT_EQ_CORO(requests_shipped[i].data.data.size(), 1);
        const auto& kind_and_data = *requests_shipped[i].data.data.begin();
        GTEST_ASSERT_EQ_CORO(kind_and_data.first, kinds::test_kind_0::id);
        validate_pairs(
          std::get<pairs_vector_of_kind<kinds::test_kind_0>>(
            kind_and_data.second),
          {{.key = {.key = i}, .value = {.value = fmt::format("v{}", i)}}});
    }
}
