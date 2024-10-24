// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/rm_stm.h"
#include "model/record.h"

#include <seastar/testing/thread_test_case.hh>

#include <boost/test/unit_test.hpp>

#include <optional>

namespace cluster {

static ss::logger logger{"rm_stm_compatibility-test"};

bool compare_batch(
  tx::fence_batch_data batch_data,
  model::producer_identity pid,
  std::optional<model::tx_seq> tx_seq = std::nullopt,
  std::optional<std::chrono::milliseconds> transaction_timeout_ms
  = std::nullopt,
  model::partition_id tm = model::partition_id(0)) {
    if (batch_data.bid.pid != pid) {
        vlog(
          logger.error,
          "Invalid batch pid expected: {} current: {}",
          pid,
          batch_data.bid.pid);
        return false;
    }
    if (batch_data.tx_seq != tx_seq) {
        vlog(
          logger.error,
          "Invalid batch tx_seq expected: {} current: {}",
          tx_seq,
          batch_data.tx_seq);
        return false;
    }
    if (batch_data.transaction_timeout_ms != transaction_timeout_ms) {
        vlog(
          logger.error,
          "Invalid batch transaction_timeout_ms expected: {} current: {}",
          transaction_timeout_ms,
          batch_data.transaction_timeout_ms);
        return false;
    }
    if (batch_data.tm != tm) {
        vlog(
          logger.error,
          "Invalid batch tm expected: {} current: {}",
          tm,
          batch_data.tm);
        return false;
    }
    return true;
}

model::record_batch make_fence_batch_v1(
  model::producer_identity pid,
  model::tx_seq tx_seq,
  std::chrono::milliseconds transaction_timeout_ms) {
    iobuf key;
    auto pid_id = pid.id;
    reflection::serialize(key, model::record_batch_type::tx_fence, pid_id);

    iobuf value;
    reflection::serialize(
      value,
      tx::fence_control_record_v1_version,
      tx_seq,
      transaction_timeout_ms);

    storage::record_batch_builder builder(
      model::record_batch_type::tx_fence, model::offset(0));
    builder.set_producer_identity(pid.id, pid.epoch);
    builder.set_control_type();
    builder.add_raw_kv(std::move(key), std::move(value));

    return std::move(builder).build();
}

SEASTAR_THREAD_TEST_CASE(fence_batch_compatibility) {
    vlog(logger.info, "Test fence_batch_v1");
    model::producer_identity pid1{3, 4};
    model::tx_seq tx_seq_1{100};
    std::chrono::milliseconds transaction_timeout_ms_1{200};
    auto batch_v1 = make_fence_batch_v1(
      pid1, tx_seq_1, transaction_timeout_ms_1);
    auto batch_data_v1 = tx::read_fence_batch(std::move(batch_v1));
    BOOST_REQUIRE(
      compare_batch(batch_data_v1, pid1, tx_seq_1, transaction_timeout_ms_1));

    vlog(logger.info, "Test fence_batch_v2");
    model::producer_identity pid2{5, 6};
    model::tx_seq tx_seq_2{300};
    std::chrono::milliseconds transaction_timeout_ms_2{400};
    model::partition_id tm_2{5};
    auto batch_v2 = tx::make_fence_batch(
      pid2, tx_seq_2, transaction_timeout_ms_2, tm_2);
    auto batch_data_v2 = tx::read_fence_batch(std::move(batch_v2));
    BOOST_REQUIRE(compare_batch(
      batch_data_v2, pid2, tx_seq_2, transaction_timeout_ms_2, tm_2));
}

} // namespace cluster
