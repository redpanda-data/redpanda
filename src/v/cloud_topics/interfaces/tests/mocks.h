// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cloud_topics/interfaces/cluster_partition_manager.h"
#include "container/fragmented_vector.h"
#include "gmock/gmock.h"
#include "model/fundamental.h"
#include "model/record_batch_reader.h"

#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/seastar.hh>

#include <gmock/gmock.h>

#include <exception>

namespace seastar {
// Print seastar strings in gmock error messages
template<typename Ch, typename Size, Size max_size, bool null_terminate>
void PrintTo(
  const basic_sstring<Ch, Size, max_size, null_terminate>& s, std::ostream* o) {
    *o << s;
}
// Print ss::shared_ptr in gmock error messages
inline void PrintTo(
  const shared_ptr<::experimental::cloud_topics::cluster_partition_api>&,
  std::ostream* o) {
    *o << "cluster_partition_api_mock";
}
inline void PrintTo(
  const shared_ptr<
    ::experimental::cloud_topics::cluster_partition_manager_api>&,
  std::ostream* o) {
    *o << "cluster_partition_manager_api_mock";
}
} // namespace seastar

class partition_mock final
  : public experimental::cloud_topics::cluster_partition_api {
public:
    MOCK_METHOD(
      ss::future<fragmented_vector<model::tx_range>>,
      aborted_transactions,
      (model::offset base, model::offset last),
      (const override));

    MOCK_METHOD(
      ss::future<model::record_batch_reader>,
      make_reader,
      (storage::log_reader_config config,
       std::optional<model::timeout_clock::time_point> debounce_deadline),
      (override));

    void expect_aborted_transactions(
      model::offset base,
      model::offset last,
      fragmented_vector<model::tx_range> result) {
        auto fut = ss::make_ready_future<fragmented_vector<model::tx_range>>(
          std::move(result));
        EXPECT_CALL(*this, aborted_transactions(base, last))
          .Times(1)
          .WillOnce(::testing::Return(std::move(fut)));
    }

    void expect_aborted_transactions_fail(
      model::offset base, model::offset last, std::exception_ptr e) {
        auto fut
          = ss::make_exception_future<fragmented_vector<model::tx_range>>(
            std::move(e));
        EXPECT_CALL(*this, aborted_transactions(base, last))
          .Times(1)
          .WillOnce(::testing::Return(std::move(fut)));
    }

    void expect_make_reader(model::record_batch_reader result) {
        auto fut = ss::make_ready_future<model::record_batch_reader>(
          std::move(result));
        EXPECT_CALL(*this, make_reader(::testing::_, ::testing::_))
          .Times(1)
          .WillOnce(::testing::Return(std::move(fut)));
    }

    void expect_make_reader_fail(std::exception_ptr result) {
        auto fut = ss::make_exception_future<model::record_batch_reader>(
          std::move(result));
        EXPECT_CALL(*this, make_reader(::testing::_, ::testing::_))
          .Times(1)
          .WillOnce(::testing::Return(std::move(fut)));
    }
};

class partition_manager_mock
  : public experimental::cloud_topics::cluster_partition_manager_api {
public:
    MOCK_METHOD(
      ss::shared_ptr<experimental::cloud_topics::cluster_partition_api>,
      get_partition,
      (const model::ntp&),
      (override));

    void expect_get_partition(
      const model::ntp& expected_ntp,
      ss::shared_ptr<experimental::cloud_topics::cluster_partition_api>
        result) {
        EXPECT_CALL(*this, get_partition(expected_ntp))
          .Times(1)
          .WillOnce(::testing::Return(std::move(result)));
    }

    template<class Exception>
    void
    expect_get_partition_fail(const model::ntp& expected_ntp, Exception err) {
        EXPECT_CALL(*this, get_partition(expected_ntp))
          .Times(1)
          .WillOnce(::testing::Throw(err));
    }
};
