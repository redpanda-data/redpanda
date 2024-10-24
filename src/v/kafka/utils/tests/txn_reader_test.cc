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

#include "bytes/iobuf.h"
#include "gmock/gmock.h"
#include "kafka/utils/txn_reader.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/record_batch_types.h"
#include "model/tests/random_batch.h"
#include "model/timeout_clock.h"
#include "random/generators.h"
#include "storage/record_batch_builder.h"

#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/circular_buffer.hh>
#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/util/variant_utils.hh>

#include <absl/algorithm/container.h>
#include <absl/container/btree_map.h>
#include <absl/container/flat_hash_map.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace kafka {
namespace {

struct begin_txn {};
struct data_batch {};
struct commit_txn {};
struct abort_txn {};

using log_data = std::variant<begin_txn, data_batch, commit_txn, abort_txn>;

struct pid {
    int64_t id;
    int16_t epoch = 0;

    auto operator<=>(const pid&) const = default;
};

struct producer {
    pid pid;
    std::vector<log_data> data;
};

class predetermined_aborted_transaction_tracker
  : public aborted_transaction_tracker {
public:
    explicit predetermined_aborted_transaction_tracker(
      std::vector<model::tx_range> aborted)
      : _aborted(std::move(aborted)) {}

    ss::future<std::vector<model::tx_range>>
    compute_aborted_transactions(model::offset base, model::offset max) final {
        std::vector<model::tx_range> ranges;
        std::copy_if(
          _aborted.begin(),
          _aborted.end(),
          std::back_inserter(ranges),
          [base, max](model::tx_range range) {
              return range.first <= max && range.last >= base;
          });
        co_return ranges;
    }

private:
    std::vector<model::tx_range> _aborted;
};

struct aborted_txn_range {
    pid p;
    model::offset start;
    model::offset end;
};

std::unique_ptr<predetermined_aborted_transaction_tracker>
make_aborted_txns(const std::vector<aborted_txn_range>& aborts) {
    std::vector<model::tx_range> tracked;
    tracked.reserve(aborts.size());
    for (const auto& aborted : aborts) {
        tracked.emplace_back(
          model::producer_identity(aborted.p.id, aborted.p.epoch),
          aborted.start,
          aborted.end);
    }
    return std::make_unique<predetermined_aborted_transaction_tracker>(tracked);
}

model::record_batch_reader
make_reader(const std::initializer_list<
            std::reference_wrapper<const model::record_batch>>& batches) {
    ss::circular_buffer<model::record_batch> buffer;
    for (const auto& b : batches) {
        buffer.push_back(b.get().copy());
    }
    return model::make_memory_record_batch_reader(std::move(buffer));
}

std::vector<model::record_batch>
read_to_vector(model::record_batch_reader reader) {
    std::vector<model::record_batch> v;
    auto read = model::consume_reader_to_memory(
                  std::move(reader), model::no_timeout)
                  .get();
    std::copy(
      std::move_iterator(read.begin()),
      std::move_iterator(read.end()),
      std::back_inserter(v));
    return v;
}

struct test_case {
    std::string name;
    std::vector<producer> producers;
};

class TransactionReaderTest : public testing::Test {
public:
    std::unique_ptr<aborted_transaction_tracker>
    make_aborted_transaction_tracker() {
        return std::make_unique<predetermined_aborted_transaction_tracker>(
          _aborted);
    }

    template<typename T, typename... Args>
    auto create_batches(pid p, T head, Args... rest) {
        if constexpr (std::is_same_v<T, begin_txn>) {
            make_batch(p, head);
            return create_batches<Args...>(p, std::forward<Args>(rest)...);
        } else {
            auto b = std::make_tuple(make_batch(p, head));
            if constexpr (sizeof...(Args) == 0) {
                vassert(
                  _pending_txns.empty(),
                  "pending transactions are still outstanding");
                return std::move(b);
            } else {
                return std::tuple_cat(
                  std::move(b),
                  create_batches<Args...>(p, std::forward<Args>(rest)...));
            }
        }
    }

    model::record_batch_reader
    construct_log_reader(std::vector<producer> producers) {
        // Reverse the log data so we can pop_back
        for (producer& p : producers) {
            std::reverse(p.data.begin(), p.data.end());
        }
        ss::chunked_fifo<model::record_batch_reader::data_t> full_log;
        while (!producers.empty()) {
            constexpr static int max_batches = 10;
            auto batch_size = random_generators::get_int<size_t>(
              1, max_batches);
            ss::circular_buffer<model::record_batch> batches;
            while (batches.size() < batch_size && !producers.empty()) {
                auto& selected = random_generators::random_choice(producers);
                auto batch = make_batch(selected.pid, selected.data.back());
                if (batch) {
                    batches.push_back(std::move(*batch));
                }
                selected.data.pop_back();
                std::erase_if(
                  producers, [](const producer& p) { return p.data.empty(); });
            }
            if (!batches.empty()) {
                full_log.emplace_back(std::move(batches));
            }
        }
        vassert(
          _pending_txns.empty(), "pending transactions are still outstanding");
        return model::make_generating_record_batch_reader(
          [log = std::move(full_log)]() mutable {
              if (log.empty()) {
                  return ss::make_ready_future<
                    model::record_batch_reader::data_t>();
              }
              auto front = std::move(log.front());
              log.pop_front();
              return ss::make_ready_future<model::record_batch_reader::data_t>(
                std::move(front));
          });
    }

    std::vector<model::record_batch> committed_batches() const {
        std::vector<model::record_batch> copy;
        copy.reserve(_committed_log.size());
        for (const auto& batch : _committed_log) {
            copy.push_back(batch.copy());
        }
        return copy;
    }

private:
    struct pending_txn {
        model::offset start;
        std::vector<model::record_batch> batches;
    };

    std::optional<model::record_batch> make_batch(pid p, log_data d) {
        return std::visit(
          [this, p](auto d) -> std::optional<model::record_batch> {
              return make_batch(p, d);
          },
          d);
    }
    std::optional<model::record_batch> make_batch(pid p, begin_txn) {
        auto [_, inserted] = _pending_txns.emplace(p, _current_offset);
        vassert(inserted, "invalid nested txn");
        // There are no records associated with starting a txn
        return std::nullopt;
    }
    model::record_batch make_batch(pid p, data_batch) {
        constexpr static int max_batch_size = 8;
        auto batch = model::test::make_random_batch(
          model::test::record_batch_spec{
            .offset = model::next_offset(_current_offset),
            .count = random_generators::get_int(1, max_batch_size),
            .enable_idempotence = true,
            .producer_id = p.id,
            .producer_epoch = p.epoch,
            .base_sequence = _pid_sequences[p] + 1,
            .is_transactional = _pending_txns.contains(p),
          });
        // increment the seqno for the pid
        _pid_sequences[p] = batch.header().base_sequence
                            + batch.header().last_offset_delta;
        _current_offset = batch.header().last_offset();
        auto it = _pending_txns.find(p);
        if (it != _pending_txns.end()) {
            it->second.batches.push_back(batch.copy());
        }
        _committed_log.push_back(batch.copy());
        return batch;
    }

    model::record_batch make_batch(pid p, commit_txn) {
        vassert(_pending_txns.erase(p) > 0, "invalid commit without begin");
        iobuf key;
        key.append("\x00\x00\x00\x00", 4);
        return make_control_batch(p, std::move(key));
    }
    model::record_batch make_batch(pid p, abort_txn) {
        auto pending = _pending_txns.extract(p);
        vassert(!pending.empty(), "invalid abort without begin");
        const auto& aborted_batches = pending.mapped().batches;
        // Remove the aborted records from the log
        // We remove records from the log instead of waiting to add records
        // until commit so that we keep the original order if other producers
        // add records in the meantime.
        std::erase_if(
          _committed_log,
          [&aborted_batches](const model::record_batch& committed) {
              return absl::c_find(aborted_batches, committed)
                     != aborted_batches.end();
          });
        _aborted.emplace_back(
          model::producer_identity(p.id, p.epoch),
          pending.mapped().start,
          _current_offset);
        iobuf key;
        key.append("\x00\x00\x00\x01", 4);
        return make_control_batch(p, std::move(key));
    }

    model::record_batch make_control_batch(pid p, iobuf key) {
        storage::record_batch_builder builder(
          model::record_batch_type::raft_data, _current_offset++);
        builder.set_producer_identity(p.id, p.epoch);
        builder.set_control_type();
        builder.add_raw_kv(std::move(key), std::nullopt);
        return std::move(builder).build();
    }

    model::offset _current_offset = model::offset(0);
    absl::btree_map<pid, pending_txn> _pending_txns;
    absl::btree_map<pid, int32_t> _pid_sequences;
    std::vector<model::tx_range> _aborted;
    std::vector<model::record_batch> _committed_log;
};

using ::testing::IsEmpty;
using ::testing::Not;

MATCHER_P2(
  IsBetween,
  a,
  b,
  absl::StrCat(
    negation ? "isn't" : "is",
    " between ",
    testing::PrintToString(a),
    " and ",
    testing::PrintToString(b))) {
    return a <= arg && arg <= b;
}

// A matcher to compare a reference wrapper to it's original type.
MATCHER_P(
  ByRefImpl,
  a,
  absl::StrCat(
    negation ? "isn't" : "is", " equal to ", testing::PrintToString(a.get()))) {
    return a.get() == arg;
}

// A helper matcher to compare containers of non-copyable types, as by default
// gtest requires that arguments here are copyable.
template<typename... Args>
auto ElementsAre(const Args&... args) {
    return ::testing::ElementsAre(ByRefImpl(std::ref(args))...);
}

TEST_F(TransactionReaderTest, WithinRange) {
    auto [data, abort] = create_batches(
      pid{.id = 1}, begin_txn(), data_batch(), abort_txn());

    std::vector<std::pair<model::offset, model::offset>> ranges = {
      {model::prev_offset(data.base_offset()), data.last_offset()},
      {data.base_offset(), model::next_offset(data.last_offset())},
      {data.base_offset(), data.last_offset()},
    };
    for (auto range : ranges) {
        auto tracker = make_aborted_txns(
          {{pid{.id = 1}, range.first, range.second}});

        auto actual = read_to_vector(
          model::make_record_batch_reader<read_committed_reader>(
            std::move(tracker), make_reader({data, abort})));
        EXPECT_THAT(actual, IsEmpty());
    }
}

TEST_F(TransactionReaderTest, OutsideRange) {
    auto [data, abort] = create_batches(
      pid{.id = 1}, begin_txn(), data_batch(), commit_txn());

    std::vector<std::pair<model::offset, model::offset>> ranges = {
      {model::next_offset(data.base_offset()), data.last_offset()},
      {data.base_offset(), model::prev_offset(data.last_offset())},
      {model::next_offset(data.base_offset()),
       model::prev_offset(data.last_offset())},
    };

    for (auto range : ranges) {
        // ensure our ranges are inclusive
        auto tracker = make_aborted_txns(
          {{pid{.id = 1}, range.first, range.second}});

        auto actual = read_to_vector(
          model::make_record_batch_reader<read_committed_reader>(
            std::move(tracker), make_reader({data, abort})));
        EXPECT_THAT(actual, ElementsAre(data));
    }
}

class RandomizedTransactionReaderTest
  : public TransactionReaderTest
  , public testing::WithParamInterface<test_case> {};

TEST_P(RandomizedTransactionReaderTest, ReadsOnlyCommittedRecords) {
    auto reader = construct_log_reader(GetParam().producers);
    // Wrap the reader with our filtering logic
    auto log = read_to_vector(
      model::make_record_batch_reader<read_committed_reader>(
        make_aborted_transaction_tracker(), std::move(reader)));
    // We should only have committed data
    EXPECT_EQ(log, committed_batches());
    auto aborted = make_aborted_transaction_tracker()
                     ->compute_aborted_transactions(
                       model::offset::min(), model::offset::max())
                     .get();
    for (const auto& batch : log) {
        model::producer_identity pid(
          batch.header().producer_id, batch.header().producer_epoch);
        EXPECT_FALSE(batch.header().attrs.is_control());
        if (!batch.header().attrs.is_transactional()) {
            continue;
        }
        for (const auto& aborted_tx : aborted) {
            if (aborted_tx.pid != pid) {
                continue;
            }
            EXPECT_THAT(
              batch.base_offset(),
              Not(IsBetween(aborted_tx.first, aborted_tx.last)));
            EXPECT_THAT(
              batch.last_offset(),
              Not(IsBetween(aborted_tx.first, aborted_tx.last)));
        }
    }
}

INSTANTIATE_TEST_SUITE_P(
  RandomizedTransactions,
  RandomizedTransactionReaderTest,
  testing::Values(
    test_case{
    .name = "one_aborted_txn",
      .producers = {
        producer{
          .pid = {.id = 1},
          .data = {
            begin_txn(),
            data_batch(),
            abort_txn(),
          },
        },
      },
    },
    test_case{
      .name = "one_committed_txn",
      .producers = {
         producer{
           .pid = {.id = 1},
           .data = {
             begin_txn(),
             data_batch(),
             commit_txn(),
           },
         },
      },
    },
    test_case{
      .name = "commit_abort_commit",
      .producers = {
         producer{
           .pid = {.id = 1},
           .data = {
             begin_txn(),
             data_batch(),
             commit_txn(),

             begin_txn(),
             data_batch(),
             abort_txn(),

             begin_txn(),
             data_batch(),
             commit_txn(),
           },
         },
      },
    },
    test_case{
      .name = "two_producers",
      .producers = {
         producer{
           .pid = {.id = 1},
           .data = {
              begin_txn(),
              data_batch(),
              commit_txn(),

              begin_txn(),
              data_batch(),
              abort_txn(),

              begin_txn(),
              data_batch(),
              commit_txn(),
           },
         },
         producer{
           .pid = {.id = 2},
           .data = {
              data_batch(),
              data_batch(),
              data_batch(),
              data_batch(),
              data_batch(),
              data_batch(),
           },
         },
      },
    },
    test_case{
    .name = "three_producers",
      .producers = {
         producer{
           .pid = {.id = 1},
           .data = {
              begin_txn(),
              data_batch(),
              data_batch(),
              data_batch(),
              commit_txn(),
           },
         },
         producer{
           .pid = {.id = 2},
           .data = {
              begin_txn(),
              data_batch(),
              data_batch(),
              data_batch(),
              abort_txn(),

              begin_txn(),
              data_batch(),
              data_batch(),
              data_batch(),
              abort_txn(),

              begin_txn(),
              data_batch(),
              data_batch(),
              data_batch(),
              abort_txn(),
           },
         },
         producer{
           .pid = {.id = 3},
           .data = {
              data_batch(),
              begin_txn(),
              data_batch(),
              data_batch(),
              data_batch(),
              commit_txn(),
              data_batch(),
           },
         },
      },
    }), 
    [](const auto& test) { return test.param.name; });

} // namespace
} // namespace kafka
