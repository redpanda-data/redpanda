/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#pragma once
#include "coproc/tests/utils/coprocessor.h"
#include "model/record_batch_types.h"
#include "raft/types.h"
#include "storage/record_batch_builder.h"
#include "storage/tests/utils/disk_log_builder.h"

#include <seastar/core/circular_buffer.hh>

// Silly logic, checks if the key_size is evenly divisible by two, places
// the record in the even or 'odd' collection based on this predicate
struct two_way_split_copro : public coprocessor {
    using batches_t = ss::circular_buffer<model::record_batch>;

    two_way_split_copro(coproc::script_id sid, input_set input)
      : coprocessor(sid, std::move(input)) {}

    ss::future<coprocessor::result>
    apply(const model::topic&, batches_t&& batches) override {
        static const model::topic even("even");
        static const model::topic odd("odd");
        coprocessor::result r;
        r.emplace(even, batches_t());
        r.emplace(odd, batches_t());
        return ss::do_with(
          std::move(r),
          std::move(batches),
          [this](coprocessor::result& r, batches_t& batches) {
              return ss::do_for_each(
                       batches,
                       [this, &r](model::record_batch& record_batch) {
                           auto split = split_batches(record_batch);
                           r[even].emplace_back(std::move(split.even));
                           r[odd].emplace_back(std::move(split.odd));
                       })
                .then([&r] { return std::move(r); });
          });
    }

    static absl::flat_hash_set<model::topic> output_topics() {
        return {model::topic("even"), model::topic("odd")};
    }

private:
    struct two_way_split_batch {
        model::record_batch even;
        model::record_batch odd;
    };

    two_way_split_batch split_batches(model::record_batch& record_batch) {
        storage::record_batch_builder even_rbb(
          raft::data_batch_type, model::offset(0));
        storage::record_batch_builder odd_rbb(
          raft::data_batch_type, model::offset(0));
        record_batch.for_each_record(
          [&even_rbb, &odd_rbb](model::record&& record) {
              if (record.key_size() % 2 == 0) {
                  even_rbb.add_raw_kv(record.share_key(), record.share_value());
              } else {
                  odd_rbb.add_raw_kv(record.share_key(), record.share_value());
              }
          });
        return two_way_split_batch{
          .even = std::move(even_rbb).build(),
          .odd = std::move(odd_rbb).build()};
    }
};

struct two_way_split_stats {
    std::size_t n_records{0};
    std::size_t n_even{0};
    std::size_t n_odd{0};

    two_way_split_stats() = default;

    explicit two_way_split_stats(
      const model::record_batch_reader::data_t& data) {
        for (const auto& record_batch : data) {
            record_batch.for_each_record([this](const model::record& record) {
                if (record.key_size() % 2 == 0) {
                    n_even += 1;
                } else {
                    n_odd += 1;
                }
            });
        }
        n_records = n_even + n_odd;
    }

    bool operator==(const two_way_split_stats& other) const {
        return n_records == other.n_records && n_even == other.n_even
               && n_odd == other.n_odd;
    }

    bool operator!=(const two_way_split_stats& other) const {
        return !(*this == other);
    }

    two_way_split_stats operator+(const two_way_split_stats& other) const {
        two_way_split_stats c(*this);
        c.n_records += other.n_records;
        c.n_even += other.n_even;
        c.n_odd += other.n_odd;
        return c;
    }

    friend std::ostream& operator<<(std::ostream&, const two_way_split_stats&);
};

std::ostream& operator<<(std::ostream& o, const two_way_split_stats& twss) {
    o << "Total records: " << twss.n_records << " total even: " << twss.n_even
      << " total odd: " << twss.n_odd << std::endl;
    return o;
}

two_way_split_stats
map_stats(std::optional<model::record_batch_reader::data_t> data) {
    return !data ? two_way_split_stats() : two_way_split_stats(*data);
}

template<typename Iterator>
two_way_split_stats aggregate_totals(const Iterator begin, const Iterator end) {
    return std::accumulate(
      begin, end, two_way_split_stats(), [](const auto acc, const auto x) {
          return acc + x;
      });
}
