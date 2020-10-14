#pragma once
#include "coproc/tests/coprocessor.h"
#include "model/record_batch_types.h"
#include "storage/record_batch_builder.h"
#include "storage/tests/utils/disk_log_builder.h"

// Silly logic, checks if the key_size is evenly divisible by two, places
// the record in the even or 'odd' collection based on this predicate
struct two_way_split_copro : public coprocessor {
    two_way_split_copro(coproc::script_id sid, input_set input)
      : coprocessor(sid, std::move(input)) {}

    coprocessor::result
    apply(model::topic, std::vector<model::record_batch> batches) override {
        model::topic even("even");
        model::topic odd("odd");
        coprocessor::result r;
        r.emplace(even, std::vector<model::record_batch>());
        r.emplace(odd, std::vector<model::record_batch>());
        for (auto& record_batch : batches) {
            storage::record_batch_builder even_rbb(
              model::well_known_record_batch_types[1], model::offset(0));
            storage::record_batch_builder odd_rbb(
              model::well_known_record_batch_types[1], model::offset(0));
            record_batch.for_each_record(
              [&even_rbb, &odd_rbb](model::record&& record) {
                  if (record.key_size() % 2 == 0) {
                      even_rbb.add_raw_kv(
                        record.release_key(), record.release_value());
                  } else {
                      odd_rbb.add_raw_kv(
                        record.release_key(), record.release_value());
                  }
              });
            r[even].emplace_back(std::move(even_rbb).build());
            r[odd].emplace_back(std::move(odd_rbb).build());
        }
        return r;
    }

    static absl::flat_hash_set<model::topic> output_topics() {
        return {model::topic("even"), model::topic("odd")};
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
