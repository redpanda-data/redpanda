#pragma once
#include <cstdint>
#include <thread>
#include <unordered_map>

#include <smf/fbs_typed_buf.h>
#include <smf/human_bytes.h>
#include <smf/native_type_utils.h>
#include <smf/random.h>
#include <smf/time_utils.h>

#include "wal_core_mapping.h"
#include "wal_generated.h"
#include "wal_segment_record.h"

// test only
#include "gen_create_topic_buf.h"


class wal_topic_test_input {
 public:
  static constexpr int32_t max_rand_bytes =
    static_cast<int32_t>(std::numeric_limits<uint8_t>::max());

  explicit wal_topic_test_input(
    seastar::sstring ns, seastar::sstring topic, int32_t partitions,
    wal_topic_type type,
    std::unordered_map<seastar::sstring, seastar::sstring> props,
    int32_t max_batch_size_per_partition = 1,
    int32_t rand_bytes = max_rand_bytes) {

    // create
    create_ = std::make_unique<smf::fbs_typed_buf<wal_topic_create_request>>(
      gen_create_topic_buf(ns, topic, partitions, type, props));

    auto ptr = std::make_unique<wal_put_requestT>();

    ptr->topic = xxhash_64(topic.c_str(), topic.size());
    ptr->ns = xxhash_64(ns.c_str(), ns.size());
    for (auto i = 0; i < partitions; ++i) {
      auto idx = std::make_unique<wal_put_partition_recordsT>();
      idx->partition = i;
      idx->records.reserve(max_batch_size_per_partition);
      for (auto j = 0; j < max_batch_size_per_partition; ++j) {
        seastar::sstring key =
          rand_.next_alphanum(std::max<uint64_t>(1, rand_.next() % rand_bytes));
        seastar::sstring value =
          rand_.next_str(std::max<uint64_t>(1, rand_.next() % rand_bytes));
        idx->records.push_back(wal_segment_record::coalesce(
          key.data(), key.size(), value.data(), value.size()));
      }
      ptr->partition_puts.push_back(std::move(idx));
    }

    put_ = std::make_unique<smf::fbs_typed_buf<wal_put_request>>(
      smf::native_table_as_buffer<wal_put_request>(*ptr));
  }

  inline const wal_topic_create_request *
  get_create() {
    return create_->get();
  }
  inline const wal_put_request *
  get_put() {
    return put_->get();
  }
  inline std::vector<wal_write_request>
  write_requests() {
    return wal_core_mapping::core_assignment(get_put());
  }
  inline std::vector<wal_create_request>
  create_requests() {
    return wal_core_mapping::core_assignment(get_create());
  }

 private:
  smf::random rand_;
  std::unique_ptr<smf::fbs_typed_buf<wal_put_request>> put_ = nullptr;
  std::unique_ptr<smf::fbs_typed_buf<wal_topic_create_request>> create_ =
    nullptr;
};

