#include "coproc/tests/utils.h"

#include "model/record_batch_reader.h"
#include "storage/log.h"
#include "storage/log_appender.h"

template<typename itr_start, typename itr_end, typename func>
std::size_t sum_records(const itr_start begin, const itr_end end, func f) {
    return std::accumulate(
      begin,
      end,
      static_cast<std::size_t>(0),
      [&f](std::size_t acc, const auto& rb) { return acc + f(rb); });
}

std::size_t sum_records(const std::vector<model::record_batch>& batches) {
    return sum_records(
      batches.cbegin(), batches.cend(), [](const model::record_batch& rb) {
          return rb.record_count();
      });
}

std::size_t sum_records(const model::record_batch_reader::data_t& batches) {
    return sum_records(
      batches.cbegin(), batches.cend(), [](const model::record_batch& rb) {
          return rb.record_count();
      });
}

std::size_t
sum_records(const std::vector<model::record_batch_reader::data_t>& batches) {
    return sum_records(
      batches.cbegin(),
      batches.cend(),
      [](const model::record_batch_reader::data_t& data) {
          return sum_records(data);
      });
}

// to_topic_set
absl::flat_hash_set<model::topic>
to_topic_set(std::vector<ss::sstring>&& topics) {
    absl::flat_hash_set<model::topic> topic_set;
    std::transform(
      topics.begin(),
      topics.end(),
      std::inserter(topic_set, topic_set.begin()),
      [](const ss::sstring& t) { return model::topic(t); });
    return topic_set;
}

// to_topic_vector
std::vector<model::topic> to_topic_vector(std::vector<ss::sstring>&& topics) {
    std::vector<model::topic> topic_vec;
    std::transform(
      topics.begin(),
      topics.end(),
      std::back_inserter(topic_vec),
      [](const ss::sstring& t) { return model::topic(t); });
    return topic_vec;
}

absl::flat_hash_set<model::ntp>
to_ntps(absl::flat_hash_map<model::topic_namespace, std::size_t>&& mp) {
    absl::flat_hash_set<model::ntp> r;
    for (auto&& e : mp) {
        for (auto n = 0; n < e.second; ++n) {
            r.emplace(e.first.ns, e.first.tp, model::partition_id(n));
        }
    }
    return r;
}

coproc::enable_copros_request::data make_enable_req(
  uint32_t id,
  std::vector<std::pair<ss::sstring, coproc::topic_ingestion_policy>> topics) {
    std::vector<coproc::enable_copros_request::data::topic_mode>
      topics_and_modes;
    topics_and_modes.reserve(topics.size());
    for (auto& p : topics) {
        topics_and_modes.emplace_back(
          model::topic(std::move(p.first)), p.second);
    }
    return coproc::enable_copros_request::data{
      .id = coproc::script_id(id), .topics = std::move(topics_and_modes)};
}
