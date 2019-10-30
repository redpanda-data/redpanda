#include "cluster/metadata_cache.h"

#include "seastarx.h"

#include <seastar/core/thread.hh>

#include <boost/range/algorithm/copy.hpp>
#include <fmt/format.h>

namespace cluster {

future<std::vector<model::topic_view>> metadata_cache::all_topics() const {
    std::vector<model::topic_view> topics;
    topics.reserve(_cache.size());
    std::transform(
      _cache.begin(),
      _cache.end(),
      std::back_inserter(topics),
      [](const cache_t::value_type& pair) {
          return model::topic_view(pair.first());
      });
    return make_ready_future<std::vector<model::topic_view>>(topics);
}

future<std::optional<model::topic_metadata>>
metadata_cache::get_topic_metadata(model::topic_view topic) const {
    if (auto it = _cache.find(topic); it != _cache.end()) {
        return make_ready_future<std::optional<model::topic_metadata>>(
          it->second.as_model_type(it->first));
    }
    return make_ready_future<std::optional<model::topic_metadata>>();
}

void metadata_cache::add_topic(model::topic_view topic) {
    _cache.emplace(topic, topic_metadata_entry{});
}

void metadata_cache::remove_topic(model::topic_view topic) {
    _cache.erase(topic);
}

metadata_cache::cache_t::iterator
metadata_cache::find_topic_metadata(model::topic_view topic) {
    if (auto it = _cache.find(topic); it != _cache.end()) {
        return it;
    }
    throw std::runtime_error(fmt::format(
      "The topic {} is not yet in the metadata cache, data are corrupted",
      topic));
}

void metadata_cache::update_partition_assignment(
  const partition_assignment& p_as) {
    auto it = find_topic_metadata(p_as.ntp.tp.topic);
    it->second.update_partition(
      p_as.ntp.tp.partition, p_as.get_partition_replica());
}

void metadata_cache::update_partition_leader(
  model::topic_view topic,
  model::partition_id partition_id,
  model::node_id leader_id) {
    auto it = find_topic_metadata(topic);
    auto p = it->second.find_partition(partition_id);
    if (!p) {
        throw std::runtime_error(fmt::format(
          "Requested topic {} partion {} does not exist in cache",
          topic(),
          partition_id()));
    }
    p->get().leader_node = leader_id;
}
} // namespace cluster
