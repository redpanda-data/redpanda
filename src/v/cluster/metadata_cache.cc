#include "cluster/metadata_cache.h"

#include <seastar/core/thread.hh>

#include <boost/range/algorithm/copy.hpp>

namespace cluster {

metadata_cache::metadata_cache(seastar::sharded<write_ahead_log>& log) noexcept
  : _log(log) {
}

seastar::future<std::vector<model::topic_view>>
metadata_cache::all_topics() const {
    return seastar::async([this] {
        auto topic_set = _log
                           .map_reduce0(
                             [](write_ahead_log& log) {
                                 std::vector<model::topic_view> topics;
                                 for (auto& [_, s] :
                                      log.topics_manager().props()) {
                                     topics.emplace_back(std::string_view(
                                       s->topic()->string_view()));
                                 }
                                 return topics;
                             },
                             std::unordered_set<model::topic_view>{},
                             [](
                               std::unordered_set<model::topic_view>&& res,
                               std::vector<model::topic_view>&& ts) {
                                 for (auto& t : ts) {
                                     res.emplace(std::move(t));
                                 }
                                 return std::move(res);
                             })
                           .get0();
        return boost::copy_range<std::vector<model::topic_view>>(
          std::move(topic_set));
    });
}

seastar::future<std::optional<model::topic_metadata>>
metadata_cache::get_topic_metadata(model::topic_view topic) const {
    return seastar::async([this, topic = std::move(topic)]() mutable {
        auto all_partitions
          = _log
              .map([&](write_ahead_log& log) {
                  std::vector<model::partition_type> partitions;
                  for (auto& [_, s] : log.topics_manager().props()) {
                      if (s->topic()->string_view() == topic.name()) {
                          partitions.push_back(s->partitions());
                      }
                  }
                  return partitions;
              })
              .get0();
        std::optional<model::topic_metadata> metadata;
        for (auto& shard_partitions : all_partitions) {
            if (shard_partitions.empty()) {
                continue;
            }
            if (!metadata) {
                metadata = model::topic_metadata{std::move(topic)};
            }
            for (model::partition_type p : shard_partitions) {
                metadata->partitions.push_back(model::partition_metadata{p});
            }
        }
        return metadata;
    });
}

} // namespace cluster