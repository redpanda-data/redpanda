#include "cluster/metadata_cache.h"

#include <seastar/core/thread.hh>

#include <boost/range/algorithm/copy.hpp>

namespace cluster {

metadata_cache::metadata_cache(sharded<write_ahead_log>& log) noexcept
  : _log(log) {
}

future<std::vector<model::topic_view>> metadata_cache::all_topics() const {
    return async([this] {
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

future<std::optional<model::topic_metadata>>
metadata_cache::get_topic_metadata(model::topic_view topic) const {
    auto ret = std::make_unique<model::topic_metadata>(std::move(topic));
    auto ptr = ret.get();
    return _log
      .map([name = ptr->topic.name()](write_ahead_log& log) {
          std::vector<model::partition::type> partitions;
          for (auto& [_, s] : log.topics_manager().props()) {
              if (s->topic()->string_view() == name) {
                  for (auto i = 0; i < s->partitions(); ++i) {
                      partitions.push_back(i);
                  }
              }
          }
          return partitions;
      })
      .then([ret = std::move(ret)](auto all_partitions) mutable {
          if (all_partitions.empty()) {
              return std::optional<model::topic_metadata>();
          }
          for (auto& vp : all_partitions) {
              for (auto& p : vp) {
                  ret->partitions.emplace_back(model::partition(p));
              }
          }
          return std::optional<model::topic_metadata>(std::move(*ret));
      });
}

} // namespace cluster
