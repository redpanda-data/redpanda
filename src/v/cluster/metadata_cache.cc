#include "cluster/metadata_cache.h"

#include <fmt/format.h>

namespace cluster {

std::vector<model::topic> metadata_cache::all_topics() const {
    std::vector<model::topic> topics;
    topics.reserve(_cache.size());
    std::transform(
      std::cbegin(_cache),
      std::cend(_cache),
      std::back_inserter(topics),
      [](const cache_t::value_type& t_md) { return t_md.first; });
    return topics;
}

std::optional<model::topic_metadata>
metadata_cache::get_topic_metadata(model::topic_view topic) const {
    if (auto it = _cache.find(topic); it != std::cend(_cache)) {
        return create_topic_metadata(*it);
    }
    return std::nullopt;
}

std::vector<model::topic_metadata> metadata_cache::all_topics_metadata() const {
    std::vector<model::topic_metadata> metadata;
    std::transform(
      std::cbegin(_cache),
      std::cend(_cache),
      std::back_inserter(metadata),
      [](const cache_t::value_type& tp_md) {
          return create_topic_metadata(tp_md);
      });

    return metadata;
}

std::vector<broker_ptr> metadata_cache::all_brokers() const {
    std::vector<broker_ptr> brokers;
    brokers.reserve(_brokers_cache.size());
    std::transform(
      std::cbegin(_brokers_cache),
      std::cend(_brokers_cache),
      std::back_inserter(brokers),
      [](const broker_cache_t::value_type& b) { return b.second; });

    return brokers;
}

std::vector<model::node_id> metadata_cache::all_broker_ids() const {
    std::vector<model::node_id> ids;
    ids.reserve(_brokers_cache.size());
    std::transform(
      std::cbegin(_brokers_cache),
      std::cend(_brokers_cache),
      std::back_inserter(ids),
      [](const broker_cache_t::value_type& b) { return b.first; });

    return ids;
}

/// Returns single broker if exists in cache
std::optional<broker_ptr> metadata_cache::get_broker(model::node_id id) const {
    if (auto it = _brokers_cache.find(id); it != _brokers_cache.end()) {
        return it->second;
    }
    return std::nullopt;
}

/// Updates or add broker to cache
void metadata_cache::update_brokers_cache(
  std::vector<model::broker>&& brokers) {
    _brokers_cache.clear();
    for (auto& b : brokers) {
        auto id = b.id();
        _brokers_cache.emplace(id, ss::make_lw_shared(std::move(b)));
    }
}

void metadata_cache::add_topic(model::topic_view topic) {
    _cache.emplace(topic, topic_metadata{});
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
    auto p = find_partition(it->second, p_as.ntp.tp.partition);
    auto p_md = p_as.create_partition_metadata();
    if (p) {
        // This partition already exists, update it
        p->p_md = std::move(p_md);
    } else {
        // This partition is new for this topic, just add it
        it->second.partitions.push_back(partition{std::move(p_md)});
    }
}

void metadata_cache::update_partition_leader(
  model::topic_view topic,
  model::partition_id partition_id,
  model::term_id term,
  std::optional<model::node_id> leader_id) {
    auto it = find_topic_metadata(topic);
    auto p = find_partition(it->second, partition_id);
    if (!p) {
        throw std::runtime_error(fmt::format(
          "Requested topic {} partion {} does not exist in cache",
          topic(),
          partition_id()));
    }
    if (p->term_id > term) {
        // Do nothing if update term is older
        return;
    }
    p->p_md.leader_node = leader_id;
    p->term_id = term;
}

model::topic_metadata
create_topic_metadata(const metadata_cache::cache_t::value_type& tp_md_pair) {
    model::topic_metadata tp_md(tp_md_pair.first);
    std::transform(
      std::cbegin(tp_md_pair.second.partitions),
      std::cend(tp_md_pair.second.partitions),
      std::back_inserter(tp_md.partitions),
      [](const metadata_cache::partition& p) { return p.p_md; });
    return tp_md;
}

metadata_cache::partition*
find_partition(metadata_cache::topic_metadata& t_md, model::partition_id p_id) {
    auto it = std::find_if(
      std::begin(t_md.partitions),
      std::end(t_md.partitions),
      [p_id](const metadata_cache::partition& p_md) {
          return p_md.p_md.id == p_id;
      });
    return it == std::end(t_md.partitions) ? nullptr : &(*it);
}

bool metadata_cache::contains(
  const model::topic& topic, const model::partition_id pid) const {
    if (auto it = _cache.find(topic); it != _cache.end()) {
        const auto& partitions = it->second.partitions;
        return std::any_of(
          partitions.cbegin(),
          partitions.cend(),
          [&pid](const metadata_cache::partition& partition) {
              return partition.p_md.id == pid;
          });
    }
    return false;
}

} // namespace cluster
