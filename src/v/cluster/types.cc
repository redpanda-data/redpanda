#include "cluster/types.h"

namespace cluster {

std::ostream& operator<<(std::ostream& o, topic_error_code code) {
    o << "{ topic_error_code: ";
    if (
      code > topic_error_code::topic_error_code_max
      || code < topic_error_code::topic_error_code_min) {
        o << "out_of_range";
    } else {
        o << topic_error_code_names[static_cast<int16_t>(code)];
    }
    o << " }";
    return o;
}

void topic_metadata_entry::update_partition(
  model::partition_id p_id, partition_replica p_r) {
    auto partition = find_partition(p_id);

    if (partition) {
        // This partition already exists, update it
        partition->get().update_replicas(std::move(p_r));
    } else {
        // This partition is new for this topic, just add it
        add_new_partition(p_id, std::move(p_r));
    }
}

model::topic_metadata
topic_metadata_entry::as_model_type(model::topic_view topic) const {
    auto t_md = model::topic_metadata(topic);
    t_md.partitions.reserve(partitions.size());
    std::transform(
      partitions.begin(),
      partitions.end(),
      std::back_inserter(t_md.partitions),
      [](const partition_metadata_entry& p_md) {
          return p_md.as_model_type();
      });
    return t_md;
}

std::optional<std::reference_wrapper<partition_metadata_entry>>
topic_metadata_entry::find_partition(model::partition_id p_id) {
    auto it = std::find_if(
      partitions.begin(),
      partitions.end(),
      [p_id](const partition_metadata_entry& p_md) { return p_md.id == p_id; });
    return it == partitions.end()
             ? std::nullopt
             : std::make_optional<
               std::reference_wrapper<partition_metadata_entry>>(*it);
}

void topic_metadata_entry::add_new_partition(
  model::partition_id p_id, partition_replica replica) {
    std::vector<partition_replica> replicas;
    replicas.push_back(std::move(replica));
    partitions.push_back(
      partition_metadata_entry{.id = p_id, .replicas = std::move(replicas)});
}

partition_metadata_entry::replicas_t::iterator
partition_metadata_entry::find_replica(model::replica_id r_id) {
    return std::find_if(
      replicas.begin(), replicas.end(), [r_id](const partition_replica& r) {
          return r.id == r_id;
      });
}

model::partition_metadata partition_metadata_entry::as_model_type() const {
    model::partition_metadata p_md(id);
    p_md.replicas.reserve(replicas.size());
    p_md.leader_node = leader_node;
    std::transform(
      replicas.begin(),
      replicas.end(),
      std::back_inserter(p_md.replicas),
      [](const partition_replica& pr) { return pr.node; });
    return p_md;
}

void partition_metadata_entry::update_replicas(partition_replica p_r) {
    if (auto it = find_replica(p_r.id); it == replicas.end()) {
        // This is a new replica
        replicas.push_back(std::move(p_r));
    } else {
        // This replica has to be updated
        *it = std::move(p_r);
    }
}

} // namespace cluster