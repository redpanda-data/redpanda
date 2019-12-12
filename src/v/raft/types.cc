#include "raft/types.h"

#include "raft/consensus_utils.h"

namespace raft {
group_configuration::const_iter
group_configuration::find_in_nodes(model::node_id id) const {
    return details::find_machine(cbegin(nodes), cend(nodes), id);
}
group_configuration::iter
group_configuration::find_in_nodes(model::node_id id) {
    return details::find_machine(begin(nodes), end(nodes), id);
}

group_configuration::const_iter
group_configuration::find_in_learners(model::node_id id) const {
    return details::find_machine(cbegin(learners), cend(learners), id);
}

group_configuration::iter
group_configuration::find_in_learners(model::node_id id) {
    return details::find_machine(begin(learners), end(learners), id);
}

bool group_configuration::contains_broker(model::node_id id) const {
    auto find_by_id = [id](const model::broker b) { return b.id() == id; };
    return std::any_of(std::cbegin(nodes), std::cend(nodes), find_by_id)
           || std::any_of(
             std::cbegin(learners), std::cend(learners), find_by_id);
}

group_configuration::brokers_t group_configuration::all_brokers() const {
    std::vector<model::broker> all;
    all.reserve(nodes.size() + learners.size());
    std::copy(std::cbegin(nodes), std::cend(nodes), std::back_inserter(all));
    std::copy(
      std::cbegin(learners), std::cend(learners), std::back_inserter(all));
    return all;
}
} // namespace raft