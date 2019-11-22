#include "raft/types.h"

#include "raft/consensus_utils.h"

namespace raft {
std::optional<model::broker>
group_configuration::find_in_nodes(model::node_id id) const {
    return raft::details::find_machine(nodes, id);
}

std::optional<model::broker>
group_configuration::find_in_learners(model::node_id id) const {
    return raft::details::find_machine(learners, id);
}

bool group_configuration::contains_machine(model::node_id id) const {
    auto find_by_id = [id](const model::broker b) { return b.id() == id; };
    return std::any_of(std::cbegin(nodes), std::cend(nodes), find_by_id)
           || std::any_of(
             std::cbegin(learners), std::cend(learners), find_by_id);
}
} // namespace raft