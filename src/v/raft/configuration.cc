#include "raft/configuration.h"

#include "raft/consensus_utils.h"

namespace raft {
group_configuration::const_iterator
group_configuration::find_in_nodes(model::node_id id) const {
    return details::find_machine(cbegin(nodes), cend(nodes), id);
}
group_configuration::iterator
group_configuration::find_in_nodes(model::node_id id) {
    return details::find_machine(begin(nodes), end(nodes), id);
}

group_configuration::const_iterator
group_configuration::find_in_learners(model::node_id id) const {
    return details::find_machine(cbegin(learners), cend(learners), id);
}

group_configuration::iterator
group_configuration::find_in_learners(model::node_id id) {
    return details::find_machine(begin(learners), end(learners), id);
}

bool group_configuration::contains_broker(model::node_id id) const {
    auto joined_range = boost::join(nodes, learners);
    return std::any_of(
      std::cbegin(joined_range),
      std::cend(joined_range),
      [id](const model::broker& b) { return b.id() == id; });
}

bool operator==(const group_configuration& a, const group_configuration& b) {
    return a.nodes == b.nodes && a.learners == b.learners;
}

void group_configuration::update_broker(model::broker broker) {
    auto joined_range = boost::join(nodes, learners);

    auto it = std::find_if(
      std::cbegin(joined_range),
      std::cend(joined_range),
      [id = broker.id()](const model::broker& b) { return b.id() == id; });

    if (it != std::cend(joined_range)) {
        *it = std::move(broker);
        return;
    }

    throw std::invalid_argument(
      fmt::format("node with id {} not found in configuration", broker.id()));
}

std::ostream& operator<<(std::ostream& o, const group_configuration& c) {
    o << "{group_configuration: nodes: [";
    for (auto& n : c.nodes) {
        o << n.id();
    }
    o << "], learners: [";
    for (auto& n : c.learners) {
        o << n.id();
    }
    return o << "]}";
}

std::ostream& operator<<(std::ostream& o, const offset_configuration& c) {
    fmt::print(o, "{{offset: {}, group_configuration: {}}}", c.offset, c.cfg);
    return o;
}

} // namespace raft
