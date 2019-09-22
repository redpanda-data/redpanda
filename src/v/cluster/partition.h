#pragma once

#include "raft/consensus.h"

namespace cluster {
class partition {
public:
    partition(
      model::node_id,
      model::namespaced_topic_partition ntpidx,
      raft::group_id,
      sharded<raft::client_cache>& cli)
      : _ntp(std::move(ntpidx))
      , _clients(cli) {
    }
    raft::group_id group() const {
        return raft::group_id(_raft->meta().group);
    }
    future<> replicate(std::unique_ptr<raft::entry> e) {
        return _raft->replicate(std::move(e));
    }
    // entry point
    future<> recover(storage::log_config cfg) {
        return make_ready_future<>();
    }
    raft::consensus& raft_group() {
        return *_raft;
    }
    const model::namespaced_topic_partition& ntp() const {
        return _ntp;
    }

private:
    model::namespaced_topic_partition _ntp;
    sharded<raft::client_cache>& _clients;
    std::unique_ptr<raft::consensus> _raft;
};
} // namespace cluster
namespace std {
template<>
struct hash<cluster::partition> {
    size_t operator()(const cluster::partition& x) const {
        return std::hash<model::namespaced_topic_partition>()(x.ntp());
    }
};
ostream& operator<<(ostream& o, const cluster::partition& x) {
    return o << "{cluster::partition{" << x.ntp() << "}}";
}
} // namespace std
