#pragma once

#include "raft/consensus.h"

namespace cluster {
class partition {
public:
    using consensus_ptr = lw_shared_ptr<raft::consensus>;

    explicit partition(consensus_ptr r)
      : _raft(r) {
    }
    raft::group_id group() const {
        return raft::group_id(_raft->meta().group);
    }
    future<> start() {
        return _raft->start();
    }
    future<> replicate(raft::entry) {
        return make_ready_future<>();
    }
    const model::ntp& ntp() const {
        return _raft->ntp();
    }

    /// \brief needs to be exposed for raft/service.h
    consensus_ptr raft() {
        return _raft;
    }

private:
    consensus_ptr _raft;
};
} // namespace cluster
namespace std {
template<>
struct hash<cluster::partition> {
    size_t operator()(const cluster::partition& x) const {
        return std::hash<model::ntp>()(x.ntp());
    }
};
inline ostream& operator<<(ostream& o, const cluster::partition& x) {
    return o << "{cluster::partition{" << x.ntp() << "}}";
}
} // namespace std
