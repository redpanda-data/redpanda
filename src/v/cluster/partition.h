#pragma once

#include "raft/consensus.h"

namespace cluster {
class partition {
public:
    partition(raft::consensus&& r)
      : _raft(std::move(r)) {
    }
    raft::group_id group() const {
        return raft::group_id(_raft.meta().group);
    }
    future<> start() {
        return _raft.start();
    }
    future<> replicate(std::unique_ptr<raft::entry> e) {
        return make_ready_future<>();
    }
    const model::ntp& ntp() const {
        return _raft.ntp();
    }

    /// \brief needs to be exposed for raft/service.h
    raft::consensus& raft() {
        return _raft;
    }

private:
    raft::consensus _raft;
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
