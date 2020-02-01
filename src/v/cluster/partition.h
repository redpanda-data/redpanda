#pragma once

#include "cluster/types.h"
#include "raft/consensus.h"
#include "raft/types.h"

#include <utility>

namespace cluster {
class partition_manager;

/// holds cluster logic that is not raft related
/// all raft logic is proxied transparently
class partition {
public:
    explicit partition(consensus_ptr r)
      : _raft(std::move(r)) {}
    raft::group_id group() const { return raft::group_id(_raft->meta().group); }
    ss::future<> start() { return _raft->start(); }
    ss::future<> stop() { return _raft->stop(); }

    ss::future<result<raft::replicate_result>>
    replicate(model::record_batch_reader&& r) {
        return _raft->replicate(std::move(r));
    }
    const model::ntp& ntp() const { return _raft->ntp(); }

private:
    friend partition_manager;

    consensus_ptr raft() { return _raft; }

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
