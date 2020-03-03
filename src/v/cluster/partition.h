#pragma once

#include "cluster/types.h"
#include "model/record_batch_reader.h"
#include "raft/consensus.h"
#include "raft/types.h"
#include "storage/types.h"

namespace cluster {
class partition_manager;

/// holds cluster logic that is not raft related
/// all raft logic is proxied transparently
class partition {
public:
    explicit partition(consensus_ptr r)
      : _raft(r) {}
    raft::group_id group() const { return raft::group_id(_raft->meta().group); }
    ss::future<> start() { return _raft->start(); }
    ss::future<> stop() { return _raft->stop(); }

    ss::future<result<raft::replicate_result>>
    replicate(model::record_batch_reader&& r) {
        return _raft->replicate(std::move(r));
    }

    /**
     * The reader is modified such that the max offset is configured to be
     * the minimum of the max offset requested and the committed index of the
     * underlying raft group.
     */
    model::record_batch_reader make_reader(storage::log_reader_config config) {
        return _raft->make_reader(std::move(config));
    }

    /**
     * The returned value of last committed offset should not be used to
     * do things like initialize a reader (use partition::make_reader). Instead
     * it can be used to report upper offset bounds to clients.
     */
    model::offset committed_offset() const { return _raft->committed_offset(); }

    const model::ntp& ntp() const { return _raft->ntp(); }

    ss::future<std::optional<storage::timequery_result>>
    timequery(model::timestamp t, ss::io_priority_class p) {
        storage::timequery_config cfg(t, _raft->committed_offset(), p);
        return _raft->timequery(cfg);
    }

    bool is_leader() const { return _raft->is_leader(); }

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
