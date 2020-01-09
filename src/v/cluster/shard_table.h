#pragma once

#include "model/fundamental.h"
#include "raft/types.h"
#include "seastarx.h"

#include <seastar/core/reactor.hh> // shard_id

namespace cluster {
/// \brief this is populated by consensus::controller
/// every core will have a _full_ copy of all indexes
class shard_table final {
public:
    bool contains(const raft::group_id& group) {
        return _group_idx.find(group) != _group_idx.end();
    }
    ss::shard_id shard_for(const raft::group_id& group) {
        return _group_idx.find(group)->second;
    }
    bool contains(const model::ntp& ntp) const {
        return _ntp_idx.find(ntp) != _ntp_idx.end();
    }
    /// \brief from storage::shard_assignment
    ss::shard_id shard_for(const model::ntp& ntp) {
        return _ntp_idx.find(ntp)->second;
    }
    void insert(model::ntp ntp, ss::shard_id i) {
        _ntp_idx.insert({std::move(ntp), i});
    }
    void insert(raft::group_id g, ss::shard_id i) { _group_idx.insert({g, i}); }

private:
    // kafka index
    std::unordered_map<model::ntp, ss::shard_id> _ntp_idx;
    // raft index
    std::unordered_map<raft::group_id, ss::shard_id> _group_idx;
};
} // namespace cluster
