#pragma once

#include "model/fundamental.h"
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
    shard_id shard_for(const raft::group_id& group) {
        return _group_idx.find(group)->second;
    }
    bool contains(const model::ntp& ntp) {
        return _ntp_idx.find(ntp) != _ntp_idx.end();
    }
    /// \brief from storage::shard_assignment
    shard_id shard_for(const model::ntp& ntp) {
        return _ntp_idx.find(ntp)->second;
    }
    void insert(model::ntp ntp, shard_id i) {
        _ntp_idx.insert({std::move(ntp), i});
    }
    void insert(raft::group_id g, shard_id i) {
        _group_idx.insert({g, i});
    }

private:
    // kafka index
    std::unordered_map<model::ntp, shard_id> _ntp_idx;
    // raft index
    std::unordered_map<raft::group_id, shard_id> _group_idx;
};
} // namespace cluster
