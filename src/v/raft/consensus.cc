#include "raft/consensus.h"

namespace raft {
consensus::consensus(
  model::node_id nid,
  protocol_metadata m,
  group_configuration gcfg,
  storage::log& l,
  sharded<client_cache>& clis)
  : _self(std::move(nid))
  , _meta(std::move(m))
  , _conf(std::move(gcfg))
  , _log(l)
  , _clients(clis) {
}

future<vote_reply> consensus::vote(vote_request) {
    throw std::runtime_error("unimplemented");
}

future<append_entries_reply> consensus::append_entries(append_entries_request) {
    throw std::runtime_error("unimplemented");
}

future<> consensus::replicate(std::unique_ptr<entry>) {
    return make_ready_future<>();
}

} // namespace raft
