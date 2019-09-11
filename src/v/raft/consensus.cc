#include "raft/consensus.h"

namespace raft {
consensus::consensus(
  configuration c, sharded<client_cache>& clis)
  : cfg(std::move(c))
  , _clients(clis) {
}

future<vote_reply> consensus::vote(vote_request) {
    throw std::runtime_error("unimplemented");
}

future<append_entries_reply> consensus::append_entries(append_entries_request) {
    throw std::runtime_error("unimplemented");
}

future<configuration_reply>
consensus::configuration_update(configuration_request) {
    throw std::runtime_error("unimplemented");
}

} // namespace raft
