#include "raft/service.h"

namespace raft {
service::service(scheduling_group& sc, smp_service_group& ssg)
  : raftgen_service(sc, ssg) {
    finjector::shard_local_badger().register_probe(
      failure_probes::name(), _probe);
}

future<vote_reply> service::vote(vote_request, rpc::streaming_context&) {
    throw std::runtime_error("unimplemented method");
}

future<append_entries_reply>
service::append_entries(append_entries_request, rpc::streaming_context&) {
    throw std::runtime_error("unimplemented method");
}

future<configuration_reply>
service::configuration_update(configuration_request, rpc::streaming_context&) {
    throw std::runtime_error("unimplemented method");
}

service::~service() {
    finjector::shard_local_badger().deregister_probe(failure_probes::name());
}
} // namespace raft
