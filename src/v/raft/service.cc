#include "raft/service.h"

namespace raft {
service::service(scheduling_group& sc, smp_service_group& ssg)
  : raftgen_service(sc, ssg) {
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

} // namespace raft
