#pragma once

#include "raft/raftgen_service.h"

namespace raft {
  class service final : public raftgen_service {
  public:
    service(scheduling_group& sc, smp_service_group& ssg);
    ~service() noexcept;

    future<vote_reply> vote(vote_request, rpc::streaming_context&);

    future<append_entries_reply>
    append_entries(append_entries_request, rpc::streaming_context&);

    future<configuration_reply>
    configuration_update(configuration_request, rpc::streaming_context&);
  };
} // namespace raft
