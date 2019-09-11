#include "raft/service.h"

namespace raft {
service::service(
  scheduling_group sc,
  smp_service_group ssg,
  sharded<consensus>& c,
  sharded<node_local_controller>& local_controller)
  : raftgen_service(sc, ssg)
  , _consensus(c)
  , _nlc(local_controller) {
    finjector::shard_local_badger().register_probe(
      failure_probes::name(), &_probe);
}

future<vote_reply> service::do_vote(vote_request r, rpc::streaming_context&) {
    auto shard = _nlc.local().shard_for_raft_group(group_id(r.group));
    return with_scheduling_group(
      get_scheduling_group(), [this, shard, r = std::move(r)]() mutable {
          return _consensus.invoke_on(
            shard,
            get_smp_service_group(),
            [this, r = std::move(r)](consensus& c) mutable {
                return c.vote(std::move(r));
            });
      });
}

future<append_entries_reply>
service::do_append_entries(append_entries_request r, rpc::streaming_context&) {
    auto shard = _nlc.local().shard_for_raft_group(group_id(r.meta.group));
    return with_scheduling_group(
      get_scheduling_group(), [this, shard, r = std::move(r)]() mutable {
          return _consensus.invoke_on(
            shard,
            get_smp_service_group(),
            [this, r = std::move(r)](consensus& c) mutable {
                return c.append_entries(std::move(r));
            });
      });
}

future<configuration_reply> service::do_configuration_update(
  configuration_request r, rpc::streaming_context&) {
    auto shard = _nlc.local().shard_for_raft_group(group_id(r.group()));
    return with_scheduling_group(
      get_scheduling_group(), [this, shard, r = std::move(r)]() mutable {
          return _consensus.invoke_on(
            shard,
            get_smp_service_group(),
            [this, r = std::move(r)](consensus& c) mutable {
                return c.configuration_update(std::move(r));
            });
      });
}

service::~service() {
    finjector::shard_local_badger().deregister_probe(failure_probes::name());
}
} // namespace raft
