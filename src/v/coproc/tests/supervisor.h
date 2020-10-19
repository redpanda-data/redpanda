#pragma once
#include "coproc/supervisor.h"
#include "coproc/tests/coprocessor.h"
#include "coproc/types.h"
#include "model/record_batch_reader.h"

#include <seastar/core/gate.hh>

#include <absl/container/flat_hash_map.h>

namespace coproc {
// A super simplistic form of the javascript supervisor soley used for
// the purposes of testing
class supervisor final : public coproc::supervisor_service {
public:
    using copro_map
      = absl::flat_hash_map<coproc::script_id, std::unique_ptr<coprocessor>>;

    supervisor(
      ss::scheduling_group sc,
      ss::smp_service_group ssg,
      ss::sharded<copro_map>& cp_map)
      : supervisor_service(sc, ssg)
      , _coprocessors(cp_map) {}

    ~supervisor() { _gate.close().get(); }

    /// Method is hit when a request arrives from redpanda
    /// Data is transformed by all applicable coprocessors in the copro_map
    ss::future<process_batch_reply>
    process_batch(process_batch_request&& r, rpc::streaming_context&) final;

private:
    void invoke_coprocessor(
      const model::ntp&,
      const script_id,
      const std::vector<model::record_batch>&,
      std::vector<process_batch_reply::data>&);

    ss::future<std::vector<process_batch_reply::data>>
      invoke_coprocessors(process_batch_request::data);

    /// Map of coprocessors organized by their global identifiers
    ss::sharded<copro_map>& _coprocessors;

    /// Ensure no outstanding futures are executing before shutdown
    ss::gate _gate;
};
} // namespace coproc
