#pragma once
#include "coproc/types.h"
#include "model/limits.h"
#include "random/simple_time_jitter.h"
#include "storage/api.h"
#include "storage/log.h"
#include "utils/mutex.h"

#include <seastar/core/gate.hh>

#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>

namespace coproc {

class router_source_manager {
public:
    struct topic_state {
        storage::log log;
        absl::flat_hash_set<script_id> scripts;
        model::offset committed{model::model_limits<model::offset>::min()};
        model::offset dirty{model::model_limits<model::offset>::min()};
        mutex mtx;
    };

    using consumers_state
      = absl::flat_hash_map<model::ntp, ss::lw_shared_ptr<topic_state>>;

    explicit router_source_manager(
      ss::sharded<storage::api>&, consumers_state&);

    /// Calling add or remove source will not change the consumers_state, but
    /// will return a future which will be executed when the action is performed
    ss::future<errc> add_source(
      script_id, const model::topic_namespace&, topic_ingestion_policy);
    ss::future<bool> remove_source(script_id);

    /// Executing these methods will drain the deferred* data structures,
    /// modifying the consumers_state cache with the pending changes
    ss::future<> process_additions();
    ss::future<> process_removals();
    void cancel_pending_updates();

private:
    struct source {
        model::topic_namespace tn;
        topic_ingestion_policy tip;
        ss::promise<coproc::errc> promise;
    };

    using deferred_additions = std::vector<std::pair<script_id, source>>;
    using deferred_removals
      = std::vector<std::pair<script_id, ss::promise<bool>>>;

    void do_process_additions(script_id, source&);
    void do_process_removals(script_id, ss::promise<bool>&);

    ss::sharded<storage::api>& _api;
    consumers_state& _sources;
    deferred_additions _additions;
    deferred_removals _removals;
};

} // namespace coproc
