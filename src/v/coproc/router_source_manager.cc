#include "coproc/router_source_manager.h"

#include "coproc/logger.h"
#include "vlog.h"
namespace coproc {

router_source_manager::router_source_manager(
  ss::sharded<storage::api>& api, consumers_state& sources)
  : _api(api)
  , _sources(sources) {}

ss::future<errc> router_source_manager::add_source(
  script_id id, const model::topic_namespace& tns, topic_ingestion_policy p) {
    _additions.emplace_back(id, source{.tn = tns, .tip = p});
    return _additions.back().second.promise.get_future();
}

ss::future<bool> router_source_manager::remove_source(script_id id) {
    _removals.emplace_back(id, ss::promise<bool>());
    return _removals.back().second.get_future();
}

ss::future<> router_source_manager::process_additions() {
    return ss::do_with(
      std::exchange(_additions, deferred_additions()),
      [this](deferred_additions& dfa) {
          return ss::do_for_each(
            dfa.begin(), dfa.end(), [this](deferred_additions::value_type& e) {
                do_process_additions(e.first, e.second);
            });
      });
}

void router_source_manager::do_process_additions(script_id id, source& src) {
    // For now only support the 'latest' policy
    if (!is_valid_ingestion_policy(src.tip)) {
        src.promise.set_value(errc::invalid_ingestion_policy);
        return;
    }
    absl::flat_hash_map<model::ntp, storage::log> logs
      = _api.local().log_mgr().get(src.tn);
    if (logs.empty()) {
        src.promise.set_value(errc::topic_does_not_exist);
        return;
    }

    for (const auto& [ntp, log] : logs) {
        if (auto found = _sources.find(ntp); found == _sources.end()) {
            auto ts = ss::make_lw_shared<topic_state>(
              topic_state{.log = log, .scripts = {id}});
            vassert(
              _sources.emplace(ntp, std::move(ts)).second,
              "Insertion into _sources failed when it was checked to already "
              "not have the existing key: {}",
              ntp);
        } else {
            found->second->scripts.emplace(id);
        }
        vlog(coproclog.info, "Inserted ntp {} id {}", ntp, id);
    }
    src.promise.set_value(errc::success);
}

ss::future<> router_source_manager::process_removals() {
    return ss::do_with(
      std::exchange(_removals, deferred_removals()),
      [this](deferred_removals& drm) {
          return ss::do_for_each(
            drm.begin(), drm.end(), [this](deferred_removals::value_type& e) {
                do_process_removals(e.first, e.second);
            });
      });
}

void router_source_manager::do_process_removals(
  script_id id, ss::promise<bool>& promise) {
    absl::flat_hash_set<model::ntp> deleted;
    for (const auto& p : _sources) {
        auto& scripts = p.second->scripts;
        scripts.erase(id);
        vlog(coproclog.info, "Deleted script id: {}", id);
        if (scripts.empty()) {
            deleted.emplace(p.first);
        }
    }
    // If no more scripts are tracking an ntp, remove the ntp
    absl::erase_if(_sources, [&deleted](const consumers_state::value_type& p) {
        return deleted.contains(p.first);
    });

    promise.set_value(!deleted.empty());
}

void router_source_manager::cancel_pending_updates() {
    for (auto& dfa : std::exchange(_additions, deferred_additions())) {
        dfa.second.promise.set_exception(
          std::make_exception_ptr(std::runtime_error(fmt_with_ctx(
            fmt::format, "Pending addition cancelled: {}", dfa.first))));
    }
    for (auto& drm : std::exchange(_removals, deferred_removals())) {
        drm.second.set_exception(
          std::make_exception_ptr(std::runtime_error(fmt_with_ctx(
            fmt::format, "Pending removal cancelled: {}", drm.first))));
    }
}

} // namespace coproc
