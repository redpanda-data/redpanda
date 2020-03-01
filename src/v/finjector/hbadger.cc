#include "finjector/hbadger.h"

#include "vlog.h"

#include <seastar/util/log.hh>

namespace finjector {

ss::logger log{"fault_injector"};

void honey_badger::register_probe(std::string_view view, probe* p) {
    if (p && p->is_enabled()) {
        vlog(log.trace, "Probe registration: {}", view);
        _probes.insert({ss::sstring(view), p});
    } else {
        vlog(log.debug, "Invalid probe: {}", view);
    }
}
void honey_badger::deregister_probe(std::string_view view) {
    ss::sstring module(view);
    auto it = _probes.find(module);
    if (it != _probes.end()) {
        log.trace("Probe deregistration: {}", view);
        _probes.erase(it);
    }
}
void honey_badger::set_exception(
  const ss::sstring& module, const ss::sstring& point) {
    if (auto it = _probes.find(module); it != _probes.end()) {
        auto& [_, p] = *it;
        vlog(log.debug, "Setting exception probe: {}-{}", module, point);
        p->set_exception(point);
    }
}
void honey_badger::set_delay(
  const ss::sstring& module, const ss::sstring& point) {
    if (auto it = _probes.find(module); it != _probes.end()) {
        auto& [_, p] = *it;
        vlog(log.debug, "Setting delay probe: {}-{}", module, point);
        p->set_delay(point);
    }
}
void honey_badger::set_termination(
  const ss::sstring& module, const ss::sstring& point) {
    if (auto it = _probes.find(module); it != _probes.end()) {
        auto& [_, p] = *it;
        vlog(log.debug, "Setting termination probe: {}-{}", module, point);
        p->set_termination(point);
    }
}
void honey_badger::unset(const ss::sstring& module, const ss::sstring& point) {
    if (auto it = _probes.find(module); it != _probes.end()) {
        auto& [_, p] = *it;
        vlog(log.debug, "Unsetting probes: {}-{}", module, point);
        p->unset(point);
    }
}
std::unordered_map<ss::sstring, std::vector<ss::sstring>>
honey_badger::points() const {
    std::unordered_map<ss::sstring, std::vector<ss::sstring>> retval;
    for (auto& [module, probe] : _probes) {
        retval.insert({module, probe->points()});
    }
    return retval;
}

honey_badger& shard_local_badger() {
    static thread_local honey_badger badger;
    return badger;
}
} // namespace finjector
