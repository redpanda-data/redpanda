#include "finjector/hbadger.h"

namespace finjector {

void honey_badger::register_probe(std::string_view view, shared_ptr<probe> p) {
    _probes.insert({sstring(view), p});
}
void honey_badger::deregister_probe(std::string_view view) {
    sstring module(view);
    auto it = _probes.find(module);
    if (it != _probes.end()) {
        _probes.erase(it);
    }
}
void honey_badger::set_exception(const sstring& module, const sstring& point) {
    if (auto it = _probes.find(module); it != _probes.end()) {
        auto& [_, p] = *it;
        p->set_exception(point);
    }
}
void honey_badger::set_delay(const sstring& module, const sstring& point) {
    if (auto it = _probes.find(module); it != _probes.end()) {
        auto& [_, p] = *it;
        p->set_delay(point);
    }
}
void honey_badger::set_termination(
  const sstring& module, const sstring& point) {
    if (auto it = _probes.find(module); it != _probes.end()) {
        auto& [_, p] = *it;
        p->set_termination(point);
    }
}
void honey_badger::unset(const sstring& module, const sstring& point) {
    if (auto it = _probes.find(module); it != _probes.end()) {
        auto& [_, p] = *it;
        p->unset(point);
    }
}
std::unordered_map<sstring, std::vector<sstring>> honey_badger::points() const {
    std::unordered_map<sstring, std::vector<sstring>> retval;
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
