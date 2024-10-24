// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "finjector/hbadger.h"

#include "base/vlog.h"

#include <seastar/util/log.hh>

namespace finjector {

ss::logger log{"fault_injector"};

void honey_badger::register_probe(std::string_view module, probe* p) {
    if (p && p->is_enabled()) {
        vlog(log.trace, "Probe registration: {}", module);
        _probes.insert({std::string_view(module), p});
    } else {
        vlog(log.debug, "Invalid probe: {}", module);
    }
}
void honey_badger::deregister_probe(std::string_view module) {
    auto it = _probes.find(module);
    if (it != _probes.end()) {
        log.trace("Probe deregistration: {}", module);
        _probes.erase(it);
    }
}
void honey_badger::set_exception(
  std::string_view module, std::string_view point) {
    if (auto it = _probes.find(module); it != _probes.end()) {
        auto& [_, p] = *it;
        vlog(log.debug, "Setting exception probe: {}-{}", module, point);
        p->set_exception(point);
    }
}
void honey_badger::set_delay(std::string_view module, std::string_view point) {
    if (auto it = _probes.find(module); it != _probes.end()) {
        auto& [_, p] = *it;
        vlog(log.debug, "Setting delay probe: {}-{}", module, point);
        p->set_delay(point);
    }
}
void honey_badger::set_termination(
  std::string_view module, std::string_view point) {
    if (auto it = _probes.find(module); it != _probes.end()) {
        auto& [_, p] = *it;
        vlog(log.debug, "Setting termination probe: {}-{}", module, point);
        p->set_termination(point);
    }
}
void honey_badger::unset(std::string_view module, std::string_view point) {
    if (auto it = _probes.find(module); it != _probes.end()) {
        auto& [_, p] = *it;
        vlog(log.debug, "Unsetting probes: {}-{}", module, point);
        p->unset(point);
    }
}
absl::node_hash_map<std::string_view, std::vector<std::string_view>>
honey_badger::modules() const {
    absl::node_hash_map<std::string_view, std::vector<std::string_view>> retval;
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
