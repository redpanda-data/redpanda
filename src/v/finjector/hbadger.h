/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "base/seastarx.h"

#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>

#include <absl/container/node_hash_map.h>

namespace finjector {

struct probe {
    probe() = default;
    virtual ~probe() = default;
    virtual std::vector<std::string_view> points() = 0;
    // Returns bit(s) corresponding to an injection point.
    // Bits must be distinct across points. Implementations may
    // support a pattern or wildcard for 'point', which may return
    // multiple bits set here.
    virtual uint32_t point_to_bit(std::string_view point) const = 0;

    [[gnu::always_inline]] bool operator()() const {
#ifndef NDEBUG
        // debug
        return true;
#else
        // production
        return false;
#endif
    }

    bool is_enabled() const { return operator()(); }
    void set_exception(std::string_view point) {
        _exception_methods |= point_to_bit(point);
    }
    void set_delay(std::string_view point) {
        _delay_methods |= point_to_bit(point);
    }
    void set_termination(std::string_view point) {
        _termination_methods |= point_to_bit(point);
    }
    void unset(std::string_view point) {
        const uint32_t m = point_to_bit(point);
        _exception_methods &= ~m;
        _delay_methods &= ~m;
        _termination_methods &= ~m;
    }

protected:
    uint32_t _exception_methods = 0;
    uint32_t _delay_methods = 0;
    uint32_t _termination_methods = 0;
};

class honey_badger {
public:
    honey_badger() = default;
    void register_probe(std::string_view module, probe* p);
    void deregister_probe(std::string_view module);

    static constexpr bool is_enabled() {
#ifndef NDEBUG
        // debug
        return true;
#else
        // production
        return false;
#endif
    }

    void set_exception(std::string_view module, std::string_view point);
    void set_delay(std::string_view module, std::string_view point);
    void set_termination(std::string_view module, std::string_view point);
    void unset(std::string_view module, std::string_view point);
    absl::node_hash_map<std::string_view, std::vector<std::string_view>>
    modules() const;

private:
    absl::node_hash_map<std::string_view, probe*> _probes;
};

honey_badger& shard_local_badger();

} // namespace finjector
