// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once
#include "bytes/bytes.h"
#include "outcome.h"
#include "vassert.h"

#include <memory>

namespace kafka {

/*
 * Generic SASL mechanism interface.
 */
class sasl_mechanism {
public:
    virtual ~sasl_mechanism() = default;
    virtual bool complete() const = 0;
    virtual bool failed() const = 0;
    virtual result<bytes> authenticate(bytes_view) = 0;
};

/*
 * SASL server protocol manager.
 */
class sasl_server final {
public:
    enum class sasl_state {
        initial,
        handshake,
        authenticate,
        complete,
        failed,
    };

    explicit sasl_server(sasl_state state)
      : _state(state) {}

    sasl_state state() const { return _state; }
    void set_state(sasl_state state) { _state = state; }

    bool complete() const { return _state == sasl_state::complete; }

    bool has_mechanism() const { return bool(_mechanism); }
    sasl_mechanism& mechanism() { return *_mechanism; }

    result<bytes> authenticate(bytes data) {
        return _mechanism->authenticate(data);
    }

    void set_mechanism(std::unique_ptr<sasl_mechanism> m) {
        vassert(!_mechanism, "Cannot change mechanism");
        _mechanism = std::move(m);
    }

private:
    sasl_state _state;
    std::unique_ptr<sasl_mechanism> _mechanism;
};

}; // namespace kafka
