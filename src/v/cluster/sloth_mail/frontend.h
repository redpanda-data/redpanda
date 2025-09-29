/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "backend.h"
#include "model/fundamental.h"
#include "ssx/future-util.h"
#include "ssx/single_sharded.h"

namespace cluster::sloth_mail {
class frontend final {
public:
    frontend(model::node_id self, ssx::single_sharded<backend>& backend)
      : _self(self)
      , _backend(backend) {}

    template<mail_kind Kind>
    void dispatch(
      model::node_id destination,
      Kind::key_t&& key,
      Kind::value_t&& value,
      deadline_t deadline) {
        ssx::background = _backend.invoke_on_instance([destination,
                                                       key = std::move(key),
                                                       value = std::move(value),
                                                       deadline](
                                                        backend& b) mutable {
            b.dispatch(destination, std::move(key), std::move(value), deadline);
        });
    }

private:
    model::node_id _self;
    ssx::single_sharded<backend>& _backend;
};
} // namespace cluster::sloth_mail
