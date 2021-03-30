/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "config/tls_config.h"
#include "seastarx.h"

namespace config {
struct endpoint_tls_config {
    ss::sstring name;
    tls_config config;

    friend std::ostream&
    operator<<(std::ostream& o, const endpoint_tls_config& cfg) {
        fmt::print(o, "{{name: {}, tls_config: {}}}", cfg.name, cfg.config);
        return o;
    }

    static std::optional<ss::sstring> validate(const endpoint_tls_config& ec) {
        return tls_config::validate(ec.config);
    }
    static std::optional<ss::sstring>
    validate_many(const std::vector<endpoint_tls_config>& v) {
        for (auto& ec : v) {
            auto err = tls_config::validate(ec.config);
            if (err) {
                return err;
            }
        }
        return std::nullopt;
    }
};
} // namespace config
