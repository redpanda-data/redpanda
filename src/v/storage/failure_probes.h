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
#include "finjector/hbadger.h"
#include "random/fast_prng.h"
#include "strings/string_switch.h"

#include <seastar/core/sleep.hh>

#include <fmt/format.h>

namespace storage {

class log_failure_probes final : public finjector::probe {
public:
    using type = uint32_t;

    static constexpr std::string_view name() {
        return "storage::log::failure_probes";
    }

    enum class methods : type {
        append = 1,
        roll = 2,
        truncate = 4,
        truncate_prefix = 8
    };

    type point_to_bit(std::string_view point) const final {
        return string_switch<type>(point)
          .match("append", static_cast<type>(methods::append))
          .match("roll", static_cast<type>(methods::roll))
          .match("truncate", static_cast<type>(methods::truncate))
          .match("truncate_prefix", static_cast<type>(methods::truncate_prefix))
          .default_match(0);
    }

    std::vector<std::string_view> points() final {
        return {"append", "roll", "truncate"};
    }

    ss::future<> append() {
        if (is_enabled()) {
            return inject_method_failure(methods::append, "append");
        }
        return ss::make_ready_future<>();
    }

    ss::future<> roll() {
        if (is_enabled()) {
            return inject_method_failure(methods::roll, "roll");
        }
        return ss::make_ready_future<>();
    }

    ss::future<> truncate() {
        if (is_enabled()) {
            return inject_method_failure(methods::truncate, "truncate");
        }
        return ss::make_ready_future<>();
    }
    ss::future<> truncate_prefix() {
        if (is_enabled()) {
            return inject_method_failure(
              methods::truncate_prefix, "truncate_prefix");
        }
        return ss::make_ready_future<>();
    }

private:
    [[gnu::noinline]] ss::future<>
    inject_method_failure(methods method, std::string_view method_name) {
        if (_exception_methods & type(method)) {
            return ss::make_exception_future<>(std::runtime_error(fmt::format(
              "FailureInjector: "
              "storage::log::{}",
              method_name)));
        }
        if (_delay_methods & type(method)) {
            return ss::sleep(std::chrono::milliseconds(_prng() % 50));
        }
        if (_termination_methods & type(method)) {
            std::terminate();
        }
        return ss::make_ready_future<>();
    }
    fast_prng _prng;
};

}; // namespace storage
