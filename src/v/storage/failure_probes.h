#pragma once
#include "finjector/hbadger.h"
#include "random/fast_prng.h"
#include "seastarx.h"
#include "utils/string_switch.h"

#include <seastar/core/sleep.hh>

#include <fmt/format.h>

namespace storage {

class log_failure_probes final : public finjector::probe {
public:
    using type = int8_t;

    static constexpr std::string_view name() {
        return "storage::log::failure_probes";
    }

    enum class methods : type {
        append = 1,
        roll = 2,
        truncate = 3,
        truncate_prefix = 4
    };

    type method_for_point(std::string_view point) const final {
        return string_switch<type>(point)
          .match("append", static_cast<type>(methods::append))
          .match("roll", static_cast<type>(methods::roll))
          .match("truncate", static_cast<type>(methods::truncate))
          .match("truncate_prefix", static_cast<type>(methods::truncate_prefix))
          .default_match(0);
    }

    std::vector<ss::sstring> points() final {
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

class parser_failure_probes final : public finjector::probe {
public:
    using type = int8_t;
    static constexpr std::string_view name() {
        return "storage::parser::failure_probes";
    }

    enum class methods : type { consume = 1 };

    parser_failure_probes() = default;
    parser_failure_probes(parser_failure_probes&&) = default;
    parser_failure_probes& operator=(parser_failure_probes&&) = default;

    type method_for_point(std::string_view point) const final {
        return point == "consume" ? static_cast<type>(methods::consume) : 0;
    }

    std::vector<ss::sstring> points() final { return {"consume"}; }

    ss::future<> consume() {
        if (is_enabled()) {
            return do_consume();
        }
        return ss::make_ready_future<>();
    }

private:
    [[gnu::noinline]] ss::future<> do_consume() {
        if (_exception_methods & type(methods::consume)) {
            return ss::make_exception_future<>(
              std::runtime_error("FailureInjector: "
                                 "storage::parser::consume"));
        }
        if (_delay_methods & type(methods::consume)) {
            return ss::sleep(std::chrono::milliseconds(_prng() % 50));
        }
        if (_termination_methods & type(methods::consume)) {
            std::terminate();
        }
        return ss::make_ready_future<>();
    }
    fast_prng _prng;
};
}; // namespace storage
