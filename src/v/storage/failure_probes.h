#pragma once
#include "finjector/hbadger.h"
#include "random/fast_prng.h"
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

    enum class methods : type { append = 1, roll = 2, truncate = 3 };

    type method_for_point(std::string_view point) const final {
        return string_switch<type>(point)
          .match("append", static_cast<type>(methods::append))
          .match("roll", static_cast<type>(methods::roll))
          .match("truncate", static_cast<type>(methods::truncate))
          .default_match(0);
    }

    std::vector<sstring> points() final {
        return {"append", "roll", "truncate"};
    }

    future<> append() {
        if (is_enabled()) {
            return inject_method_failure(methods::append, "append");
        }
        return make_ready_future<>();
    }

    future<> roll() {
        if (is_enabled()) {
            return inject_method_failure(methods::roll, "roll");
        }
        return make_ready_future<>();
    }

    future<> truncate() {
        if (is_enabled()) {
            return inject_method_failure(methods::truncate, "truncate");
        }
        return make_ready_future<>();
    }

private:
    [[gnu::noinline]] future<>
    inject_method_failure(methods method, std::string_view method_name) {
        if (_exception_methods & type(method)) {
            return make_exception_future<>(std::runtime_error(fmt::format(
              "FailureInjector: "
              "storage::log::{}",
              method_name)));
        }
        if (_delay_methods & type(method)) {
            return sleep(std::chrono::milliseconds(_prng() % 50));
        }
        if (_termination_methods & type(method)) {
            std::terminate();
        }
        return make_ready_future<>();
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

    std::vector<sstring> points() final {
        return {"consume"};
    }

    future<> consume() {
        if (is_enabled()) {
            return do_consume();
        }
        return make_ready_future<>();
    }

private:
    [[gnu::noinline]] future<> do_consume() {
        if (_exception_methods & type(methods::consume)) {
            return make_exception_future<>(
              std::runtime_error("FailureInjector: "
                                 "storage::parser::consume"));
        }
        if (_delay_methods & type(methods::consume)) {
            return sleep(std::chrono::milliseconds(_prng() % 50));
        }
        if (_termination_methods & type(methods::consume)) {
            std::terminate();
        }
        return make_ready_future<>();
    }
    fast_prng _prng;
};
}; // namespace storage
