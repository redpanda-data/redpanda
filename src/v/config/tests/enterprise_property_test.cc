#include "config/bounded_property.h"
#include "config/configuration.h"

#include <seastar/testing/thread_test_case.hh>

namespace {

struct test_config : public config::config_store {
    config::enterprise<config::property<bool>> enterprise_bool;
    config::enterprise<config::enum_property<ss::sstring>> enterprise_enum;
    config::enterprise<config::property<std::vector<ss::sstring>>>
      enterprise_str_vec;

    test_config()
      : enterprise_bool(
          *this,
          std::vector<bool>{},
          "enterprise_bool",
          "An enterprise-only bool config",
          config::base_property::metadata{},
          false,
          config::property<bool>::noop_validator,
          std::nullopt)
      , enterprise_enum(
          *this,
          std::vector<ss::sstring>{"bar"},
          "enterprise_enum",
          "An enterprise-only enum property",
          config::base_property::metadata{},
          "foo",
          std::vector<ss::sstring>{"foo", "bar", "baz"})
      , enterprise_str_vec(
          *this,
          std::vector<ss::sstring>{"GSSAPI"},
          "enterprise_str_vec",
          "An enterprise-only vector of strings") {}
};
} // namespace
