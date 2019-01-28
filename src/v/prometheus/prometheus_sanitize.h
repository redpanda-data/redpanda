#pragma once

#include <seastar/core/sstring.hh>

namespace v {

struct prometheus_sanitize {
  static seastar::sstring
  metrics_name(seastar::sstring n) {
    for (auto i = 0u; i < n.size(); ++i) {
      if (!std::isalnum(n[i])) n[i] = '_';
    }
    return n;
  }
};

}  // namespace v
