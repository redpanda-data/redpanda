#pragma once
#include "kafka/errors.h"
#include "kafka/requests/fwd.h"
#include "kafka/types.h"

#include <seastar/core/future.hh>

namespace kafka {

struct describe_groups_api final {
    static constexpr const char* name = "describe groups";
    static constexpr api_key key = api_key(15);
    static constexpr api_version min_supported = api_version(0);
    static constexpr api_version max_supported = api_version(0);
};

} // namespace kafka
