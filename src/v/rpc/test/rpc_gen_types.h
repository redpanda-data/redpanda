#pragma once

#include <seastar/core/sstring.hh>

#include <cstdint>

namespace cycling {
struct ultimate_cf_slx {
    int x = 42;
};
struct nairo_quintana {
    int x = 43;
};
struct san_francisco {
    int x = 44;
};
struct mount_tamalpais {
    int x = 45;
};
} // namespace cycling

namespace echo {
struct echo_req {
    ss::sstring str;
};

struct echo_resp {
    ss::sstring str;
};
} // namespace echo
