#pragma once

#include "seastarx.h"
#include "utils/unresolved_address.h"

#include <model/metadata.h>

#include <cstdint>

namespace config {
struct seed_server {
    model::node_id id;
    unresolved_address addr;
};
} // namespace config

namespace std {
static inline ostream& operator<<(ostream& o, const config::seed_server& s) {
    fmt::print(o, "node_id: {}, addr: {}", s.id, s.addr);
    return o;
}
} // namespace std
