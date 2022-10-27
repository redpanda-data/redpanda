#include "utils/uuid.h"

#include "bytes/details/out_of_range.h"

#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <fmt/ostream.h>

#include <ostream>

uuid_t::uuid_t(const std::vector<uint8_t>& v)
  : _uuid({}) {
    if (v.size() != length) {
        details::throw_out_of_range(
          "Expected size of {} for UUID, got {}", length, v.size());
    }
    std::copy(v.begin(), v.end(), _uuid.begin());
}

uuid_t uuid_t::create() {
    static thread_local boost::uuids::random_generator uuid_gen;
    return uuid_t(uuid_gen());
}

std::ostream& operator<<(std::ostream& os, const uuid_t& u) {
    return os << fmt::format("{}", u._uuid);
}
