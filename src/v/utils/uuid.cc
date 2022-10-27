#include "utils/uuid.h"

#include <boost/uuid/random_generator.hpp>

#include <ostream>

uuid_t uuid_t::create() {
    static thread_local boost::uuids::random_generator uuid_gen;
    return uuid_t(uuid_gen());
}

std::ostream& operator<<(std::ostream& os, const uuid_t& u) {
    return os << fmt::format("{}", u._uuid);
}
