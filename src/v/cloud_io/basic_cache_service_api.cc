/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_io/basic_cache_service_api.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/core/manual_clock.hh>

namespace cloud_io {

std::ostream& operator<<(std::ostream& o, cache_element_status s) {
    switch (s) {
    case cache_element_status::available:
        o << "cache_element_available";
        break;
    case cache_element_status::not_available:
        o << "cache_element_not_available";
        break;
    case cache_element_status::in_progress:
        o << "cache_element_in_progress";
        break;
    }
    return o;
}

template<class Clock>
void basic_space_reservation_guard<Clock>::wrote_data(
  uint64_t written_bytes, size_t written_objects) {
    // Release the reservation, and update usage stats for how much we actually
    // wrote.
    _cache.reserve_space_release(
      _bytes, _objects, written_bytes, written_objects);

    // This reservation is now used up.
    _bytes = 0;
    _objects = 0;
}

template<class Clock>
basic_space_reservation_guard<Clock>::~basic_space_reservation_guard() {
    if (_bytes || _objects) {
        // This is the case of a failed write, where wrote_data was never
        // called: release the reservation and do not acquire any space
        // usage for the written data (there should be none).
        _cache.reserve_space_release(_bytes, _objects, 0, 0);
    }
}

template class basic_space_reservation_guard<ss::lowres_clock>;
template class basic_space_reservation_guard<ss::manual_clock>;

} // namespace cloud_io
