/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "datalake/coordinator/types.h"

#include "utils/to_string.h"

namespace datalake::coordinator {

std::ostream& operator<<(std::ostream& o, const errc& errc) {
    switch (errc) {
    case errc::ok:
        o << "errc::ok";
        break;
    case errc::coordinator_topic_not_exists:
        o << "errc::coordinator_topic_not_exists";
        break;
    case errc::not_leader:
        o << "errc::not_leader";
        break;
    case errc::timeout:
        o << "errc::timeout";
        break;
    case errc::fenced:
        o << "errc::fenced";
        break;
    case errc::stale:
        o << "errc::stale";
        break;
    case errc::concurrent_requests:
        o << "errc::concurrent_requests";
        break;
    }
    return o;
}

std::ostream&
operator<<(std::ostream& o, const add_translated_data_files_reply& reply) {
    fmt::print(o, "{{errc: {}}}", reply.errc);
    return o;
}

std::ostream&
operator<<(std::ostream& o, const add_translated_data_files_request& request) {
    fmt::print(
      o,
      "{{partition: {}, files: {}, translation term: {}}}",
      request.tp,
      request.ranges,
      request.translator_term);
    return o;
}

std::ostream&
operator<<(std::ostream& o, const fetch_latest_translated_offset_reply& reply) {
    fmt::print(
      o, "{{errc: {}, offset: {}}}", reply.errc, reply.last_added_offset);
    return o;
}

std::ostream& operator<<(
  std::ostream& o, const fetch_latest_translated_offset_request& request) {
    fmt::print(o, "{{partition: {}}}", request.tp);
    return o;
}
} // namespace datalake::coordinator
