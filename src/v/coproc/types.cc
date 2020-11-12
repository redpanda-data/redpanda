// Copyright 2020 Vectorized, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md

#include "types.h"

#include "model/async_adl_serde.h"
#include "model/validation.h"

#include <boost/range/irange.hpp>

namespace coproc {

std::optional<materialized_topic>
make_materialized_topic(const model::topic& topic) {
    // Materialized topics will follow this schema:
    // <source_topic>.$<destination_topic>$
    // Note that dollar signs are not valid kafka topic names but they
    // can be part of a valid coproc materialized_topic name
    const auto found = topic().find('.');
    if (
      found == ss::sstring::npos || found == 0 || found == topic().size() - 1) {
        // If the char '.' is never found, or at a position that would make
        // either of the parts empty, fail out
        return std::nullopt;
    }
    std::string_view src(topic().begin(), found);
    std::string_view dest(
      (topic().begin() + found + 1), topic().size() - found - 1);
    if (!(dest.size() >= 3 && dest[0] == '$' && dest.back() == '$')) {
        // Dest must have at least two dollar chars surronding the topic name
        return std::nullopt;
    }

    // No need for '$' chars in the string_view, its implied by being in the
    // 'dest' mvar
    dest.remove_prefix(1);
    dest.remove_suffix(1);

    model::topic_view src_tv(src);
    model::topic_view dest_tv(dest);
    if (
      model::validate_kafka_topic_name(src_tv).value() != 0
      || model::validate_kafka_topic_name(dest_tv).value() != 0) {
        // The parts of this whole must be valid kafka topics
        return std::nullopt;
    }

    return materialized_topic{.src = src_tv, .dest = dest_tv};
}

std::ostream& operator<<(
  std::ostream& os, enable_copros_request::data::topic_mode topic_mode) {
    os << "<Topic: " << topic_mode.first << " mode: " << (int)topic_mode.second
       << ">";
    return os;
}

} // namespace coproc

namespace reflection {

ss::future<> async_adl<coproc::process_batch_request>::to(
  iobuf& out, coproc::process_batch_request&& r) {
    return async_adl<std::vector<coproc::process_batch_request::data>>{}.to(
      out, std::move(r.reqs));
}

ss::future<coproc::process_batch_request>
async_adl<coproc::process_batch_request>::from(iobuf_parser& in) {
    return async_adl<std::vector<coproc::process_batch_request::data>>{}
      .from(in)
      .then([](std::vector<coproc::process_batch_request::data> reqs) mutable {
          return coproc::process_batch_request{.reqs = std::move(reqs)};
      });
}

ss::future<> async_adl<coproc::process_batch_request::data>::to(
  iobuf& out, coproc::process_batch_request::data&& r) {
    reflection::serialize<std::vector<coproc::script_id>>(
      out, std::move(r.ids));
    reflection::serialize<model::ntp>(out, std::move(r.ntp));
    return async_adl<model::record_batch_reader>{}.to(out, std::move(r.reader));
}

ss::future<coproc::process_batch_request::data>
async_adl<coproc::process_batch_request::data>::from(iobuf_parser& in) {
    auto ids = adl<std::vector<coproc::script_id>>{}.from(in);
    auto ntp = adl<model::ntp>{}.from(in);
    return async_adl<model::record_batch_reader>{}.from(in).then(
      [ids = std::move(ids),
       ntp = std::move(ntp)](model::record_batch_reader rbr) mutable {
          return coproc::process_batch_request::data{
            .ids = std::move(ids),
            .ntp = std::move(ntp),
            .reader = std::move(rbr)};
      });
}

ss::future<> async_adl<coproc::process_batch_reply::data>::to(
  iobuf& out, coproc::process_batch_reply::data&& r) {
    reflection::serialize<coproc::script_id>(out, std::move(r.id));
    reflection::serialize<model::ntp>(out, std::move(r.ntp));
    return async_adl<model::record_batch_reader>{}.to(out, std::move(r.reader));
}

ss::future<coproc::process_batch_reply::data>
async_adl<coproc::process_batch_reply::data>::from(iobuf_parser& in) {
    auto id = adl<coproc::script_id>{}.from(in);
    auto ntp = adl<model::ntp>{}.from(in);
    return async_adl<model::record_batch_reader>{}.from(in).then(
      [id, ntp = std::move(ntp)](model::record_batch_reader rbr) mutable {
          return coproc::process_batch_reply::data{
            .id = id, .ntp = std::move(ntp), .reader = std::move(rbr)};
      });
}

ss::future<> async_adl<coproc::process_batch_reply>::to(
  iobuf& out, coproc::process_batch_reply&& r) {
    return async_adl<std::vector<coproc::process_batch_reply::data>>{}.to(
      out, std::move(r.resps));
}

ss::future<coproc::process_batch_reply>
async_adl<coproc::process_batch_reply>::from(iobuf_parser& in) {
    return async_adl<std::vector<coproc::process_batch_reply::data>>{}
      .from(in)
      .then([](std::vector<coproc::process_batch_reply::data> resps) mutable {
          return coproc::process_batch_reply{.resps = std::move(resps)};
      });
}

} // namespace reflection
