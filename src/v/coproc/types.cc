// Copyright 2020 Vectorized, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

#include "coproc/types.h"

#include "coproc/errc.h"
#include "model/async_adl_serde.h"
#include "reflection/std/vector.h"

#include <boost/range/irange.hpp>

namespace coproc {

std::ostream& operator<<(std::ostream& os, const enable_response_code erc) {
    switch (erc) {
    case enable_response_code::success:
        os << "success";
        break;
    case enable_response_code::internal_error:
        os << "internal_error";
        break;
    case enable_response_code::script_id_already_exists:
        os << "script_id_already_exists";
        break;
    case enable_response_code::script_contains_invalid_topic:
        os << "script_contains_invalid_topic";
        break;
    case enable_response_code::script_contains_no_topics:
        os << "script_contains_no_topics";
        break;
    default:
        __builtin_unreachable();
    };
    return os;
}

std::ostream& operator<<(std::ostream& os, const disable_response_code drc) {
    switch (drc) {
    case disable_response_code::success:
        os << "success";
        break;
    case disable_response_code::internal_error:
        os << "internal_error";
        break;
    case disable_response_code::script_id_does_not_exist:
        os << "script_id_does_not_exist";
        break;
    default:
        __builtin_unreachable();
    };
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
    reflection::serialize(out, r.id, std::move(r.source), std::move(r.ntp));
    return async_adl<std::optional<model::record_batch_reader>>{}.to(
      out, std::move(r.reader));
}

ss::future<coproc::process_batch_reply::data>
async_adl<coproc::process_batch_reply::data>::from(iobuf_parser& in) {
    auto id = adl<coproc::script_id>{}.from(in);
    auto source = adl<model::ntp>{}.from(in);
    auto ntp = adl<model::ntp>{}.from(in);
    return async_adl<std::optional<model::record_batch_reader>>{}.from(in).then(
      [id, source = std::move(source), ntp = std::move(ntp)](
        std::optional<model::record_batch_reader> rbr) mutable {
          return coproc::process_batch_reply::data{
            .id = id,
            .source = std::move(source),
            .ntp = std::move(ntp),
            .reader = std::move(rbr)};
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
