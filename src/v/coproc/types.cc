// Copyright 2020 Vectorized, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md

#include "coproc/types.h"

#include "coproc/errc.h"
#include "model/async_adl_serde.h"

#include <boost/range/irange.hpp>

namespace coproc {
std::ostream& operator<<(std::ostream& os, const enable_response_code erc) {
    const auto errc = make_error_code(
      static_cast<coproc::errc>(static_cast<int8_t>(erc)));
    os << errc.message();
    return os;
}

std::ostream& operator<<(std::ostream& os, const disable_response_code drc) {
    const auto errc = make_error_code(
      static_cast<coproc::errc>(static_cast<int8_t>(drc)));
    os << errc.message();
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
