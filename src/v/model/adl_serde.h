/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "bytes/iobuf_parser.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/record.h"
#include "model/timeout_clock.h"
#include "reflection/adl.h"
#include "tristate.h"

#include <seastar/net/inet_address.hh>
#include <seastar/net/ip.hh>
#include <seastar/net/socket_defs.hh>

namespace reflection {

template<>
struct adl<model::timeout_clock::duration> {
    using duration = model::timeout_clock::duration;

    void to(iobuf& out, duration dur) {
        adl<std::chrono::milliseconds>{}.to(
          out, std::chrono::duration_cast<std::chrono::milliseconds>(dur));
    }

    model::timeout_clock::duration from(iobuf_parser& in) {
        return std::chrono::duration_cast<duration>(
          adl<std::chrono::milliseconds>{}.from(in));
    }
};

template<>
struct adl<model::topic> {
    void to(iobuf& out, model::topic&& t);
    model::topic from(iobuf_parser& in);
};

template<>
struct adl<model::ns> {
    void to(iobuf& out, model::ns&& t);
    model::ns from(iobuf_parser& in);
};

template<>
struct adl<model::topic_partition> {
    void to(iobuf& out, model::topic_partition&& t);
    model::topic_partition from(iobuf_parser& in);
};

template<>
struct adl<model::ntp> {
    void to(iobuf& out, model::ntp&& ntp);
    model::ntp from(iobuf_parser& in);
};

template<>
struct adl<net::unresolved_address> {
    void to(iobuf& out, const net::unresolved_address& address);
    net::unresolved_address from(iobuf_parser& in);
};

template<>
struct adl<model::broker_properties> {
    using type = std::vector<std::pair<ss::sstring, ss::sstring>>;
    void to(iobuf& out, const model::broker_properties& prop);
    model::broker_properties from(iobuf_parser& in);
};

template<>
struct adl<model::broker_endpoint> {
    void to(iobuf&, model::broker_endpoint&&);
    model::broker_endpoint from(iobuf_parser&);
};

template<>
struct adl<model::broker> {
    void to(iobuf& out, model::broker&& r);
    model::broker from(iobuf_parser& in);
};

template<>
struct adl<model::internal::broker_v0> {
    void to(iobuf& out, model::internal::broker_v0&& r);
    model::internal::broker_v0 from(iobuf_parser& in);
};

// TODO: optimize this transmition with varints
template<>
struct adl<model::record> {
    void to(iobuf& ref, model::record&& record);
    model::record from(iobuf_parser& in);
};

template<>
struct adl<model::record_batch_header> {
    void to(iobuf& out, model::record_batch_header&& r);
    template<typename Parser>
    model::record_batch_header parse_from(Parser& in);
    model::record_batch_header from(iobuf_const_parser& in) {
        return parse_from(in);
    }

    model::record_batch_header from(iobuf_parser& in) { return parse_from(in); }
};

struct batch_header {
    model::record_batch_header bhdr;
    int8_t is_compressed;
};

template<>
struct adl<batch_header> {
    void to(iobuf& out, batch_header&& header);
    batch_header from(iobuf_parser& in);
};

template<>
struct adl<model::record_batch> {
    void to(iobuf& out, model::record_batch&& batch);
    model::record_batch from(iobuf_parser& in);
};

template<>
struct adl<model::partition_metadata> {
    void to(iobuf& out, model::partition_metadata&& md);
    model::partition_metadata from(iobuf_parser& in);
};

template<>
struct adl<model::topic_namespace> {
    void to(iobuf& out, model::topic_namespace&& md);
    model::topic_namespace from(iobuf_parser& in);
};

template<>
struct adl<model::topic_metadata> {
    void to(iobuf& out, model::topic_metadata&& md);
    model::topic_metadata from(iobuf_parser& in);
};

template<typename T>
struct adl<tristate<T>> {
    void to(iobuf& out, tristate<T>&& t) {
        if (t.is_disabled()) {
            adl<int8_t>{}.to(out, -1);
            return;
        }
        if (!t.has_optional_value()) {
            adl<int8_t>{}.to(out, 0);
            return;
        }
        adl<int8_t>{}.to(out, 1);
        adl<T>{}.to(out, std::move(t.value()));
    }

    tristate<T> from(iobuf b) {
        return reflection::from_iobuf<tristate<T>>(std::move(b));
    }
    tristate<T> from(iobuf_parser& buf) {
        auto state = adl<int8_t>{}.from(buf);
        if (state == -1) {
            return tristate<T>{};
        }
        if (state == 0) {
            return tristate<T>(std::nullopt);
        }

        return tristate<T>(adl<T>{}.from(buf));
    }
};

template<>
struct adl<model::producer_identity> {
    void to(iobuf& out, model::producer_identity&& md);
    model::producer_identity from(iobuf_parser& in);
};

template<typename Parser>
model::record_batch_header
adl<model::record_batch_header>::parse_from(Parser& in) {
    auto header_crc = adl<uint32_t>{}.from(in);
    auto sz = adl<int32_t>{}.from(in);
    auto off = adl<model::offset>{}.from(in);
    auto type = adl<model::record_batch_type>{}.from(in);
    auto crc = adl<int32_t>{}.from(in);
    using attr_t = model::record_batch_attributes::type;
    auto attrs = model::record_batch_attributes(adl<attr_t>{}.from(in));
    auto delta = adl<int32_t>{}.from(in);
    using tmstmp_t = model::timestamp::type;
    auto first = model::timestamp(adl<tmstmp_t>{}.from(in));
    auto max = model::timestamp(adl<tmstmp_t>{}.from(in));
    auto producer_id = adl<int64_t>{}.from(in);
    auto producer_epoch = adl<int16_t>{}.from(in);
    auto base_sequence = adl<int32_t>{}.from(in);
    auto record_count = adl<int32_t>{}.from(in);
    auto term_id = adl<model::term_id>{}.from(in);
    return model::record_batch_header{
      .header_crc = header_crc,
      .size_bytes = sz,
      .base_offset = off,
      .type = type,
      .crc = crc,
      .attrs = attrs,
      .last_offset_delta = delta,
      .first_timestamp = first,
      .max_timestamp = max,
      .producer_id = producer_id,
      .producer_epoch = producer_epoch,
      .base_sequence = base_sequence,
      .record_count = record_count,
      .ctx = model::record_batch_header::context(term_id, ss::this_shard_id())};
}
} // namespace reflection
