#pragma once

#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/record.h"
#include "reflection/adl.h"

#include <seastar/net/inet_address.hh>
#include <seastar/net/ip.hh>
#include <seastar/net/socket_defs.hh>

namespace reflection {

template<>
struct adl<model::topic> {
    void to(iobuf& out, model::topic&& t) {
        auto str = ss::sstring(t);
        reflection::serialize(out, str);
    }

    model::topic from(iobuf_parser& in) {
        auto str = adl<ss::sstring>{}.from(in);
        return model::topic(std::move(str));
    }
};

template<>
struct adl<model::ns> {
    void to(iobuf& out, model::ns&& t) {
        auto str = ss::sstring(t);
        reflection::serialize(out, str);
    }

    model::ns from(iobuf_parser& in) {
        auto str = adl<ss::sstring>{}.from(in);
        return model::ns(std::move(str));
    }
};

template<>
struct adl<model::topic_partition> {
    void to(iobuf& out, model::topic_partition&& t) {
        auto str = ss::sstring(t.topic);
        reflection::serialize(out, std::move(str), std::move(t.partition));
    }

    model::topic_partition from(iobuf_parser& in) {
        auto topic = model::topic(adl<ss::sstring>{}.from(in));
        auto id = adl<model::partition_id>{}.from(in);
        return model::topic_partition{std::move(topic), id};
    }
};

template<>
struct adl<model::ntp> {
    void to(iobuf& out, model::ntp&& ntp) {
        reflection::serialize(out, ntp.ns, std::move(ntp.tp));
    }
    model::ntp from(iobuf_parser& in) {
        auto ns = adl<model::ns>{}.from(in);
        auto partition = adl<model::topic_partition>{}.from(in);
        return model::ntp{std::move(ns), std::move(partition)};
    }
};

template<>
struct adl<unresolved_address> {
    void to(iobuf& out, const unresolved_address& address) {
        adl<ss::sstring>{}.to(out, address.host());
        adl<uint16_t>{}.to(out, address.port());
    }

    unresolved_address from(iobuf_parser& in) {
        auto host = adl<ss::sstring>{}.from(in);
        auto port = adl<uint16_t>{}.from(in);
        return unresolved_address(std::move(host), port);
    }
};

template<>
struct adl<model::broker_properties> {
    using type = std::vector<std::pair<ss::sstring, ss::sstring>>;

    void to(iobuf& out, const model::broker_properties& prop) {
        adl<uint32_t>{}.to(out, prop.cores);
        adl<uint32_t>{}.to(out, prop.available_memory);
        adl<uint32_t>{}.to(out, prop.available_disk);
        adl<std::vector<ss::sstring>>{}.to(out, prop.mount_paths);
        type vec;
        vec.reserve(prop.etc_props.size());
        std::move(
          std::begin(prop.etc_props),
          std::end(prop.etc_props),
          std::back_inserter(vec));
        serialize(out, std::move(vec));
    }

    model::broker_properties from(iobuf_parser& in) {
        auto cores = adl<uint32_t>{}.from(in);
        auto mem = adl<uint32_t>{}.from(in);
        auto disk = adl<uint32_t>{}.from(in);
        auto paths = adl<std::vector<ss::sstring>>{}.from(in);
        auto vec = adl<type>{}.from(in);
        auto props = std::unordered_map<ss::sstring, ss::sstring>(
          vec.begin(), vec.end());
        return model::broker_properties{
          cores, mem, disk, std::move(paths), std::move(props)};
    }
};

template<>
struct adl<model::broker> {
    void to(iobuf& out, model::broker&& r) {
        adl<model::node_id>{}.to(out, r.id());
        adl<unresolved_address>{}.to(out, r.kafka_api_address());
        adl<unresolved_address>{}.to(out, r.rpc_address());
        adl<std::optional<ss::sstring>>{}.to(out, r.rack());
        adl<model::broker_properties>{}.to(out, r.properties());
    }

    model::broker from(iobuf_parser& in) {
        auto id = adl<model::node_id>{}.from(in);
        auto kafka_adrs = adl<unresolved_address>{}.from(in);
        auto rpc_adrs = adl<unresolved_address>{}.from(in);
        auto rack = adl<std::optional<ss::sstring>>{}.from(in);
        auto etc_props = adl<model::broker_properties>{}.from(in);
        return model::broker{id, kafka_adrs, rpc_adrs, rack, etc_props};
    }
};

template<>
struct adl<model::record> {
    void to(iobuf& ref, model::record&& record) {
        reflection::serialize(
          ref,
          record.size_bytes(),
          record.attributes().value(),
          record.timestamp_delta(),
          record.offset_delta(),
          record.share_key(),
          record.share_packed_value_and_headers());
    }

    model::record from(iobuf_parser& in) {
        auto sz_bytes = adl<uint32_t>{}.from(in);
        using attr_t = model::record_attributes::type;
        auto attributes = model::record_attributes(adl<attr_t>{}.from(in));
        auto timestamp = adl<int32_t>{}.from(in);
        auto offset_data = adl<int32_t>{}.from(in);
        auto key = adl<iobuf>{}.from(in);
        auto value_and_headers = adl<iobuf>{}.from(in);
        return model::record{sz_bytes,
                             attributes,
                             timestamp,
                             offset_data,
                             std::move(key),
                             std::move(value_and_headers)};
    }
};

template<>
struct adl<model::record_batch_header> {
    void to(iobuf& out, model::record_batch_header&& r) {
        reflection::serialize(
          out,
          r.size_bytes,
          r.base_offset,
          r.type,
          r.crc,
          r.attrs.value(),
          r.last_offset_delta,
          r.first_timestamp.value(),
          r.max_timestamp.value(),
          r.ctx.term);
    }

    model::record_batch_header from(iobuf_parser& in) {
        auto sz = adl<uint32_t>{}.from(in);
        auto off = adl<model::offset>{}.from(in);
        auto type = adl<model::record_batch_type>{}.from(in);
        auto crc = adl<int32_t>{}.from(in);
        using attr_t = model::record_batch_attributes::type;
        auto attrs = model::record_batch_attributes(adl<attr_t>{}.from(in));
        auto delta = adl<int32_t>{}.from(in);
        using tmstmp_t = model::timestamp::type;
        auto first = model::timestamp(adl<tmstmp_t>{}.from(in));
        auto max = model::timestamp(adl<tmstmp_t>{}.from(in));
        auto term_id = adl<model::term_id>{}.from(in);
        return model::record_batch_header{
          sz,
          off,
          type,
          crc,
          attrs,
          delta,
          first,
          max,
          model::record_batch_header::context{.term = term_id}};
    }
};

struct batch_header {
    model::record_batch_header bhdr;
    uint32_t batch_size;
    int8_t is_compressed;
};

template<>
struct adl<batch_header> {
    void to(iobuf& out, batch_header&& header) {
        reflection::serialize(
          out, header.bhdr, header.batch_size, header.is_compressed);
        /*
        This is ambiguous --> Fron adl.h line 92
        to(iobuf& out, type& t) and
        to(iobuf& out, type t)
        adl<model::record_batch_header>{}.to(out, header.bhdr);
        adl<uint32_t>{}.to(out, header.batch_size);
        adl<int8_t>{}.to(out, header.is_compressed); */
    }

    batch_header from(iobuf_parser& in) {
        auto record = adl<model::record_batch_header>{}.from(in);
        auto batch_sz = adl<uint32_t>{}.from(in);
        auto is_compressed = adl<int8_t>{}.from(in);
        return batch_header{std::move(record), batch_sz, is_compressed};
    }
};

template<>
struct adl<model::record_batch> {
    void to(iobuf& out, model::record_batch&& batch) {
        batch_header hdr{
          .bhdr = batch.release_header(),
          .batch_size = batch.size(),
          .is_compressed = static_cast<int8_t>(batch.compressed() ? 1 : 0)};
        reflection::serialize(out, std::move(hdr));
        if (batch.compressed()) {
            reflection::serialize(out, std::move(batch).release().release());
            return;
        }
        for (model::record& r : batch) {
            reflection::serialize(out, std::move(r));
        }
    }

    model::record_batch from(iobuf_parser& in) {
        auto hdr = reflection::adl<batch_header>{}.from(in);
        if (hdr.is_compressed == 1) {
            auto io = reflection::adl<iobuf>{}.from(in);

            return model::record_batch(
              std::move(hdr.bhdr),
              model::record_batch::compressed_records(
                hdr.batch_size, std::move(io)));
        }
        auto recs = std::vector<model::record>{};
        recs.reserve(hdr.batch_size);
        for (int i = 0; i < hdr.batch_size; ++i) {
            recs.push_back(adl<model::record>{}.from(in));
        }

        return model::record_batch(std::move(hdr.bhdr), std::move(recs));
    }
};

} // namespace reflection
