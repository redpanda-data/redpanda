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

// TODO: optimize this transmition with varints
template<>
struct adl<model::record> {
    void to(iobuf& ref, model::record&& record) {
        reflection::serialize(
          ref,
          record.size_bytes(),
          record.attributes().value(),
          record.timestamp_delta(),
          record.offset_delta(),
          record.key_size(),
          record.share_key(),
          record.value_size(),
          record.share_value(),
          int32_t(record.headers().size()));
        for (auto& h : record.headers()) {
            reflection::serialize(
              ref,
              h.key_size(),
              h.share_key(),
              h.value_size(),
              h.share_value());
        }
    }

    model::record from(iobuf_parser& in) {
        auto sz_bytes = adl<uint32_t>{}.from(in);
        using attr_t = model::record_attributes::type;
        auto attributes = model::record_attributes(adl<attr_t>{}.from(in));
        auto timestamp = adl<int32_t>{}.from(in);
        auto offset_data = adl<int32_t>{}.from(in);
        auto key_len = adl<int32_t>{}.from(in);
        auto key = adl<iobuf>{}.from(in);
        auto value_len = adl<int32_t>{}.from(in);
        auto value = adl<iobuf>{}.from(in);
        auto hdr_size = adl<int32_t>{}.from(in);
        std::vector<model::record_header> headers;
        headers.reserve(hdr_size);
        for (int i = 0; i < hdr_size; ++i) {
            auto hkey_len = adl<int32_t>{}.from(in);
            auto hkey = adl<iobuf>{}.from(in);
            auto hvalue_len = adl<int32_t>{}.from(in);
            auto hvalue = adl<iobuf>{}.from(in);
            headers.emplace_back(model::record_header(
              hkey_len, std::move(hkey), hvalue_len, std::move(hvalue)));
        }
        return model::record(
          sz_bytes,
          attributes,
          timestamp,
          offset_data,
          key_len,
          std::move(key),
          value_len,
          std::move(value),
          std::move(headers));
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
          r.producer_id,
          r.producer_epoch,
          r.base_sequence,
          r.record_count,
          r.ctx.term);
    }

    model::record_batch_header from(iobuf_parser& in) {
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
          sz,
          off,
          type,
          crc,
          attrs,
          delta,
          first,
          max,
          producer_id,
          producer_epoch,
          base_sequence,
          record_count,
          model::record_batch_header::context{.term = term_id}};
    }
};

struct batch_header {
    model::record_batch_header bhdr;
    int8_t is_compressed;
};

template<>
struct adl<batch_header> {
    void to(iobuf& out, batch_header&& header) {
        reflection::serialize(out, header.bhdr, header.is_compressed);
    }

    batch_header from(iobuf_parser& in) {
        auto record = adl<model::record_batch_header>{}.from(in);
        auto is_compressed = adl<int8_t>{}.from(in);
        return batch_header{record, is_compressed};
    }
};

template<>
struct adl<model::record_batch> {
    void to(iobuf& out, model::record_batch&& batch) {
        batch_header hdr{
          .bhdr = batch.header(),
          .is_compressed = static_cast<int8_t>(batch.compressed() ? 1 : 0)};
        reflection::serialize(out, hdr);
        if (batch.compressed()) {
            reflection::serialize(out, std::move(batch).release());
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
            return model::record_batch(hdr.bhdr, std::move(io));
        }
        auto recs = std::vector<model::record>{};
        recs.reserve(hdr.bhdr.record_count);
        for (int i = 0; i < hdr.bhdr.record_count; ++i) {
            recs.push_back(adl<model::record>{}.from(in));
        }
        return model::record_batch(hdr.bhdr, std::move(recs));
    }
};

template<>
struct adl<model::partition_metadata> {
    void to(iobuf& out, model::partition_metadata&& md) {
        reflection::serialize(
          out, md.id, std::move(md.replicas), md.leader_node);
    }

    model::partition_metadata from(iobuf_parser& in) {
        auto md = model::partition_metadata(
          reflection::adl<model::partition_id>{}.from(in));
        md.replicas = reflection::adl<std::vector<model::broker_shard>>{}.from(
          in);
        md.leader_node = reflection::adl<std::optional<model::node_id>>{}.from(
          in);
        return md;
    };
};

template<>
struct adl<model::topic_metadata> {
    void to(iobuf& out, model::topic_metadata&& md) {
        reflection::serialize(out, md.tp, std::move(md.partitions));
    }

    model::topic_metadata from(iobuf_parser& in) {
        auto md = model::topic_metadata(
          reflection::adl<model::topic>{}.from(in));
        md.partitions
          = reflection::adl<std::vector<model::partition_metadata>>{}.from(in);
        return md;
    };
};
} // namespace reflection
