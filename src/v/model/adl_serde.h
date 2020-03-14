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
struct adl<unresolved_address> {
    void to(iobuf& out, const unresolved_address& address);
    unresolved_address from(iobuf_parser& in);
};

template<>
struct adl<model::broker_properties> {
    using type = std::vector<std::pair<ss::sstring, ss::sstring>>;
    void to(iobuf& out, const model::broker_properties& prop);
    model::broker_properties from(iobuf_parser& in);
};

template<>
struct adl<model::broker> {
    void to(iobuf& out, model::broker&& r);
    model::broker from(iobuf_parser& in);
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
    model::record_batch_header from(iobuf_parser& in);
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
struct adl<model::topic_metadata> {
    void to(iobuf& out, model::topic_metadata&& md);
    model::topic_metadata from(iobuf_parser& in);
};
} // namespace reflection
