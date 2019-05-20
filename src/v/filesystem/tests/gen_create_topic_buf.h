#pragma once

#include "wal_generated.h"

#include <smf/fbs_typed_buf.h>
#include <smf/native_type_utils.h>

#include <unordered_map>

inline smf::fbs_typed_buf<wal_topic_create_request> gen_create_topic_buf(
  seastar::sstring ns,
  seastar::sstring topic,
  int32_t partitions,
  wal_topic_type type,
  std::unordered_map<seastar::sstring, seastar::sstring> props) {
    wal_topic_create_requestT x;
    x.topic = topic;
    x.ns = ns;
    x.partitions = partitions;
    x.type = type;
    for (auto& kv : props) {
        auto p = std::make_unique<wal_topic_propertyT>();
        p->key = kv.first;
        p->value = kv.second;
        x.props.push_back(std::move(p));
    }
    auto body = smf::native_table_as_buffer<wal_topic_create_request>(x);
    return smf::fbs_typed_buf<wal_topic_create_request>(std::move(body));
}
