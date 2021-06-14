/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include "coproc/api/wasm_list.h"

#include "model/record_batch_types.h"
#include "rapidjson/document.h"
#include "reflection/adl.h"
#include "storage/record_batch_builder.h"

#include <seastar/core/coroutine.hh>

#include <absl/container/flat_hash_map.h>

namespace coproc::wasm {

struct copro_deploy_info {
    deploy_attributes attrs;
    std::set<model::topic> topics;
};

using layout = absl::flat_hash_map<ss::sstring, copro_deploy_info>;

static model::record_batch_reader
status(model::node_id nid, layout&& lt, bool is_up) {
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    writer.StartObject();
    writer.Key("status");
    writer.String(is_up ? "up" : "down");
    writer.Key("node_id");
    writer.Int(nid());
    writer.Key("coprocessors");
    writer.StartObject();
    for (auto& p : lt) {
        writer.Key(p.first.c_str());
        writer.StartObject();
        writer.Key("input_topics");
        writer.StartArray();
        for (auto& topic : p.second.topics) {
            writer.String(topic());
        }
        writer.EndArray();
        writer.Key("description");
        writer.String(p.second.attrs.description);
        writer.EndObject();
    }
    writer.EndObject();
    writer.EndObject();
    ss::sstring str(buffer.GetString());
    storage::record_batch_builder b(
      model::record_batch_type::raft_data, model::offset{0});
    iobuf iob;
    iob.append(str.data(), str.size());
    b.add_raw_kv(reflection::to_iobuf(nid()), std::move(iob));
    return model::make_memory_record_batch_reader(std::move(b).build());
}

ss::future<model::record_batch_reader> current_status(
  model::node_id nid,
  ss::sharded<pacemaker>& pacemaker,
  const absl::btree_map<script_id, deploy_attributes>& scripts) {
    if (!pacemaker.local().is_connected()) {
        co_return status(nid, {}, false);
    }
    layout lt;
    for (const auto& item : scripts) {
        auto current_topics = co_await pacemaker.map_reduce0(
          [id = item.first](coproc::pacemaker& p) {
              return p.registered_ntps(id);
          },
          std::set<model::topic>(),
          [](std::set<model::topic> acc, absl::flat_hash_set<model::ntp> x) {
              std::set<model::topic> topics;
              std::transform(
                x.begin(),
                x.end(),
                std::inserter(topics, topics.begin()),
                [](const model::ntp& ntp) { return ntp.tp.topic; });
              acc.merge(std::move(topics));
              return acc;
          });
        lt[item.second.name] = copro_deploy_info{
          .attrs = item.second, .topics = current_topics};
    }
    co_return status(nid, std::move(lt), true);
}
} // namespace coproc::wasm
