/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "event.h"

#include "bytes/streambuf.h"
#include "json/json.h"
#include "json/ostreamwrapper.h"

#include <type_traits>

using namespace std::chrono_literals;

namespace {

using otel_xform_name_attr
  = named_type<model::transform_name_view, struct otel_xform_name_attr_tag>;

using otel_node_id_attr
  = named_type<model::node_id, struct otel_node_id_attr_tag>;

struct otel_log_event {
    otel_log_event() = delete;
    explicit otel_log_event(
      model::transform_name_view name, const transform::logging::event& ev)
      : name(name)
      , source_id(ev.source_id)
      , ts(ev.ts.time_since_epoch() / 1ns)
      , level(ev.level)
      , message(ev.message) {}
    otel_xform_name_attr name;
    otel_node_id_attr source_id;
    uint64_t ts;
    ss::log_level level;
    std::string_view message;
};

} // namespace

namespace json {

inline void rjson_serialize(
  Writer<OStreamWrapper>& w, const model::transform_name_view& name) {
    w.String(name().data(), name().size());
}

inline void
rjson_serialize(Writer<OStreamWrapper>& w, const ::otel_xform_name_attr& name) {
    w.StartObject();
    w.Key("key");
    w.String("transform_name");
    w.Key("value");
    w.StartObject();
    w.Key("stringValue");
    rjson_serialize(w, name());
    w.EndObject();
    w.EndObject();
}

inline void
rjson_serialize(Writer<OStreamWrapper>& w, const ::otel_node_id_attr& nid) {
    w.StartObject();
    w.Key("key");
    w.String("node");
    w.Key("value");
    w.StartObject();
    w.Key("intValue");
    w.Int(static_cast<int>(nid()));
    w.EndObject();
    w.EndObject();
}

inline void
rjson_serialize(Writer<OStreamWrapper>& w, const ::otel_log_event& ev) {
    w.StartObject();
    w.Key("body");
    w.StartObject();
    w.Key("stringValue");
    w.String(ev.message.data(), ev.message.size());
    w.EndObject();
    w.Key("timeUnixNano");
    w.Uint64(ev.ts);
    w.Key("severityNumber");
    w.Uint(transform::logging::log_level_to_severity(ev.level));
    w.Key("attributes");
    w.StartArray();
    rjson_serialize(w, ev.name);
    rjson_serialize(w, ev.source_id);
    w.EndArray();
    w.EndObject();
}

} // namespace json

namespace transform::logging {

uint32_t log_level_to_severity(ss::log_level lvl) {
    static const std::unordered_map<ss::log_level, int> severity{
      {ss::log_level::trace, 1},
      {ss::log_level::debug, 5},
      {ss::log_level::info, 9},
      {ss::log_level::warn, 13},
      {ss::log_level::error, 17},
    };
    return severity.at(lvl);
}

event::event(
  model::node_id source,
  clock_type::time_point ts,
  ss::log_level level,
  ss::sstring message)
  : source_id(source)
  , ts(ts)
  , level(level)
  , message(std::move(message)) {}

void event::to_json(model::transform_name_view name, iobuf& b) const {
    iobuf_ostreambuf obuf{b};
    std::ostream os{&obuf};
    ::json::OStreamWrapper wrapper{os};
    ::json::Writer<::json::OStreamWrapper> writer{wrapper};

    using ::json::rjson_serialize;

    rjson_serialize(writer, ::otel_log_event{name, *this});
}

} // namespace transform::logging
