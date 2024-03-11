/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/topic_manifest.h"

#include "bytes/iostream.h"
#include "bytes/streambuf.h"
#include "cloud_storage/logger.h"
#include "cloud_storage/types.h"
#include "cluster/types.h"
#include "hashing/xx.h"
#include "json/document.h"
#include "json/encodings.h"
#include "json/istreamwrapper.h"
#include "json/ostreamwrapper.h"
#include "json/reader.h"
#include "json/types.h"
#include "json/writer.h"
#include "model/compression.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/timestamp.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/sstring.hh>

#include <boost/lexical_cast.hpp>
#include <fmt/ostream.h>
#include <rapidjson/error/en.h>

#include <chrono>
#include <optional>
#include <stdexcept>
#include <string_view>

namespace {

/// Section 1: for each field in topic_properties (and other subobjects) define
/// an enum value here and its string representation

// Enum used as a json-key to refer to fields in cluster::topic_properties +
// some extra keys for cluster::topic_properites::redpanda_serde_version and
// topic_manifest-related fields
enum class fields {
    topic_manifest_version,
    ns, // namespace,
    topic,
    partition_count,
    replication_factor,
    revision_id,

    // this fields is introduced with
    // topic_manifest::cluster_topic_configuration_version special key to save
    // cluster::topic_properties::serde_version
    cluster_topic_properties_serde_version,

    // next the keys for each field in topic_properties
    compression,
    cleanup_policy_bitflags,
    compaction_strategy,
    timestamp_type,
    segment_size,
    retention_bytes,
    retention_duration,

    // fields after this are introduced with
    // topic_manifest::cluster_topic_configuration_version
    recovery,
    shadow_indexing,
    read_replica,
    read_replica_bucket,
    remote_topic_properties,
    batch_max_bytes,
    retention_local_target_bytes,
    retention_local_target_ms,
    remote_delete,
    segment_ms,
    record_key_schema_id_validation,
    record_key_schema_id_validation_compat,
    record_key_subject_name_strategy,
    record_key_subject_name_strategy_compat,
    record_value_schema_id_validation,
    record_value_schema_id_validation_compat,
    record_value_subject_name_strategy,
    record_value_subject_name_strategy_compat,
    initial_retention_local_target_bytes,
    initial_retention_local_target_ms,
    mpx_virtual_cluster_id,
    write_caching,
    flush_ms,
    flush_bytes,
    // fields in v24.1 ends here

    // end marker
    fields_end,

};

constexpr auto to_string_view(fields f) -> std::string_view {
    using enum fields;
    switch (f) {
    case topic_manifest_version:
        return "version";
    case ns:
        return "namespace";
    case topic:
        return "topic";
    case partition_count:
        return "partition_count";
    case replication_factor:
        return "replication_factor";
    case revision_id:
        return "revision_id";
    case compression:
        return "compression";
    case cleanup_policy_bitflags:
        return "cleanup_policy_bitflags";
    case compaction_strategy:
        return "compaction_strategy";
    case timestamp_type:
        return "timestamp_type";
    case segment_size:
        return "segment_size";
    case retention_bytes:
        return "retention_bytes";
    case retention_duration:
        return "retention_duration";
    case cluster_topic_properties_serde_version:
        return "$topic_properties_serde_version"; // special key starts with a $
    case recovery:
        return "recovery";
    case shadow_indexing:
        return "shadow_indexing";
    case read_replica:
        return "read_replica";
    case read_replica_bucket:
        return "read_replica_bucket";
    case remote_topic_properties:
        return "remote_topic_properties";
    case batch_max_bytes:
        return "batch_max_bytes";
    case retention_local_target_bytes:
        return "retention_local_target_bytes";
    case retention_local_target_ms:
        return "retention_local_target_ms";
    case remote_delete:
        return "remote_delete";
    case segment_ms:
        return "segment_ms";
    case record_key_schema_id_validation:
        return "record_key_schema_id_validation";
    case record_key_schema_id_validation_compat:
        return "record_key_schema_id_validation_compat";
    case record_key_subject_name_strategy:
        return "record_key_subject_name_strategy";
    case record_key_subject_name_strategy_compat:
        return "record_key_subject_name_strategy_compat";
    case record_value_schema_id_validation:
        return "record_value_schema_id_validation";
    case record_value_schema_id_validation_compat:
        return "record_value_schema_id_validation_compat";
    case record_value_subject_name_strategy:
        return "record_value_subject_name_strategy";
    case record_value_subject_name_strategy_compat:
        return "record_value_subject_name_strategy_compat";
    case initial_retention_local_target_bytes:
        return "initial_retention_local_target_bytes";
    case initial_retention_local_target_ms:
        return "initial_retention_local_target_ms";
    case mpx_virtual_cluster_id:
        return "virtual_cluster_id";
    case write_caching:
        return "write_caching";
    case flush_ms:
        return "flush_ms";
    case flush_bytes:
        return "flush_bytes";

    case fields_end:
        break;
    }
    throw std::runtime_error{fmt_with_ctx(
      fmt::format,
      "{} is outside the valid range for {}",
      int(f),
      serde::type_str<fields>())};
};

// enum class used as a json-key to refer to fields in
// cluster::remote_topic_properties::redpanda_serde_version
enum class rtp_fields : std::uint8_t {
    cluster_remote_topic_properties_serde_version,
    remote_revision,
    remote_partition_count,
    fields_end,
};

constexpr auto to_string_view(rtp_fields rf) -> std::string_view {
    using enum rtp_fields;
    switch (rf) {
    case cluster_remote_topic_properties_serde_version:
        return "$remote_topic_properties_version";
    case remote_revision:
        return "remote_revision";
    case remote_partition_count:
        return "remote_partition_count";

    case fields_end:
        break;
    }
    throw std::runtime_error{fmt_with_ctx(
      fmt::format,
      "{} is outside the valid range for {}",
      int(rf),
      serde::type_str<rtp_fields>())};
};

/// Section 2: helper functions and static checks for
/// serialization/deserialization

// special value used to check that fields is complete
constexpr auto first_topic_properties_field = fields::compression;
static_assert(
  [] {
      // Precondition: enum class fields is not sparse (does not contain gaps).
      // this is needed to ensure that we can count how many enum value there
      // are, to compare them to the number of fields of
      // cluster::topic_properties.
      // The programmer has the responsibility of actually using the enums
      // to serialize/deserialize a field.
      auto const fields_count = int(fields::fields_end)
                                - int(first_topic_properties_field);
      return std::tuple_size_v<
               decltype(cluster::topic_properties{}.serde_fields())>
             == fields_count;
  }(),
  "enum class fields is missing some values. it needs to cover all the fields "
  "in cluster::topic_properties. if there is a new field in "
  "cluster::topic_property, adds a new value before fields::fields_end, a "
  "string representation in to_string_view(fields), and populate "
  "do_serialize(DocumentView) and do_update(Document)");

// helper from json-key enums to json::Value
template<typename Enum>
requires requires(Enum e) {
    { to_string_view(e) } -> std::convertible_to<std::string_view>;
}
auto to_json_value(Enum field) -> json::Value {
    auto fname = to_string_view(field);
    return json::Value{fname.data(), unsigned(fname.size())};
}

template<typename Enum>
requires requires(Enum e) {
    { to_string_view(e) } -> std::convertible_to<std::string_view>;
}
auto operator<<(std::ostream& os, Enum field) -> std::ostream& {
    return os << to_string_view(field);
}

// contexts to pass the field_name and the root document, useful for error
// reporting and building sub-objects
struct ConstCtx {
    std::string_view field_name;
    json::Document const* doc;
};
struct Ctx {
    std::string_view field_name;
    json::Document* doc;
};

// concept used to define from_json/to_json functions for all the encoded types
// a json_mapping is a struct with from_json/to_json functions
template<typename Proj, typename T>
concept json_mapping = std::is_default_constructible_v<Proj>
                       && requires(
                         Proj const proj,
                         T const val,
                         json::Value const& json_val,
                         ConstCtx cctx,
                         Ctx ctx) {
                              proj.from_json(cctx, json_val);
                              // std::convertible_to<T>; is not enforced,
                              // to allow special handling of retention
                              // properties
                              {
                                  proj.to_json(ctx, val)
                              } -> std::convertible_to<json::Value>;
                          };

// prototype of a json_projection. specializations of this template should pass
// the json_mapping concept
template<typename T>
struct json_projection {
    // for exposition only;
    json_projection() = delete;

    T from_json(ConstCtx, json::Value const&) const;
    json::Value tp_json(Ctx, T const&) const;
};

// CRTP-Base class that implements read/write methods for T, optional<T> and
// tristate<T>. The keys are enums. Each function accepts a projection, which is
// a struct with from_json/to_json functions the projection is used to convert
// the json::Value to the type T, and vice-versa. The default value is
// json_projection<T>, but another value can be used to handle special cases,
// like retention and legacy values.
template<typename Derived, typename Enum>
struct crtp_json_read_write {
    // typename Derived provides get_ctx(Enum) -> Ctx and AddMember(Enum,
    // json::Value) for write functions, and get_ctx(Enum) const -> ConstCtx,
    // bool HasMember(Enum) and json::Value operator[](Enum) for read functions.

    // for exposition only;
    auto underlying() & -> Derived& { return *static_cast<Derived*>(this); }
    auto underlying() const& -> Derived const& {
        return *static_cast<Derived const*>(this);
    }

    // write a T as key: T
    template<typename T, json_mapping<T> Proj = json_projection<T>>
    void write(Enum f, T const& val, Proj proj = {}) {
        underlying().AddMember(f, proj.to_json(underlying().get_ctx(f), val));
    }

    // write an optional<T> as key: null or key: T
    template<typename T, json_mapping<T> Proj = json_projection<T>>
    void write(Enum f, std::optional<T> const& val, Proj proj = {}) {
        if (val.has_value()) {
            underlying().AddMember(
              f, proj.to_json(underlying().get_ctx(f), val.value()));
        } else {
            underlying().AddMember(f, json::Value{});
        }
    }

    // write a tristate<T> as [no-key], key: null or key: T
    template<typename T, json_mapping<T> Proj = json_projection<T>>
    void write(Enum f, tristate<T> const& val, Proj proj = {}) {
        if (val.is_disabled()) {
            return;
        }

        if (val.has_optional_value()) {
            return underlying().AddMember(
              f, proj.to_json(underlying().get_ctx(f), val.value()));
        } else {
            return underlying().AddMember(f, json::Value{});
        }
    }

    // read a T from key: T. expect key to exists
    template<typename T, json_mapping<T> Proj = json_projection<T>>
    void read(Enum f, T& val, Proj proj = {}) const {
        if (!underlying().HasMember(f)) {
            throw std::runtime_error(fmt_with_ctx(
              fmt::format, "Missing {} value in parsed topic manifest", f));
        }
        val = proj.from_json(underlying().get_ctx(f), underlying()[f]);
    }

    // read an optional<T> from key: null or key: T. expect key to exists
    template<typename T, json_mapping<T> Proj = json_projection<T>>
    void read(Enum f, std::optional<T>& val, Proj proj = {}) const {
        if (!underlying().HasMember(f)) {
            throw std::runtime_error(fmt_with_ctx(
              fmt::format, "Missing {} value in parsed topic manifest", f));
        }
        auto const& jv = underlying()[f];
        if (jv.IsNull()) {
            val = std::nullopt;
            return;
        }
        val = proj.from_json(underlying().get_ctx(f), jv);
    }

    // read a tristate<T> from key: null or key: T. [no-key] is interpreted as
    // disabled_tristate
    template<typename T, json_mapping<T> Proj = json_projection<T>>
    void read(Enum f, tristate<T>& val, Proj proj = {}) const {
        if (!underlying().HasMember(f)) {
            val = tristate<T>{disable_tristate};
            return;
        }

        auto const& jv = underlying()[f];

        if (jv.IsNull()) {
            val = tristate<T>{std::nullopt};
            return;
        }

        val = tristate<T>{proj.from_json(underlying().get_ctx(f), jv)};
    }
};

// read/write wrapper around json::Document, to allow using enums as keys
class DocumentView final : public crtp_json_read_write<DocumentView, fields> {
    json::Document* doc_;

public:
    DocumentView(json::Document& ref)
      : doc_(&ref) {}
    auto operator[](fields f) const -> json::Value const& {
        return (*doc_)[to_json_value(f)];
    }

    bool HasMember(fields f) const { return doc_->HasMember(to_json_value(f)); }
    void AddMember(fields f, json::Value jv) {
        doc_->AddMember(to_json_value(f), std::move(jv), doc_->GetAllocator());
    }

    auto get_ctx(fields f) const -> ConstCtx {
        return ConstCtx{to_string_view(f), doc_};
    }
    auto get_ctx(fields f) -> Ctx { return Ctx{to_string_view(f), doc_}; }
};

// read wrapper around json::Value, to allow using enums as keys. used by
// subobjects of a root Document
template<typename E>
requires std::is_enum_v<E>
struct object_view final : public crtp_json_read_write<object_view<E>, E> {
    json::Value::ConstObject obj_;
    json::Document const* doc_;

    object_view(json::Value::ConstObject ref, json::Document const* doc)
      : obj_{ref}
      , doc_{doc} {}

    bool HasMember(E f) const { return obj_.HasMember(to_json_value(f)); }

    auto operator[](E f) const -> json::Value const& {
        return obj_[to_json_value(f)];
    }
    auto get_ctx(E f) const -> ConstCtx {
        return ConstCtx{to_string_view(f), doc_};
    }
};

// write wrapper around json::Value, to allow using enums as keys. used by
// subobjects of a root Document
template<typename E>
requires std::is_enum_v<E>
struct object_writer final : public crtp_json_read_write<object_writer<E>, E> {
    json::Value* obj_;
    json::Document* doc_;

    object_writer(json::Value& ref, json::Document* doc)
      : obj_{&ref}
      , doc_{doc} {}
    json::Document::AllocatorType allocator{};
    void AddMember(rtp_fields f, json::Value jv) {
        obj_->AddMember(to_json_value(f), std::move(jv), allocator);
    }
    auto get_ctx(E f) const -> Ctx { return Ctx{to_string_view(f), doc_}; }
};

/// Section 3: json_mappings for values that we care about. for a new field that
/// is an object or a T not yet mapped, add a specialization here

// enums gets converted to their string representation
template<typename E>
requires std::is_enum_v<E>
struct json_projection<E> {
    auto from_json(ConstCtx, json::Value const& jv) const -> E {
        return boost::lexical_cast<E>(jv.GetString(), jv.GetStringLength());
    }
    auto to_json(Ctx ctx, E in) const -> json::Value {
        auto str_repr = boost::lexical_cast<std::string>(in);
        return json::Value{
          str_repr.data(), unsigned(str_repr.size()), ctx.doc->GetAllocator()};
    }
};

// strings go in as strings
template<>
struct json_projection<ss::sstring> {
    auto from_json(ConstCtx, json::Value const& jv) const -> ss::sstring {
        return ss::sstring{jv.GetString(), jv.GetStringLength()};
    }
    auto to_json(Ctx ctx, ss::sstring const& in) const -> json::Value {
        return json::Value{
          in.data(), unsigned(in.size()), ctx.doc->GetAllocator()};
    }
};

// named_types gets unwrapped and serialized as their underlying type
template<typename T, typename tag>
struct json_projection<named_type<T, tag>> {
    auto from_json(ConstCtx ctx, json::Value const& jv) const {
        return named_type<T, tag>{json_projection<T>{}.from_json(ctx, jv)};
    }
    auto to_json(Ctx ctx, named_type<T, tag> const& in) const -> json::Value {
        return json_projection<T>{}.to_json(ctx, in());
    }
};

// special handling for model::topic, since it's not just a named_type
template<>
struct json_projection<model::topic> {
    auto from_json(ConstCtx, json::Value const& jv) const {
        return model::topic{ss::sstring{jv.GetString(), jv.GetStringLength()}};
    }
    auto to_json(Ctx ctx, model::topic const& in) const -> json::Value {
        return json::Value{
          in().data(), unsigned(in().size()), ctx.doc->GetAllocator()};
    }
};

// integral types gets serialized as integral. during deserialization it's
// performed a check that they can fit in the destination type
template<typename I>
requires std::integral<I> || std::same_as<I, std::uint8_t>
struct json_projection<I> {
    auto from_json(ConstCtx ctx, json::Value const& jv) const -> I {
        using wide_int_t = std::
          conditional_t<std::is_signed_v<I>, std::int64_t, std::uint64_t>;

        auto tmp = jv.Get<wide_int_t>();
        if constexpr (!std::same_as<wide_int_t, I>) {
            // check that for tmp can fit in I
            if (!std::in_range<I>(tmp)) {
                throw std::runtime_error(fmt_with_ctx(
                  fmt::format,
                  "{} = {} is outside the admissible range of {},{}",
                  ctx.field_name,
                  tmp,
                  std::numeric_limits<I>::min(),
                  std::numeric_limits<I>::max()));
            }
        }
        return tmp;
    }

    auto to_json(Ctx, I in) const -> json::Value { return json::Value{in}; }
};

// bool is encoded directly
template<>
struct json_projection<bool> {
    auto from_json(ConstCtx, json::Value const& jv) const -> bool {
        return jv.GetBool();
    }
    auto to_json(Ctx, bool in) const -> json::Value { return json::Value{in}; }
};

// milliseconds are unwrapped as int64_t (it's a signed type)
template<>
struct json_projection<std::chrono::milliseconds> {
    auto from_json(ConstCtx, json::Value const& jv) const
      -> std::chrono::milliseconds {
        return std::chrono::milliseconds{jv.GetInt64()};
    }
    auto to_json(Ctx, std::chrono::milliseconds in) const -> json::Value {
        return json::Value{int64_t(in.count())};
    }
};

// retention properties (tristate<size_t> or tristate<millis>) gets a special
// treatment during deserialization: negative values are interpreted as
// disabled_tristate. this it to mimick with "retention_durantion": -1 what is
// done to set infinite retention
template<typename T>
struct json_projection_retention : public json_projection<T> {
    // only decoding needs special handling of negative json values, the rest
    // can be deferred to the proper json_projection
    auto from_json(ConstCtx ctx, json::Value const& jv) const -> tristate<T> {
        if (jv.IsInt64() && jv.GetInt64() < 0) {
            return tristate<T>{disable_tristate};
        }
        return tristate<T>{json_projection<T>{}.from_json(ctx, jv)};
    }
};

// due to a bug segment_size could be encoded as a negative value. this is a
// special handler to decode negative segment_size as empty optionals
struct json_projection_segment_size_legacy : public json_projection<size_t> {
    auto from_json(ConstCtx, json::Value const& jv) const
      -> std::optional<size_t> {
        if (jv.IsInt64() && jv.GetInt64() < 0) {
            return std::nullopt;
        }
        return jv.GetUint64();
    }
};

// a xid has a string representation in json
template<>
struct json_projection<xid> {
    auto from_json(ConstCtx, json::Value const& jv) const -> xid {
        auto tmp = std::string_view{jv.GetString(), jv.GetStringLength()};
        return boost::lexical_cast<xid>(tmp);
    }
    auto to_json(Ctx ctx, xid const& in) const -> json::Value {
        auto tmp = fmt::format("{}", in);
        return json::Value{
          tmp.data(), unsigned(tmp.size()), ctx.doc->GetAllocator()};
    }
};

// projection for cluster::remote_topic_properties, uses the values from enum
// rtp_fields as keys
template<>
struct json_projection<cluster::remote_topic_properties> {
    auto from_json(ConstCtx ctx, json::Value const& jv) const
      -> cluster::remote_topic_properties {
        using enum rtp_fields;
        auto rtp_doc = object_view<rtp_fields>{jv.GetObject(), ctx.doc};

        auto res = cluster::remote_topic_properties{};

        // use encoded serde version to limit the backward/forward compatibility
        serde::version_t encoded_rtp_version;
        rtp_doc.read(
          cluster_remote_topic_properties_serde_version, encoded_rtp_version);
        if (unlikely(
              encoded_rtp_version < cluster::remote_topic_properties::
                redpanda_serde_compat_version)) {
            throw std::runtime_error(fmt_with_ctx(
              fmt::format,
              "topic manifest with cluster::remote_topic_properties::version "
              "{} cannot be decoded because it's older than compat_version {}",
              encoded_rtp_version,
              cluster::remote_topic_properties::redpanda_serde_compat_version));
        }

        if (unlikely(
              encoded_rtp_version
              > cluster::remote_topic_properties::redpanda_serde_version)) {
            throw std::runtime_error(fmt_with_ctx(
              fmt::format,
              "topic manifest with cluster::remote_topic_properties::version "
              "{} cannot be decoded because it's newer than current version {}",
              encoded_rtp_version,
              cluster::remote_topic_properties::redpanda_serde_version));
        }

        rtp_doc.read(remote_partition_count, res.remote_partition_count);
        rtp_doc.read(remote_revision, res.remote_revision);
        return res;
    }

    auto to_json(Ctx ctx, cluster::remote_topic_properties const& rtp) const
      -> json::Value {
        using enum rtp_fields;
        auto res = json::Value{};
        res.SetObject();
        auto rtp_doc = object_writer<rtp_fields>{res, ctx.doc};
        rtp_doc.write(
          cluster_remote_topic_properties_serde_version,
          cluster::remote_topic_properties::redpanda_serde_version);
        rtp_doc.write(remote_partition_count, rtp.remote_partition_count);
        rtp_doc.write(remote_revision, rtp.remote_revision);
        return res;
    }
};

/// Section 4: root serialize/deserialize functions

static void do_serialize(
  model::initial_revision_id rev_id,
  cluster::topic_configuration const& tc,
  DocumentView& doc) {
    using enum fields;

    doc.write(
      topic_manifest_version, cloud_storage::topic_manifest::current_version);
    doc.write(revision_id, rev_id);

    doc.write(ns, tc.tp_ns.ns);
    doc.write(topic, tc.tp_ns.tp);
    doc.write(partition_count, tc.partition_count);
    doc.write(replication_factor, tc.replication_factor);

    // first write the current serde version of topic_properties (new in
    // topic_manifest::cluster_topic_configuration_versions)
    doc.write(
      cluster_topic_properties_serde_version,
      cluster::topic_properties::redpanda_serde_version);

    // write the fields in topic_properties
    auto& properties = tc.properties;
    doc.write(compression, properties.compression);
    doc.write(cleanup_policy_bitflags, properties.cleanup_policy_bitflags);
    doc.write(compaction_strategy, properties.compaction_strategy);
    doc.write(timestamp_type, properties.timestamp_type);
    doc.write(segment_size, properties.segment_size);
    doc.write(
      retention_bytes,
      properties.retention_bytes,
      json_projection_retention<size_t>{});
    doc.write(
      retention_duration,
      properties.retention_duration,
      json_projection_retention<std::chrono::milliseconds>{});

    // new props introduced after topic_manifest_version:
    // topic_manifest::cluster_topic_configuration_version first
    doc.write(recovery, properties.recovery);
    doc.write(shadow_indexing, properties.shadow_indexing);
    doc.write(read_replica, properties.read_replica);
    doc.write(read_replica_bucket, properties.read_replica_bucket);
    doc.write(remote_topic_properties, properties.remote_topic_properties);
    doc.write(batch_max_bytes, properties.batch_max_bytes);
    doc.write(
      retention_local_target_bytes, properties.retention_local_target_bytes);
    doc.write(retention_local_target_ms, properties.retention_local_target_ms);

    doc.write(remote_delete, properties.remote_delete);
    doc.write(segment_ms, properties.segment_ms);
    doc.write(
      record_key_schema_id_validation,
      properties.record_key_schema_id_validation);
    doc.write(
      record_key_schema_id_validation_compat,
      properties.record_key_schema_id_validation_compat);
    doc.write(
      record_key_subject_name_strategy,
      properties.record_key_subject_name_strategy);
    doc.write(
      record_key_subject_name_strategy_compat,
      properties.record_key_subject_name_strategy_compat);
    doc.write(
      record_value_schema_id_validation,
      properties.record_value_schema_id_validation);
    doc.write(
      record_value_schema_id_validation_compat,
      properties.record_value_schema_id_validation_compat);
    doc.write(
      record_value_subject_name_strategy,
      properties.record_value_subject_name_strategy);
    doc.write(
      record_value_subject_name_strategy_compat,
      properties.record_value_subject_name_strategy_compat);
    doc.write(
      initial_retention_local_target_bytes,
      properties.initial_retention_local_target_bytes);
    doc.write(
      initial_retention_local_target_ms,
      properties.initial_retention_local_target_ms);
    doc.write(mpx_virtual_cluster_id, properties.mpx_virtual_cluster_id);
    doc.write(write_caching, properties.write_caching);
    doc.write(flush_ms, properties.flush_ms);
    doc.write(flush_bytes, properties.flush_bytes);

    // add new fields topic_properties here
}

// This functions handles disabled tristate in a backward compatible manner.
// A disabled tristate is not encoded in the json. if a tristate field from
// topic_properties is not present in a topic_manifest.json file, there needs to
// be a way to decide if the field should be disabled or should it be left in
// the default state because it was added after the file was written. In order
// to do so, this function relies on the cluster_topic_properties_serde_version
// json field to stop decoding at the appropriate time.
static auto do_update(DocumentView doc) -> std::
  tuple<int32_t, model::initial_revision_id, cluster::topic_configuration> {
    using enum fields;

    int32_t encoded_manifest_version;
    doc.read(topic_manifest_version, encoded_manifest_version);

    if (
      encoded_manifest_version < cloud_storage::topic_manifest::first_version
      || encoded_manifest_version
           > cloud_storage::topic_manifest::current_version) {
        throw std::runtime_error(fmt_with_ctx(
          fmt::format,
          "topic manifest version {} is not supported",
          encoded_manifest_version,
          cloud_storage::topic_manifest::current_version));
    }

    model::initial_revision_id local_revision_id;
    doc.read(revision_id, local_revision_id);

    auto tmp = cluster::topic_configuration{};

    doc.read(ns, tmp.tp_ns.ns);
    doc.read(topic, tmp.tp_ns.tp);
    doc.read(partition_count, tmp.partition_count);
    doc.read(replication_factor, tmp.replication_factor);

    auto& properties = tmp.properties;
    doc.read(compression, properties.compression);
    doc.read(cleanup_policy_bitflags, properties.cleanup_policy_bitflags);
    doc.read(compaction_strategy, properties.compaction_strategy);
    doc.read(timestamp_type, properties.timestamp_type);
    doc.read(
      segment_size,
      properties.segment_size,
      json_projection_segment_size_legacy{});
    // special handling of retention options, to allow a negative value to mean
    // disabled
    doc.read(
      retention_bytes,
      properties.retention_bytes,
      json_projection_retention<size_t>{});
    doc.read(
      retention_duration,
      properties.retention_duration,
      json_projection_retention<std::chrono::milliseconds>{});

    if (
      encoded_manifest_version
      < cloud_storage::topic_manifest::cluster_topic_configuration_version) {
        // not possible to read properties after retention_duration from older
        // manifests, stop here
        return {encoded_manifest_version, local_revision_id, std::move(tmp)};
    }

    // after topic_manifest::cluster_topic_configuration_version, topic_manifest
    // can handle reading tristates encoded from previous topic_properties
    // version. the trick is to read the encoded version and skip reading
    // properties that where not in place before it. Read
    // cluster_topic_properties_serde_version and use it to stop decoding at the
    // appropriate moment
    serde::version_t encoded_topic_property_version;
    doc.read(
      cluster_topic_properties_serde_version, encoded_topic_property_version);

    // check for encoded version that is not supported anymore
    if (unlikely(
          encoded_topic_property_version
          < cluster::topic_properties::redpanda_serde_compat_version)) {
        throw std::runtime_error(fmt_with_ctx(
          fmt::format,
          "topic manifest with cluster::topic_properties::version {} cannot be "
          "decoded because it's older than compat_version {}",
          encoded_topic_property_version,
          cluster::topic_properties::redpanda_serde_compat_version));
    }

    // cannot easily support forward compatibility, so bail out
    if (unlikely(
          encoded_topic_property_version
          > cluster::topic_properties::redpanda_serde_version)) {
        throw std::runtime_error(fmt_with_ctx(
          fmt::format,
          "topic manifest with cluster::topic_properties::version {} cannot be "
          "decoded because it's newer than current version {}",
          encoded_topic_property_version,
          cluster::topic_properties::redpanda_serde_version));
    }

    doc.read(recovery, properties.recovery);
    doc.read(shadow_indexing, properties.shadow_indexing);
    doc.read(read_replica, properties.read_replica);
    doc.read(read_replica_bucket, properties.read_replica_bucket);
    doc.read(remote_topic_properties, properties.remote_topic_properties);
    doc.read(batch_max_bytes, properties.batch_max_bytes);
    doc.read(
      retention_local_target_bytes, properties.retention_local_target_bytes);
    doc.read(
      retention_local_target_ms,
      properties.retention_local_target_ms,
      json_projection_retention<std::chrono::milliseconds>{});
    doc.read(remote_delete, properties.remote_delete);
    doc.read(
      segment_ms,
      properties.segment_ms,
      json_projection_retention<std::chrono::milliseconds>{});
    doc.read(
      record_key_schema_id_validation,
      properties.record_key_schema_id_validation);
    doc.read(
      record_key_schema_id_validation_compat,
      properties.record_key_schema_id_validation_compat);
    doc.read(
      record_key_subject_name_strategy,
      properties.record_key_subject_name_strategy);
    doc.read(
      record_key_subject_name_strategy_compat,
      properties.record_key_subject_name_strategy_compat);
    doc.read(
      record_value_schema_id_validation,
      properties.record_value_schema_id_validation);
    doc.read(
      record_value_schema_id_validation_compat,
      properties.record_value_schema_id_validation_compat);
    doc.read(
      record_value_subject_name_strategy,
      properties.record_value_subject_name_strategy);
    doc.read(
      record_value_subject_name_strategy_compat,
      properties.record_value_subject_name_strategy_compat);
    doc.read(
      initial_retention_local_target_bytes,
      properties.initial_retention_local_target_bytes,
      json_projection_retention<size_t>{});
    doc.read(
      initial_retention_local_target_ms,
      properties.initial_retention_local_target_ms,
      json_projection_retention<std::chrono::milliseconds>{});
    doc.read(mpx_virtual_cluster_id, properties.mpx_virtual_cluster_id);
    doc.read(write_caching, properties.write_caching);
    doc.read(flush_ms, properties.flush_ms);
    doc.read(flush_bytes, properties.flush_bytes);

    if (
      encoded_manifest_version <= cluster::topic_properties_version_for_v24_1) {
        // properties after this point where not serialized in this document, so
        // we can't deserialize them
        return {encoded_manifest_version, local_revision_id, std::move(tmp)};
    }
    // nothing here for now, in the future:
    // doc.read(a_new_property_in_v24_2, properties.a_new_property_in_v24_2);
    throw std::runtime_error(fmt_with_ctx(
      fmt::format,
      "missing decoding fields, manifest topic_properties version: {} current "
      "topic_properties serde_version: {}",
      encoded_topic_property_version,
      cluster::topic_properties::redpanda_serde_version));
}

} // namespace
namespace cloud_storage {

topic_manifest::topic_manifest(
  const cluster::topic_configuration& cfg, model::initial_revision_id rev)
  : _topic_config(cfg)
  , _rev(rev) {}

topic_manifest::topic_manifest()
  : _topic_config(std::nullopt) {}

ss::future<> topic_manifest::update(ss::input_stream<char> is) {
    iobuf result;
    auto os = make_iobuf_ref_output_stream(result);
    co_await ss::copy(is, os);
    iobuf_istreambuf ibuf(result);
    std::istream stream(&ibuf);

    json::IStreamWrapper wrapper(stream);
    auto doc = json::Document{};
    doc.ParseStream(wrapper);
    if (!doc.HasParseError()) {
        auto [manifest_version, rev_id, topic_cfg] = ::do_update(doc);
        _manifest_version = manifest_version;
        _rev = rev_id;
        _topic_config = std::move(topic_cfg);
    } else {
        throw std::runtime_error(fmt_with_ctx(
          fmt::format,
          "Failed to parse topic manifest: {} @ {}",
          rapidjson::GetParseError_En(doc.GetParseError()),
          doc.GetErrorOffset()));
    }

    co_return;
}

ss::future<serialized_data_stream> topic_manifest::serialize() const {
    iobuf serialized;
    iobuf_ostreambuf obuf(serialized);
    std::ostream os(&obuf);
    serialize(os);
    size_t size_bytes = serialized.size_bytes();
    co_return serialized_data_stream{
      .stream = make_iobuf_input_stream(std::move(serialized)),
      .size_bytes = size_bytes};
}

void topic_manifest::serialize(std::ostream& out) const {
    if (!_topic_config.has_value()) {
        ss::throw_with_backtrace<std::runtime_error>(
          "_topic_config is not initialized");
    }
    // serialization happens here
    auto doc = json::Document{};
    doc.SetObject();
    auto dv = DocumentView{doc};
    do_serialize(_rev, _topic_config.value(), dv);

    auto wrapper = json::OStreamWrapper(out);
    auto writer = json::Writer<json::OStreamWrapper>{wrapper};
    // dump the json DOM into writer, that will write into `serialized`
    doc.Accept(writer);
    if (!out.good()) {
        throw std::runtime_error(fmt_with_ctx(
          fmt::format,
          "could not serialize topic manifest {}",
          get_manifest_path()));
    }
}

remote_manifest_path
topic_manifest::get_topic_manifest_path(model::ns ns, model::topic topic) {
    // The path is <prefix>/meta/<ns>/<topic>/topic_manifest.json
    constexpr uint32_t bitmask = 0xF0000000;
    auto path = fmt::format("{}/{}", ns(), topic());
    uint32_t hash = bitmask & xxhash_32(path.data(), path.size());
    return remote_manifest_path(
      fmt::format("{:08x}/meta/{}/topic_manifest.json", hash, path));
}

remote_manifest_path topic_manifest::get_manifest_path() const {
    // The path is <prefix>/meta/<ns>/<topic>/topic_manifest.json
    vassert(_topic_config, "Topic config is not set");
    return get_topic_manifest_path(
      _topic_config->tp_ns.ns, _topic_config->tp_ns.tp);
}
} // namespace cloud_storage
