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

/// JSON serialization for cluster::topic_properties.

/// For each field of topic_properties, there is a `type_mapping` /
/// `aggregate_type_mapping` in
/// `field_mappings_for_v<cluster::topic_properties>`.
///
/// A `type_mapping` has:
/// - a json name
/// - a pointer to a field in a T
/// - 2 function to map the type<->json.
/// It supports a field of type non-aggregate, optional<non-aggregate>,
/// tristate<non-aggregate>.
///
/// An aggregate_type_mapping has a json name and a
/// pointer to a field in a T. it support aggregate, optional<aggregate>,
/// tristate<aggregate>. it requires that fields_mapping_for_v<T> is defined as
/// root_type_aggregate<T, ...>.
///
/// Currently only cluster::remote_topic_properties has an
/// aggregate_type/root_aggregate_type.
///
/// Serialization
/// =============
/// On the json serialization path, starting from
/// field_mappings_for_v<cluster::topic_properties>, each field gets serialized
/// as "key": value if it's a type_mapping, or as "key": {"sub_key": value ...}
/// if it's an aggregate_type_mapping.
///
/// `optional` / `tristate` have a dedicated encoding. The process is recursive.
///
/// Deserialization
/// ===============
/// On the json deserialization path, topic_properties_handler implements
/// the corresponding SAX parser. `type_mapping` deserialization is easy, the
/// handler collects `"key": value` and dispatch them to the appropriate object
/// for deserialization.
///
/// `aggregate_type_mapping` requires the creation of a context,
/// to keep track of the sub-object currently accessed. The contexts are kept in
/// a stack, a new context is pushed with StartObject and popped with EndObject.

constexpr static std::string_view serde_version_key = "$serde_version";
constexpr static std::string_view serde_compat_version_key
  = "$serde_compat_version";
constexpr static std::string_view disabled_tristates_array_key
  = "$inactive_properties";

// these functions helps mapping cluster::topic_properties to json, via
// rapidjson, see struct field_mapping

using writer_t = json::Writer<json::OStreamWrapper>;

/// extract field type and parent type from a member pointer type
template<typename T>
struct mem_ptr_types;
template<typename K, typename F>
struct mem_ptr_types<F K::*> {
    using klass = K;
    using field = F;
};

/// return the underlying type for optional and tristate
template<typename T>
struct unwrap_container {
    using type = T;
};
template<typename T>
struct unwrap_container<std::optional<T>> {
    using type = T;
};
template<typename T>
struct unwrap_container<tristate<T>> {
    using type = T;
};

/// struct to resolve with overloading which json write function to use.
/// having a struct makes it easier to write a the concept c_to_json_fn
struct json_adapter {
    void operator()(writer_t& w, bool val) const { w.Bool(val); }
    void operator()(writer_t& w, std::string_view val) const {
        w.String(val.data(), val.size());
    }
    void operator()(writer_t& w, std::unsigned_integral auto val) const {
        w.Uint64(val);
    }
    void operator()(writer_t& w, std::signed_integral auto val) const {
        w.Int64(val);
    }
};

/// concept for a basic type serializable direcly in json
template<typename T>
concept json_basic_datatype = std::invocable<json_adapter, writer_t&, T>;

/// concept for a function that can map T to one of the basic json types
template<typename Fn, typename T>
concept c_to_json_fn = std::invocable<Fn, T>
                       && json_basic_datatype<std::invoke_result_t<Fn, T>>;

/// concepts to recognize type_mapping and aggregate_type_mapping
template<typename Fm>
concept is_mapping = requires {
    Fm::is_mapping;
    requires Fm::is_mapping;
};
template<typename Om>
concept is_aggregate_mapping = is_mapping<Om> && requires {
    Om::is_aggregate_mapping;
    requires Om::is_aggregate_mapping;
};

/// optional values are encoded in the following manner:
/// - key set to null - optional is nullopt
/// - key is not null - optional has value
template<typename T>
void tm_write(
  std::string_view name,
  std::optional<T> const& opt,
  writer_t& json_writer,
  c_to_json_fn<T> auto to_json) {
    json_writer.Key(name.data(), name.size());
    if (opt.has_value()) {
        json_adapter{}(json_writer, std::invoke(to_json, opt.value()));
    } else {
        json_writer.Null();
    }
}

/// tristate values are encoded in the following manner:
/// - key not present - tristate is disabled
/// - key set to null - tristate is enabled but not set
/// - key is not null - tristate is enabled and set
template<typename T>
void tm_write(
  std::string_view name,
  tristate<T> const& tri,
  writer_t& json_writer,
  c_to_json_fn<T> auto const& to_json) {
    if (tri.is_disabled()) {
        return;
    }
    return tm_write(name, tri.get_optional(), json_writer, to_json);
}

/// basic values are encoded as key:value
template<typename T>
void tm_write(
  std::string_view name,
  T const& val,
  writer_t& json_writer,
  c_to_json_fn<T> auto const& to_json) {
    json_writer.Key(name.data(), name.size());
    json_adapter{}(json_writer, std::invoke(to_json, val));
}

/// helpers for the json mapping "key" : null
template<typename T>
bool tm_set_null(tristate<T>& tri) {
    tri = tristate<T>(std::nullopt);
    return true;
}
template<typename T>
bool tm_set_null(std::optional<T>& opt) {
    opt = std::nullopt;
    return true;
}
bool tm_set_null(auto&) { return false; }

/// helpers to disable tristate fields
template<typename T>
bool tm_set_disabled(tristate<T>& tri) {
    tri = tristate<T>(disable_tristate);
    return true;
}
bool tm_set_disabled(auto&) { return false; }

/// helpers to set and create a reference to value inside a tristate/optional
template<typename T>
auto tm_set_default_value(T& val) -> T& {
    val = {};
    return val;
}
template<typename T>
auto tm_set_default_value(std::optional<T>& val) -> T& {
    val = T{};
    return val.value();
}
template<typename T>
auto tm_set_default_value(tristate<T>& val) -> T& {
    val = tristate<T>(T{});
    return val.value();
}

/// customization point to register a jsom mapping for a T, and constexpr
/// variable helper
template<typename T>
struct field_mappings_for_t;
template<typename T>
constexpr const auto& field_mappings_for_v = field_mappings_for_t<T>::value;

/// exception thrown from try_read that captures key and value
struct from_json_error : public std::exception {
    ss::sstring key;
    ss::sstring invalid_value;
    from_json_error(std::string_view k, std::string_view jv)
      : key{k}
      , invalid_value{jv} {}
    from_json_error(std::string_view k, std::unsigned_integral auto ui)
      : key{k}
      , invalid_value{ssx::sformat("{}", ui)} {}
    from_json_error(std::string_view k, std::signed_integral auto si)
      : key{k}
      , invalid_value{ssx::sformat("{}", si)} {}
    from_json_error(std::string_view k, bool b)
      : key{k}
      , invalid_value{ssx::sformat("{}", b)} {}
    from_json_error(std::string_view k, std::nullopt_t)
      : key{k}
      , invalid_value{"null"} {}
};

/// Struct that defines a mapping for a field inside a struct, to and from json.
/// This handles the fields of basic types, for structs see object_mapping.
/// The mapping consist of a c-string name, a member pointer to
/// the field in the struct, and a pair of functions to perform the
/// mapping of the value.
/// For optional<T>/tristate<T>, the to_json function will be called
/// only if the field has a value and it will receive the unwrapped  T
/// const & value.
/// The from_json function will be called
/// with std::string_view/uint64_t/int64_t/bool json value and their result will
/// be assigned to the field. The argument type will determine the accepted
/// parameter type. As an example, to map topic_properties::compression, write
/// this, it will perform string<->enum_value mapping:
///   type_mapping{
///     "compression",          // name
///      &tp_p::compression,    // member field pointer
///      // to_json and from_json mapper
///      boost::lexical_cast<std::string, model::compression>,
///      boost::lexical_cast<model::compression, std::string_view>},
template<typename MemPtr, typename ToJsonFn, typename FromJsonFn>
struct type_mapping {
    // tag for is_mapping concept
    constexpr static auto is_mapping = true;
    using underlying_field_t = mem_ptr_types<MemPtr>::field;
    using underlying_klass_t = mem_ptr_types<MemPtr>::klass;

    consteval type_mapping(
      const char* name, MemPtr ptr, ToJsonFn wr, FromJsonFn rd)
      : name_{name}
      , field_{ptr}
      , to_json_{wr}
      , from_json_{rd} {}

    // try to parse a key and a value. returns true if parsing was performed.
    // parsing will be attempted if from_json can be called with T and if key
    // matches the name. converts exceptions to from_json_error
    template<typename T>
    bool
    try_read(std::string_view key, T const& jv, underlying_klass_t& obj) const {
        if constexpr (std::invocable<FromJsonFn, T>) {
            if (name_ == key) {
                try {
                    std::invoke(field_, obj) = std::invoke(from_json_, jv);
                    return true;
                } catch (...) {
                    throw from_json_error{key, jv};
                }
            }
        }

        return false;
    }

    // try_read overload with std::nullopt_t, called for "key" : null.
    bool try_read(
      std::string_view key, std::nullopt_t, underlying_klass_t& obj) const {
        if (name_ == key) {
            return tm_set_null(std::invoke(field_, obj));
        }
        return false;
    }

    // try_read overload with disabled_tristate_t, called to expliclitly disable
    // tristates.
    bool try_read(
      std::string_view key, disable_tristate_t, underlying_klass_t& obj) const {
        if (name_ == key) {
            return tm_set_disabled(std::invoke(field_, obj));
        }
        return false;
    }

    // serialization function. it delegates to one of the overloads of
    // tm_serialize
    void write(underlying_klass_t const& obj, writer_t& w) const {
        tm_write(name_, std::invoke(field_, obj), w, to_json_);
    }

    // helper to write an array of disabled tristate that were not serialized
    void write_name_if_disabled_tristate(
      underlying_klass_t const& obj, writer_t& w) const {
        if constexpr (reflection::is_tristate<underlying_field_t>) {
            if (std::invoke(field_, obj).is_disabled()) {
                w.String(name_.data(), name_.size());
            }
        }
    }

    // name that will be used in json
    std::string_view name_;
    // pointer to the field in the struct
    MemPtr field_;
    // mapper to json, see details::c_to_json_fn concept
    ToJsonFn to_json_;
    // mapper from json
    FromJsonFn from_json_;
};

/// A type-erased context for each start/end object call, will point to the
/// correct handlers for a T
class object_mapping_context {
public:
    virtual ~object_mapping_context() = default;

    // start json sub-object handler (end json sub-object is implicitly
    // handled by the destructor)
    virtual auto start_subobject(std::string_view key) const
      -> std::unique_ptr<const object_mapping_context>
      = 0;

    // "key" : null
    virtual auto try_read(std::string_view key, std::nullopt_t) const -> bool
      = 0;
    // "key" : true/false
    virtual auto try_read(std::string_view key, bool) const -> bool = 0;
    // "key" : "str"
    virtual auto try_read(std::string_view key, std::string_view) const -> bool
      = 0;
    // "key" : int
    virtual auto try_read(std::string_view key, int64_t) const -> bool = 0;
    // "key" : uint
    virtual auto try_read(std::string_view key, uint64_t) const -> bool = 0;

protected:
    // rule of five + hide default constructor
    object_mapping_context() = default;
    object_mapping_context(object_mapping_context const&) = default;
    object_mapping_context& operator=(object_mapping_context const&) = default;
    object_mapping_context(object_mapping_context&&) = default;
    object_mapping_context& operator=(object_mapping_context&&) = default;
};

/// Struct that defines a mapping for a field inside a struct to and from a json
/// object.
/// It does not contains mappings for the fields inside its field,
/// rather it will retrieve the mapping from the field type of MemPtr it can
/// handle optional/tristate fields
template<typename MemPtr>
class aggregate_type_mapping {
public:
    // tag to write a simple concept
    constexpr static auto is_mapping = true;
    constexpr static auto is_aggregate_mapping = true;
    using underlying_field_t = mem_ptr_types<MemPtr>::field;
    using underlying_klass_t = mem_ptr_types<MemPtr>::klass;
    using unwrapped_type_t = unwrap_container<underlying_field_t>::type;

private:
    template<std::same_as<unwrapped_type_t> T>
    void handle_write(T const& field, writer_t& w) const {
        /// write key and delegate to mapping for T
        w.Key(name_.data(), name_.size());
        auto const& map = field_mappings_for_v<T>;
        w.StartObject();
        map.write_fields(field, w);
        w.EndObject();
    }

    template<std::same_as<unwrapped_type_t> T>
    void handle_write(std::optional<T> const& field, writer_t& w) const {
        if (field.has_value()) {
            handle_write(field.value(), w);
            return;
        }
        /// serialize null
        w.Key(name_.data(), name_.size());
        w.Null();
    }

    template<std::same_as<unwrapped_type_t> T>
    void handle_write(tristate<T> const& field, writer_t& w) const {
        if (field.is_disabled()) {
            /// disabled tristate are not serialized
            return;
        }
        handle_write(field.get_optional(), w);
    }

public:
    consteval aggregate_type_mapping(const char* name, MemPtr ptr)
      : name_{name}
      , field_{ptr} {}

    // initialize the field and return a new context that will handle all
    // key:value until end json object is reached
    auto try_start(underlying_klass_t& obj, std::string_view key) const
      -> std::unique_ptr<const object_mapping_context> {
        // return null if the key is not for this object_mapping
        if (key != name_) {
            return nullptr;
        }

        // ensure that optional/tristate has a value, and retrieve a ref to it
        unwrapped_type_t& sub_obj = tm_set_default_value(
          std::invoke(field_, obj));
        // retrieve the root_object_mapping for field_t
        auto const& map = field_mappings_for_v<unwrapped_type_t>;
        // ask it to create a context with the sub_obj, it will be used to
        // handle key=value until the end_object call
        return map.start(sub_obj);
    }

    // handle key: null, in case the field is an optional or a tristate
    auto try_read(std::string_view key, std::nullopt_t, underlying_klass_t& obj)
      const -> bool {
        if (name_ == key) {
            return tm_set_null(std::invoke(field_, obj));
        }
        return false;
    }

    // try_read overload with disabled_tristate_t, called to expliclitly disable
    // tristates.
    bool try_read(
      std::string_view key, disable_tristate_t, underlying_klass_t& obj) const {
        if (name_ == key) {
            return tm_set_disabled(std::invoke(field_, obj));
        }
        return false;
    }

    // an object mapping can only handle null and the creation of a new object
    auto try_read(std::string_view key, auto val, underlying_klass_t&) const
      -> bool {
        if (name_ == key) {
            throw from_json_error{key, val};
        }
        return false;
    }

    // serialization function. It delegates to one of the overloads of
    // handle_write, to support optional/tristate
    void write(underlying_klass_t const& obj, writer_t& w) const {
        handle_write(std::invoke(field_, obj), w);
    }

    // helper to write an array of disabled tristate that where not serialized
    void write_name_if_disabled_tristate(
      underlying_klass_t const& obj, writer_t& w) const {
        if constexpr (reflection::is_tristate<underlying_field_t>) {
            if (std::invoke(field_, obj).is_disabled()) {
                w.String(name_.data(), name_.size());
            }
        }
    }
    // name that will be used in json
    std::string_view name_;
    // pointer to the field in the struct
    MemPtr field_;
};

/// Defines the type_mapping for all the fields in Klass. It can represent
/// either a stand-alone object, or a subobject via the link from
/// aggregate_type_mapping
template<typename Klass, is_mapping... Fn>
class root_type_mapping {
    /// true iff any of the type_mapping is for a tristate: this is useful to
    /// record if some fields could be omitted from the json (like for the
    /// encoding of infinite retention)
    constexpr static auto has_tristate_properties() {
        return (
          reflection::is_tristate<typename Fn::underlying_field_t> || ...);
    }

    // private type-erased implementation of object_mapping_context that kowns
    // the actual types of the fields of Klass. it will handle every key:value
    // until end of json object is reached
    class impl final : public object_mapping_context {
        // uniform handle null, bool, integer, string
        template<typename T>
        auto handle_try_read(std::string_view key, T val) const -> bool {
            // interecept values for array `disabled_tristates_array_key`
            // and converts them to try_read(val, disable_tristate)
            if constexpr (
              has_tristate_properties()
              && std::constructible_from<std::string_view, T>) {
                if (key == disabled_tristates_array_key) {
                    return std::apply(
                      [&](auto const&... field) {
                          return (
                            field.try_read(val, disable_tristate, *obj_)
                            || ...);
                      },
                      field_mappings_for_v<Klass>.fields_);
                }
            }

            // intercept `serde_version_key: uint` to ensure that data is
            // compatible
            if constexpr (
              serde::is_envelope<Klass> && std::unsigned_integral<T>) {
                if (key == serde_version_key) {
                    if (val > std::numeric_limits<serde::version_t>::max()) {
                        throw std::invalid_argument{"conversion"};
                    }

                    return val >= Klass::redpanda_serde_compat_version;
                }

                if (key == serde_compat_version_key) {
                    if (val > std::numeric_limits<serde::version_t>::max()) {
                        throw std::invalid_argument{"conversion"};
                    }
                    return true;
                }
            }

            // push key: val to the fields
            return std::apply(
              [&](auto const&... field) {
                  return (field.try_read(key, val, *obj_) || ...);
              },
              field_mappings_for_v<Klass>.fields_);
        }

    public:
        auto start_subobject(std::string_view key) const
          -> std::unique_ptr<const object_mapping_context> override {
            // try to create a context for the object handled by "key"
            auto res = std::unique_ptr<const object_mapping_context>{};
            std::apply(
              [&]<typename... Fs>(Fs const&... field) {
                  ([&] {
                      // filter for aggregate_type_mapping
                      if constexpr (is_aggregate_mapping<Fs>) {
                          res = field.try_start(*obj_, key);
                          return bool(res);
                      }
                      return false;
                  }()
                   || ...);
              },
              field_mappings_for_v<Klass>.fields_);

            return res;
        }

        auto try_read(std::string_view key, std::nullopt_t val) const
          -> bool override {
            return handle_try_read(key, val);
        }

        auto try_read(std::string_view key, bool val) const -> bool override {
            return handle_try_read(key, val);
        }
        auto try_read(std::string_view key, std::string_view val) const
          -> bool override {
            return handle_try_read(key, val);
        }
        auto try_read(std::string_view key, int64_t val) const
          -> bool override {
            return handle_try_read(key, val);
        }
        auto try_read(std::string_view key, uint64_t val) const
          -> bool override {
            return handle_try_read(key, val);
        }

        explicit impl(Klass& obj) noexcept
          : obj_{&obj} {}

    private:
        // this points to the object to populate
        Klass* obj_;
    };

public:
    /// Create a mapping for a Klass from a collection of
    /// type_mapping/aggregate_type_mapping.
    /// \param Klass is used to enable constructor template argument deduction,
    /// only the type is meaningfull.
    /// Pass a nullptr like this: static_cast<T const*>(nullptr)
    consteval explicit root_type_mapping(Klass const*, Fn... fields)
      : fields_{fields...} {}

    /// create a new context that will handle key:value
    auto start(Klass& obj) const
      -> std::unique_ptr<const object_mapping_context> {
        // return a context pointing at this object
        return std::make_unique<const impl>(obj);
    }

    /// serialize all the fields of obj, it is expected that the caller will
    /// wrap this in StartObject EndObject
    void write_fields(Klass const& obj, writer_t& w) const {
        // for serde, serialize version and compat version to perform some
        // rejection in the read path
        if constexpr (serde::is_envelope<Klass>) {
            w.Key(serde_version_key.data(), serde_version_key.size());
            w.Int64(Klass::redpanda_serde_version);
            w.Key(
              serde_compat_version_key.data(), serde_compat_version_key.size());
            w.Int64(Klass::redpanda_serde_compat_version);
        }

        // write each field
        std::apply(
          [&](auto const&... field) { (field.write(obj, w), ...); }, fields_);

        // record in an array all the tristated that where not serialized. this
        // is useful for correctly decoding v2 manifest created from a redpanda
        // version that has less tristate properties than the current one
        if constexpr (has_tristate_properties()) {
            w.Key(
              disabled_tristates_array_key.data(),
              disabled_tristates_array_key.size());
            w.StartArray();
            std::apply(
              [&](auto const&... field) {
                  (field.write_name_if_disabled_tristate(obj, w), ...);
              },
              fields_);
            w.EndArray();
        }
    }

private:
    std::tuple<Fn...> fields_;
};

using rtp = cluster::remote_topic_properties;
/// table of mappings for cluster::remote_topic_properties
template<>
struct field_mappings_for_t<cluster::remote_topic_properties> {
    constexpr static auto value = root_type_mapping{
      static_cast<cluster::remote_topic_properties const*>(nullptr),
      type_mapping{
        "remote_revision",
        &rtp::remote_revision,
        [](model::initial_revision_id id) { return int64_t(id); },
        [](int64_t id) { return model::initial_revision_id(id); }},
      type_mapping{
        "remote_partition_count",
        &rtp::remote_partition_count,
        [](int32_t in) { return in; },
        [](int64_t in) {
            if (!std::in_range<int32_t>(in)) {
                throw std::invalid_argument{"conversion"};
            }
            return int32_t(in);
        }},
    };
};

using tp = cluster::topic_properties;
/// table of the json mappings for each field of cluster::topic_properties
template<>
struct field_mappings_for_t<cluster::topic_properties> {
    constexpr static auto value = root_type_mapping{
      static_cast<cluster::topic_properties const*>(nullptr),
      type_mapping{
        "compression",
        &tp::compression,
        boost::lexical_cast<std::string, model::compression>,
        boost::lexical_cast<model::compression, std::string_view>},
      type_mapping{
        "cleanup_policy_bitflags",
        &tp::cleanup_policy_bitflags,
        boost::lexical_cast<std::string, model::cleanup_policy_bitflags>,
        boost::lexical_cast<model::cleanup_policy_bitflags, std::string_view>},
      type_mapping{
        "compaction_strategy",
        &tp::compaction_strategy,
        boost::lexical_cast<std::string, model::compaction_strategy>,
        boost::lexical_cast<model::compaction_strategy, std::string_view>},
      type_mapping{
        "timestamp_type",
        &tp::timestamp_type,
        boost::lexical_cast<std::string, model::timestamp_type>,
        boost::lexical_cast<model::timestamp_type, std::string_view>},
      type_mapping{
        "segment_size",
        &tp::segment_size,
        [](size_t in) { return uint64_t(in); },
        []<std::integral I>(I in) -> std::optional<size_t> {
            if constexpr (std::signed_integral<I>) {
                if (in < 0) {
                    // special case: handle -1 values as empty (this is not the
                    // encoding performed by this code for size_t, it's due to
                    // an old redpanda encoding bug),
                    return std::nullopt;
                }
            }
            return size_t(in);
        }},
      type_mapping{
        "retention_bytes",
        &tp::retention_bytes,
        [](size_t in) { return uint64_t(in); },
        []<std::integral I>(I in) {
            if constexpr (std::signed_integral<I>) {
                if (in < 0) {
                    // special case: handle -1 values as disabled (this is not
                    // the encoding performed by this code for tristate, but
                    // could happen in manual edits)
                    return tristate<size_t>{disable_tristate};
                }
            }
            return tristate<size_t>{size_t(in)};
        }},
      type_mapping{
        "retention_duration",
        &tp::retention_duration,
        &std::chrono::milliseconds::count,
        []<std::integral I>(I in) {
            if constexpr (std::signed_integral<I>) {
                if (in < 0) {
                    // special case: handle -1 values as disabled (this is not
                    // the encoding performed by this code for tristate, but
                    // could happen in manual edits)
                    return tristate<std::chrono::milliseconds>{
                      disable_tristate};
                }
            }
            return tristate<std::chrono::milliseconds>{
              std::chrono::milliseconds(in)};
        }},
      type_mapping{
        "recovery",
        &tp::recovery,
        [](bool out) { return out; },
        [](bool in) { return in; }},
      type_mapping{
        "shadow_indexing",
        &tp::shadow_indexing,
        boost::lexical_cast<std::string, model::shadow_indexing_mode>,
        boost::lexical_cast<model::shadow_indexing_mode, std::string_view>},
      type_mapping{
        "read_replica",
        &tp::read_replica,
        [](bool out) { return out; },
        [](bool in) { return in; }},
      type_mapping{
        "read_replica_bucket",
        &tp::read_replica_bucket,
        [](ss::sstring const& s) { return std::string{s}; },
        [](std::string_view j) { return ss::sstring{j}; },
      },
      // remote_topic_properties is already handled by v1 of the
      // manifest with "revision_id" and "replication_factor", but it's included
      // here for completeness. being a serde struct, the mapping is handled by
      // another field_mappings_for_v
      aggregate_type_mapping{
        "remote_topic_properties", &tp::remote_topic_properties},
      type_mapping{
        "batch_max_bytes",
        &tp::batch_max_bytes,
        [](uint32_t out) { return uint64_t(out); },
        [](uint64_t in) {
            if (in > std::numeric_limits<uint32_t>::max()) {
                throw std::invalid_argument{"conversion"};
            }
            return uint32_t(in);
        }},
      type_mapping{
        "retention_local_target_bytes",
        &tp::retention_local_target_bytes,
        [](size_t in) { return uint64_t(in); },
        []<std::integral I>(I in) {
            if constexpr (std::signed_integral<I>) {
                if (in < 0) {
                    // special case: handle -1 values as disabled (this is not
                    // the encoding performed by this code for tristate, but
                    // could happen in manual edits)
                    return tristate<size_t>{disable_tristate};
                }
            }
            return tristate<size_t>{size_t(in)};
        }},
      type_mapping{
        "retention_local_target_ms",
        &tp::retention_local_target_ms,
        &std::chrono::milliseconds::count,
        []<std::integral I>(I in) {
            if constexpr (std::signed_integral<I>) {
                if (in < 0) {
                    // special case: handle -1 values as disabled (this is not
                    // the encoding performed by this code for tristate, but
                    // could happen in manual edits)
                    return tristate<std::chrono::milliseconds>{
                      disable_tristate};
                }
            }
            return tristate<std::chrono::milliseconds>{
              std::chrono::milliseconds(in)};
        }},
      type_mapping{
        "remote_delete",
        &tp::remote_delete,
        [](bool out) { return out; },
        [](bool in) { return in; }},
      type_mapping{
        "segment_ms",
        &tp::segment_ms,
        &std::chrono::milliseconds::count,
        []<std::integral I>(I in) {
            if constexpr (std::signed_integral<I>) {
                if (in < 0) {
                    // special case: handle -1 values as disabled (this is not
                    // the encoding performed by this code for tristate, but
                    // could happen in manual edits)
                    return tristate<std::chrono::milliseconds>{
                      disable_tristate};
                }
            }
            return tristate<std::chrono::milliseconds>{
              std::chrono::milliseconds(in)};
        }},
      type_mapping{
        "record_key_schema_id_validation",
        &tp::record_key_schema_id_validation,
        [](bool out) { return out; },
        [](bool in) { return in; }},
      type_mapping{
        "record_key_schema_id_validation_compat",
        &tp::record_key_schema_id_validation_compat,
        [](bool out) { return out; },
        [](bool in) { return in; }},
      type_mapping{
        "record_key_subject_name_strategy",
        &tp::record_key_subject_name_strategy,
        boost::lexical_cast<
          std::string,
          pandaproxy::schema_registry::subject_name_strategy>,
        boost::lexical_cast<
          pandaproxy::schema_registry::subject_name_strategy,
          std::string_view>},
      type_mapping{
        "record_key_subject_name_strategy_compat",
        &tp::record_key_subject_name_strategy_compat,
        boost::lexical_cast<
          std::string,
          pandaproxy::schema_registry::subject_name_strategy>,
        boost::lexical_cast<
          pandaproxy::schema_registry::subject_name_strategy,
          std::string_view>},
      type_mapping{
        "record_value_schema_id_validation",
        &tp::record_value_schema_id_validation,
        [](bool out) { return out; },
        [](bool in) { return in; }},
      type_mapping{
        "record_value_schema_id_validation_compat",
        &tp::record_value_schema_id_validation_compat,
        [](bool out) { return out; },
        [](bool in) { return in; }},
      type_mapping{
        "record_value_subject_name_strategy",
        &tp::record_value_subject_name_strategy,
        boost::lexical_cast<
          std::string,
          pandaproxy::schema_registry::subject_name_strategy>,
        boost::lexical_cast<
          pandaproxy::schema_registry::subject_name_strategy,
          std::string_view>},
      type_mapping{
        "record_value_subject_name_strategy_compat",
        &tp::record_value_subject_name_strategy_compat,
        boost::lexical_cast<
          std::string,
          pandaproxy::schema_registry::subject_name_strategy>,
        boost::lexical_cast<
          pandaproxy::schema_registry::subject_name_strategy,
          std::string_view>},
      type_mapping{
        "initial_retention_local_target_bytes",
        &tp::initial_retention_local_target_bytes,
        [](size_t in) { return uint64_t(in); },
        []<std::integral I>(I in) {
            if constexpr (std::signed_integral<I>) {
                if (in < 0) {
                    // special case: handle -1 values as disabled (this is not
                    // the encoding performed by this code for tristate, but
                    // could happen in manual edits)
                    return tristate<size_t>{disable_tristate};
                }
            }
            return tristate<size_t>{size_t(in)};
        }},
      type_mapping{
        "initial_retention_local_target_ms",
        &tp::initial_retention_local_target_ms,
        &std::chrono::milliseconds::count,
        []<std::integral I>(I in) {
            if constexpr (std::signed_integral<I>) {
                if (in < 0) {
                    // special case: handle -1 values as disabled (this is not
                    // the encoding performed by this code for tristate, but
                    // could happen in manual edits)
                    return tristate<std::chrono::milliseconds>{
                      disable_tristate};
                }
            }
            return tristate<std::chrono::milliseconds>{
              std::chrono::milliseconds(in)};
        }},
    };
};

struct topic_manifest_json_info {
    std::optional<int32_t> _version{};
    std::optional<model::ns> _namespace{};
    std::optional<model::topic> _topic{};
    std::optional<int32_t> _partition_count{};
    std::optional<int16_t> _replication_factor{};
    std::optional<model::initial_revision_id> _revision_id{};
};

using tmj = topic_manifest_json_info;
template<>
struct field_mappings_for_t<topic_manifest_json_info> {
    constexpr static auto value = root_type_mapping{
      static_cast<topic_manifest_json_info const*>(nullptr),
      type_mapping{
        "version",
        &tmj::_version,
        [](int32_t v) { return int64_t(v); },
        [](int64_t jv) {
            if (!std::in_range<int32_t>(jv)) {
                throw std::invalid_argument{"conversion"};
            }
            return int32_t(jv);
        }},
      type_mapping{
        "namespace",
        &tmj::_namespace,
        [](model::ns const& ns) { return std::string{ns()}; },
        [](std::string_view jns) { return model::ns{ss::sstring{jns}}; }},
      type_mapping{
        "topic",
        &tmj::_topic,
        [](model::topic const& tp) { return std::string{tp()}; },
        [](std::string_view jtp) { return model::topic{ss::sstring{jtp}}; }},
      type_mapping{
        "partition_count",
        &tmj::_partition_count,
        [](int32_t pc) { return int64_t(pc); },
        [](int64_t jpc) {
            if (!std::in_range<int32_t>(jpc)) {
                throw std::invalid_argument{"conversion"};
            }
            return int32_t(jpc);
        }},
      type_mapping{
        "replication_factor",
        &tmj::_replication_factor,
        [](int16_t rf) { return int64_t(rf); },
        [](int64_t jrf) {
            if (!std::in_range<int16_t>(jrf)) {
                throw std::invalid_argument{"conversion"};
            }
            return int16_t(jrf);
        }},
      type_mapping{
        "revision_id",
        &tmj::_revision_id,
        [](model::initial_revision_id ri) { return ri(); },
        [](int64_t jri) { return model::initial_revision_id{jri}; }},
    };
};

} // namespace
namespace cloud_storage {

class topic_manifest_handler
  : public json::BaseReaderHandler<json::UTF8<>, topic_manifest_handler> {
    enum class state {
        expect_root_start,
        expect_key,
        expect_value,
        expect_array_value
    };
    using key_string = ss::sstring;

    template<typename T>
    bool handle_val(T val) {
        if (context_stack_.empty()) {
            // error condition
            return false;
        }
        if (state_ == state::expect_value) {
            // normal key: value pair
            auto res = legacy_context_->try_read(json_key_, val)
                       || context_stack_.back()->try_read(json_key_, val);
            state_ = state::expect_key;
            return res;
        }
        if (state_ == state::expect_array_value) {
            return context_stack_.back()->try_read(json_key_, val);
        }
        return false;
    }

public:
    /// these are the handlers called from rapidjson
    bool StartObject() {
        // called at the start of the object. setup the first context and wait
        // for keys
        switch (state_) {
        case state::expect_root_start:
            legacy_context_
              = field_mappings_for_v<topic_manifest_json_info>.start(
                v1_manifest_json_info_);
            context_stack_.push_back(
              field_mappings_for_v<cluster::topic_properties>.start(
                properties_));
            state_ = state::expect_key;
            return true;
        case state::expect_value: {
            if (context_stack_.empty()) {
                // we are expecting a subobject but we have no key for it and
                // the invariant of a non-empty stack is broken
                return false;
            }
            // try to create a new context for the next seq of key:value pairs
            auto new_ctx = context_stack_.back()->start_subobject(json_key_);
            if (!new_ctx) {
                // no key match, signal an error
                return false;
            }

            context_stack_.push_back(std::move(new_ctx));
            state_ = state::expect_key;
            return true;
        }
        case state::expect_array_value:
        case state::expect_key:
            return false;
        }
    }

    bool EndObject(json::SizeType = {}) {
        if (context_stack_.empty() || state_ != state::expect_key) {
            // illegal sequence of actions
            return false;
        }

        // pop the context stack, new key:value pairs will be directed to the
        // new context
        context_stack_.pop_back();
        if (context_stack_.empty()) {
            legacy_context_.reset();
            state_ = state::expect_root_start;
        } else {
            state_ = state::expect_key;
        }
        return true;
    }

    bool StartArray() {
        if (state_ != state::expect_value) {
            // illegal sequence of actions
            return false;
        }
        state_ = state::expect_array_value;
        return true;
    }
    bool EndArray(json::SizeType = {}) {
        if (state_ != state::expect_array_value) {
            // illegal sequence of actions
            return false;
        }
        state_ = state::expect_key;
        return true;
    }

    bool Key(const char* str, json::SizeType length, bool = {}) {
        if (state_ != state::expect_key) {
            return false;
        }
        json_key_ = key_string(str, length);
        state_ = state::expect_value;
        return true;
    }

    bool Null() { return handle_val(std::nullopt); }
    bool Bool(bool b) { return handle_val(b); }
    bool Int(int i) { return handle_val(int64_t(i)); }
    bool Int64(int64_t i) { return handle_val(int64_t(i)); }
    bool Uint(unsigned u) { return handle_val(uint64_t(u)); }
    bool Uint64(uint64_t u) { return handle_val(uint64_t(u)); }
    bool String(const char* str, json::SizeType length, bool = {}) {
        return handle_val(std::string_view{str, length});
    }
    bool Default() { return false; }

    /// deserialization will write here
    topic_manifest_json_info v1_manifest_json_info_{};
    cluster::topic_properties properties_{};

    topic_manifest_handler() noexcept {
        properties_.retention_duration = tristate<std::chrono::milliseconds>{
          disable_tristate};
        properties_.retention_bytes = tristate<size_t>{disable_tristate};
    }

private:
    /// current key
    key_string json_key_{};

    std::unique_ptr<const object_mapping_context> legacy_context_{};
    /// stack of object_mapping_context, needed to keep track of the correct
    /// sub-object this could have static dimension with the correct
    /// datastructure (just allocate the max depth, every nested object_mapping
    /// is +1)
    std::vector<std::unique_ptr<const object_mapping_context>> context_stack_{};

    state state_{state::expect_root_start};
};

topic_manifest::topic_manifest(
  const cluster::topic_configuration& cfg, model::initial_revision_id rev)
  : _topic_config(cfg)
  , _rev(rev) {}

topic_manifest::topic_manifest()
  : _topic_config(std::nullopt) {}

void topic_manifest::do_update(const topic_manifest_handler& handler) {
    auto& v1_handler = handler.v1_manifest_json_info_;
    if (
      !v1_handler._version
      || v1_handler._version > topic_manifest::current_version) {
        throw std::runtime_error(fmt_with_ctx(
          fmt::format,
          "topic manifest version {} is not supported",
          v1_handler._version,
          topic_manifest::current_version));
    }

    _manifest_version = v1_handler._version.value();

    _rev = v1_handler._revision_id.value();

    if (!v1_handler._namespace) {
        throw std::runtime_error(fmt_with_ctx(
          fmt::format, "Missing _namespace value in parsed topic manifest"));
    }
    if (!v1_handler._topic) {
        throw std::runtime_error(fmt_with_ctx(
          fmt::format, "Missing _topic value in parsed topic manifest"));
    }
    if (!v1_handler._partition_count) {
        throw std::runtime_error(fmt_with_ctx(
          fmt::format,
          "Missing _partition_count value in parsed topic manifest"));
    }
    if (!v1_handler._replication_factor) {
        throw std::runtime_error(fmt_with_ctx(
          fmt::format,
          "Missing _replication_factor value in parsed topic manifest"));
    }
    if (!v1_handler._revision_id) {
        throw std::runtime_error(fmt_with_ctx(
          fmt::format, "Missing _revision_id value in parsed topic manifest"));
    }

    _topic_config = cluster::topic_configuration{
      model::ns(v1_handler._namespace.value()),
      model::topic(v1_handler._topic.value()),
      v1_handler._partition_count.value(),
      v1_handler._replication_factor.value()};
    if (
      _manifest_version < topic_manifest::cluster_topic_configuration_version) {
        // if the manifest is old, just copy the properties that where
        // serialized with it, to ensure that the rest gets a default value and
        // not an artifact of tristate deserialization
        auto& props = _topic_config->properties;
        auto& handler_props = handler.properties_;
        std::tie(
          props.compression,
          props.cleanup_policy_bitflags,
          props.compaction_strategy,
          props.timestamp_type,
          props.segment_size,
          props.retention_bytes,
          props.retention_duration)
          = std::tie(
            handler_props.compression,
            handler_props.cleanup_policy_bitflags,
            handler_props.compaction_strategy,
            handler_props.timestamp_type,
            handler_props.segment_size,
            handler_props.retention_bytes,
            handler_props.retention_duration);
    } else {
        _topic_config->properties = handler.properties_;
    }
}

ss::future<> topic_manifest::update(ss::input_stream<char> is) {
    iobuf result;
    auto os = make_iobuf_ref_output_stream(result);
    co_await ss::copy(is, os);
    iobuf_istreambuf ibuf(result);
    std::istream stream(&ibuf);

    json::IStreamWrapper wrapper(stream);
    json::Reader reader;
    topic_manifest_handler handler;

    try {
        // successful parse will update _topic_config and return here
        if (reader.Parse(wrapper, handler)) {
            vlog(cst_log.debug, "Parsed successfully!");
            topic_manifest::do_update(handler);
            co_return;
        }
    } catch (from_json_error const& e) {
        // this exception is thrown from a field_mapper while trying to convert
        // a value
        throw std::runtime_error{fmt_with_ctx(
          fmt::format,
          "Failed to parse topic manifest {}: Invalid {}: {}",
          get_topic_manifest_path(
            handler.v1_manifest_json_info_._namespace.value_or(
              model::ns{"[no namespace]"}),
            handler.v1_manifest_json_info_._topic.value_or(
              model::topic{"[no topic]"})),
          e.key,
          e.invalid_value)};
    }

    // parse was not successful for some syntactic error in the json
    rapidjson::ParseErrorCode e = reader.GetParseErrorCode();
    size_t o = reader.GetErrorOffset();

    if (_topic_config) {
        throw std::runtime_error(fmt_with_ctx(
          fmt::format,
          "Failed to parse topic manifest {}: {} at offset {}",
          get_manifest_path(),
          rapidjson::GetParseError_En(e),
          o));
    } else {
        throw std::runtime_error(fmt_with_ctx(
          fmt::format,
          "Failed to parse topic manifest: {} at offset {}",
          rapidjson::GetParseError_En(e),
          o));
    }
}

ss::future<serialized_data_stream> topic_manifest::serialize() const {
    iobuf serialized;
    iobuf_ostreambuf obuf(serialized);
    std::ostream os(&obuf);
    serialize(os);
    if (!os.good()) {
        throw std::runtime_error(fmt_with_ctx(
          fmt::format,
          "could not serialize topic manifest {}",
          get_manifest_path()));
    }
    size_t size_bytes = serialized.size_bytes();
    co_return serialized_data_stream{
      .stream = make_iobuf_input_stream(std::move(serialized)),
      .size_bytes = size_bytes};
}

void topic_manifest::serialize(std::ostream& out) const {
    json::OStreamWrapper wrapper(out);
    writer_t w(wrapper);
    w.StartObject();

    // serialize v1 data
    field_mappings_for_v<topic_manifest_json_info>.write_fields(
      {
        ._version = topic_manifest::current_version,
        ._namespace = _topic_config->tp_ns.ns,
        ._topic = _topic_config->tp_ns.tp,
        ._partition_count = _topic_config->partition_count,
        ._replication_factor = _topic_config->replication_factor,
        ._revision_id = _rev,
      },
      w);

    // serialize each field of cluster::topic_properties using the mapping
    field_mappings_for_v<cluster::topic_properties>.write_fields(
      _topic_config->properties, w);

    w.EndObject();
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
