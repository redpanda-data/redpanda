/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "base/seastarx.h"
#include "config/property.h"
#include "datalake/schema_identifier.h"
#include "iceberg/datatypes.h"
#include "metrics/metrics.h"
#include "pandaproxy/schema_registry/types.h"
#include "utils/chunked_kv_cache.h"

#include <seastar/core/future.hh>

#include <chrono>

namespace iceberg {
class json_conversion_ir;
}

namespace schema {
class registry;
} // namespace schema

namespace google::protobuf {
class Descriptor;
} // namespace google::protobuf

namespace datalake {

template<typename Key, typename Value>
class datalake_cache {
public:
    using key_t = Key;
    using val_t = Value;

    virtual ss::optimized_optional<ss::shared_ptr<val_t>>
    get_value(const key_t&) = 0;

    virtual bool try_insert(const key_t&, ss::shared_ptr<val_t>) = 0;
    virtual void start() = 0;
    virtual void stop() = 0;

    virtual ~datalake_cache() = default;
};

template<typename Key, typename Value>
class chunked_datalake_cache : public datalake_cache<Key, Value> {
public:
    using key_t = Key;
    using val_t = Value;
    using cache_t = utils::chunked_kv_cache<key_t, val_t>;

    explicit chunked_datalake_cache(typename cache_t::config config);
    ~chunked_datalake_cache() override = default;

    ss::optimized_optional<ss::shared_ptr<val_t>>
    get_value(const key_t&) override;
    bool try_insert(const key_t&, ss::shared_ptr<val_t>) override;

    void start() override;
    void stop() override;

private:
    cache_t cache_;
    metrics::internal_metric_groups metrics_;

    void setup_metrics();
};

using schema_cache = datalake_cache<
  pandaproxy::schema_registry::schema_id,
  pandaproxy::schema_registry::valid_schema>;
using chunked_schema_cache = chunked_datalake_cache<
  pandaproxy::schema_registry::schema_id,
  pandaproxy::schema_registry::valid_schema>;

using shared_schema_t
  = ss::shared_ptr<pandaproxy::schema_registry::valid_schema>;

// Represents an object that can be converted into an Iceberg schema.
// NOTE: these aren't exactly just the schemas from the registry: Protobuf
// schemas are FileDescriptors in the registry rather than Descriptors, and
// require additional information to get the Descriptors.
class resolved_schema {
    using storage_t = std::
      variant<shared_schema_t, ss::shared_ptr<iceberg::json_conversion_ir>>;

public:
    using resolved_schema_t = std::variant<
      std::reference_wrapper<const google::protobuf::Descriptor>,
      std::reference_wrapper<const avro::ValidSchema>,
      std::reference_wrapper<const iceberg::json_conversion_ir>>;

    resolved_schema(resolved_schema_t schema, shared_schema_t shared_schema)
      : shared_schema_(std::move(shared_schema))
      , schema_(schema) {}

    explicit resolved_schema(ss::shared_ptr<iceberg::json_conversion_ir>);

    resolved_schema_t get_schema_ref() const noexcept { return schema_; }

private:
    // Note that `schema_` is a reference to data owned by `shared_schema_`.
    storage_t shared_schema_;
    resolved_schema_t schema_;
};

struct resolved_type {
    // The resolved schema that corresponds to the type.
    resolved_schema schema;
    schema_identifier id;

    // The schema (and offsets, for protobuf), translated into an
    // Iceberg-compatible type. Note, the field IDs may not necessarily
    // correspond to their final IDs in the catalog.
    iceberg::field_type type;
};

using shared_resolved_type_t = ss::shared_ptr<const resolved_type>;

// Note that the resolved type cache needs to be keyed by the
// `schema_identifier` which can only be fully determined once the schema is
// known. Hence the resolved type can't be stored inline to the schema cache.
using resolved_type_cache = datalake_cache<schema_identifier, resolved_type>;
using chunked_resolved_type_cache
  = chunked_datalake_cache<schema_identifier, resolved_type>;

struct type_and_buf {
    std::optional<shared_resolved_type_t> type;

    // Part of a record field (key or value) that conforms to the given Iceberg
    // field type.
    std::optional<iobuf> parsable_buf;

    // Constructs a type that indicates that the record didn't have a schema or
    // there was an issue trying to parse the schema, in which case we need to
    // fall back to representing the value as a binary blob column.
    static type_and_buf make_raw_binary(std::optional<iobuf> buf);
};

class type_resolver {
public:
    enum class errc {
        registry_error,
        translation_error,
        bad_input,
        invalid_config,
    };
    friend std::ostream& operator<<(std::ostream&, const errc&);
    virtual ss::future<checked<type_and_buf, errc>>
    resolve_buf_type(std::optional<iobuf> b) const = 0;
    // TODO(iceberg): This should be it's own interface.
    virtual ss::future<checked<shared_resolved_type_t, errc>>
      resolve_identifier(schema_identifier) const = 0;
    virtual ~type_resolver() = default;
};

// binary_type_resolver is the type resolver for the raw key_value mode of
// iceberg.
class binary_type_resolver : public type_resolver {
public:
    ss::future<checked<type_and_buf, type_resolver::errc>>
    resolve_buf_type(std::optional<iobuf> b) const override;

    ss::future<checked<shared_resolved_type_t, errc>>
      resolve_identifier(schema_identifier) const override;
    ~binary_type_resolver() override = default;
};

class test_binary_type_resolver : public binary_type_resolver {
public:
    ss::future<checked<type_and_buf, type_resolver::errc>>
    resolve_buf_type(std::optional<iobuf> b) const override;

    ss::future<checked<shared_resolved_type_t, errc>>
      resolve_identifier(schema_identifier) const override;
    ~test_binary_type_resolver() override = default;
    void set_fail_requests(type_resolver::errc e) { injected_error_ = e; }

private:
    std::optional<type_resolver::errc> injected_error_{};
};

// record_schema_resolver uses the schema registry wire format
// to decode messages and resolve their schemas.
class record_schema_resolver : public type_resolver {
public:
    explicit record_schema_resolver(
      schema::registry& sr,
      std::optional<std::reference_wrapper<schema_cache>> sc = std::nullopt,
      std::optional<std::reference_wrapper<resolved_type_cache>> rc
      = std::nullopt)
      : sr_(sr)
      , cache_(sc)
      , resolved_type_cache_(rc) {}

    ss::future<checked<type_and_buf, type_resolver::errc>>
    resolve_buf_type(std::optional<iobuf> b) const override;

    ss::future<checked<shared_resolved_type_t, errc>>
      resolve_identifier(schema_identifier) const override;
    ~record_schema_resolver() override = default;

private:
    schema::registry& sr_;
    std::optional<std::reference_wrapper<schema_cache>> cache_;
    std::optional<std::reference_wrapper<resolved_type_cache>>
      resolved_type_cache_;
};

// latest_subject_schema_resolver is a schema resolver that uses the latest
// schema for a subject to parse records with a configurable cache duration.
//
// If the schema is protobuf then the first protobuf defined in the proto file
// will be used to parse the record unless `protobuf_message_name` is specified,
// then the protobuf with that specific name is used for parsing.
class latest_subject_schema_resolver : public type_resolver {
public:
    latest_subject_schema_resolver(
      schema::registry& sr,
      pandaproxy::schema_registry::subject subject,
      std::optional<ss::sstring> protobuf_message_name,
      config::binding<std::chrono::milliseconds> cache_ttl,
      std::optional<std::reference_wrapper<schema_cache>> sc,
      std::optional<std::reference_wrapper<resolved_type_cache>> rc);
    latest_subject_schema_resolver(const latest_subject_schema_resolver&)
      = delete;
    latest_subject_schema_resolver(latest_subject_schema_resolver&&) = delete;
    latest_subject_schema_resolver&
    operator=(const latest_subject_schema_resolver&) = delete;
    latest_subject_schema_resolver&
    operator=(latest_subject_schema_resolver&&) = delete;
    ~latest_subject_schema_resolver() override = default;

    ss::future<checked<type_and_buf, type_resolver::errc>>
    resolve_buf_type(std::optional<iobuf> b) const override;

    ss::future<checked<shared_resolved_type_t, errc>>
      resolve_identifier(schema_identifier) const override;

private:
    schema::registry* sr_;
    pandaproxy::schema_registry::subject subject_;
    std::optional<ss::sstring> protobuf_message_name_;
    config::binding<std::chrono::milliseconds> cache_ttl_;
    std::optional<std::reference_wrapper<schema_cache>> cache_;
    std::optional<std::reference_wrapper<resolved_type_cache>>
      resolved_type_cache_;

    class schema_lookup_cache {
    public:
        schema_lookup_cache() = default;

        schema_lookup_cache(
          checked<shared_resolved_type_t, type_resolver::errc> entry,
          ss::lowres_clock::time_point timestamp)
          : entry_(std::move(entry))
          , last_update_(timestamp) {}

    public:
        ss::lowres_clock::duration time_until_expiry(
          ss::lowres_clock::time_point timestamp,
          ss::lowres_clock::duration ttl) const {
            return ttl - age(timestamp);
        }

        ss::lowres_clock::duration
        age(ss::lowres_clock::time_point timestamp) const {
            return timestamp - last_update_;
        }

        const checked<shared_resolved_type_t, type_resolver::errc>&
        entry() const {
            return entry_;
        }

    private:
        checked<shared_resolved_type_t, type_resolver::errc> entry_
          = errc::registry_error;
        ss::lowres_clock::time_point last_update_;
    };
    mutable schema_lookup_cache schema_lookup_cache_;
};

} // namespace datalake
