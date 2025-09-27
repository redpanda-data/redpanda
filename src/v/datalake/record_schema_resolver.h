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

class schema_cache {
public:
    using key_t = pandaproxy::schema_registry::schema_id;
    using val_t = pandaproxy::schema_registry::valid_schema;

    virtual ss::optimized_optional<ss::shared_ptr<val_t>>
    get_value(const key_t&) = 0;

    virtual bool try_insert(const key_t&, ss::shared_ptr<val_t>) = 0;
    virtual void start() = 0;
    virtual void stop() = 0;

    virtual ~schema_cache() = default;
};

class chunked_schema_cache : public schema_cache {
public:
    using cache_t = utils::chunked_kv_cache<key_t, val_t>;

    explicit chunked_schema_cache(cache_t::config config);
    ~chunked_schema_cache() override = default;

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

    resolved_type copy() const;
};

struct type_and_buf {
    std::optional<resolved_type> type;

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
    virtual ss::future<checked<resolved_type, errc>>
      resolve_identifier(schema_identifier) const = 0;
    virtual ~type_resolver() = default;
};

// binary_type_resolver is the type resolver for the raw key_value mode of
// iceberg.
class binary_type_resolver : public type_resolver {
public:
    ss::future<checked<type_and_buf, type_resolver::errc>>
    resolve_buf_type(std::optional<iobuf> b) const override;

    ss::future<checked<resolved_type, errc>>
      resolve_identifier(schema_identifier) const override;
    ~binary_type_resolver() override = default;
};

class test_binary_type_resolver : public binary_type_resolver {
public:
    ss::future<checked<type_and_buf, type_resolver::errc>>
    resolve_buf_type(std::optional<iobuf> b) const override;

    ss::future<checked<resolved_type, errc>>
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
      std::optional<std::reference_wrapper<schema_cache>> sc = std::nullopt)
      : sr_(sr)
      , cache_(sc) {}

    ss::future<checked<type_and_buf, type_resolver::errc>>
    resolve_buf_type(std::optional<iobuf> b) const override;

    ss::future<checked<resolved_type, errc>>
      resolve_identifier(schema_identifier) const override;
    ~record_schema_resolver() override = default;

private:
    schema::registry& sr_;
    std::optional<std::reference_wrapper<schema_cache>> cache_;
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
      config::binding<std::chrono::milliseconds> cache_duration,
      std::optional<std::reference_wrapper<schema_cache>> sc);
    latest_subject_schema_resolver(const latest_subject_schema_resolver&)
      = delete;
    latest_subject_schema_resolver(latest_subject_schema_resolver&&) = delete;
    latest_subject_schema_resolver&
    operator=(const latest_subject_schema_resolver&)
      = delete;
    latest_subject_schema_resolver& operator=(latest_subject_schema_resolver&&)
      = delete;
    ~latest_subject_schema_resolver() override = default;

    ss::future<checked<type_and_buf, type_resolver::errc>>
    resolve_buf_type(std::optional<iobuf> b) const override;

    ss::future<checked<resolved_type, errc>>
      resolve_identifier(schema_identifier) const override;

private:
    schema::registry* sr_;
    pandaproxy::schema_registry::subject subject_;
    std::optional<ss::sstring> protobuf_message_name_;
    config::binding<std::chrono::milliseconds> cache_duration_;
    std::optional<std::reference_wrapper<schema_cache>> cache_;
    struct cached_schema {
        resolved_type type;
        ss::lowres_clock::time_point created_time;
    };
    mutable std::optional<cached_schema> latest_cached_schema_;
};

} // namespace datalake
