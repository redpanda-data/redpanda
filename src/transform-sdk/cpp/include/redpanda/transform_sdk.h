// Copyright 2023 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <chrono>
#include <cstddef>
#include <expected>
#include <functional>
#include <optional>
#include <ranges>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <vector>

namespace detail {

template<typename T, typename Tag>
struct simple_named_type {
    using type = T;
    explicit simple_named_type(type val)
      : _val(val) {}
    [[nodiscard]] type value() const { return _val; }
    constexpr operator type() const { return _val; }
    type* data() { return &_val; }

private:
    friend bool operator==(
      const simple_named_type<type, Tag>&, const simple_named_type<type, Tag>&)
      = default;
    type _val;
};

} // namespace detail

namespace redpanda {

/**
 * Owned variable sized byte array.
 */
using bytes = std::vector<uint8_t>;

/**
 * Unowned variable sized byte array.
 *
 * This is not `std::span` because `std::span` does not provide
 * an equality operator, and it's a mutable view, where we want an immutable
 * one.
 */
class bytes_view : public std::ranges::view_interface<bytes_view> {
public:
    /** A default empty view of bytes. */
    bytes_view() = default;
    /** A view over `size` bytes starting at `start`. */
    bytes_view(bytes::const_pointer start, size_t size);
    /** A view over `size` bytes starting at `start`. */
    bytes_view(bytes::const_iterator start, size_t size);
    /** A view from `start` to `end`. */
    bytes_view(bytes::const_iterator start, bytes::const_iterator end);
    /** A view from `start` to `end`. */
    bytes_view(bytes::const_pointer start, bytes::const_pointer end);
    /** A view over an entire bytes range. */
    // NOLINTNEXTLINE(*-explicit-*)
    bytes_view(const bytes&);
    /** A view over the bytes in a string. */
    explicit bytes_view(const std::string& str);
    /** A view over the bytes in a string view. */
    explicit bytes_view(std::string_view str);

    /** The start of this range. */
    [[nodiscard]] bytes::const_pointer begin() const;
    /** The end of this range. */
    [[nodiscard]] bytes::const_pointer end() const;
    /**
     * Create a subview of this view. Similar to `std::span::subspan` or
     * `std::string_view::substr`.
     */
    [[nodiscard]] bytes_view
    subview(size_t offset, size_t count = std::dynamic_extent) const;

    /** Access the nth element of the view. */
    bytes::value_type operator[](size_t n) const;

    /** Returns true if the views have the same contents. */
    bool operator==(const bytes_view& other) const;

    /** Convert this bytes view into a string view. */
    explicit operator std::string_view() const;

private:
    bytes::const_pointer _start{}, _end{};
};

/**
 * A zero-copy `header`.
 */
struct header_view {
    std::string_view key;
    std::optional<bytes_view> value;

    bool operator==(const header_view&) const = default;
};

/**
 * A zero-copy representation of a record within Redpanda.
 *
 * `review_view` are handed to [`on_record_written`] event handlers as the
 * record that Redpanda wrote.
 */
struct record_view {
    std::optional<bytes_view> key;
    std::optional<bytes_view> value;
    std::vector<header_view> headers;

    bool operator==(const record_view&) const = default;
};

/**
 * Records may have a collection of headers attached to them.
 *
 * Headers are opaque to the broker and are purely a mechanism for the producer
 * and consumers to pass information.
 */
struct header {
    std::string key;
    std::optional<bytes> value;

    // NOLINTNEXTLINE(*-explicit-*)
    operator header_view() const;
    bool operator==(const header&) const = default;
};

/**
 * A record in Redpanda.
 *
 * Records are generated as the result of any transforms that act upon a
 * `record_view`.
 */
struct record {
    std::optional<bytes> key;
    std::optional<bytes> value;
    std::vector<header> headers;

    // NOLINTNEXTLINE(*-explicit-*)
    operator record_view() const;
    bool operator==(const record&) const = default;
};

/**
 * A persisted record written to a topic within Redpanda.
 *
 * It is similar to a `record_view` except that it also carries a timestamp of
 * when it was produced.
 */
struct written_record {
    std::optional<bytes_view> key;
    std::optional<bytes_view> value;
    std::vector<header_view> headers;
    std::chrono::system_clock::time_point timestamp;

    // NOLINTNEXTLINE(*-explicit-*)
    operator record_view() const;
    bool operator==(const written_record&) const = default;
};

/**
 * An event generated after a write event within the broker.
 */
struct write_event {
    /** The record that was written as part of this event. */
    written_record record;
};

/**
 * A writer for transformed records that are output to the destination topic.
 */
class record_writer {
public:
    record_writer() = default;
    record_writer(const record_writer&) = delete;
    record_writer(record_writer&&) = delete;
    record_writer& operator=(const record_writer&) = delete;
    record_writer& operator=(record_writer&&) = delete;
    virtual ~record_writer() = default;

    /**
     * Write a record to the output topic, returning any errors.
     */
    virtual std::error_code write(record_view) = 0;
};

/**
 * A callback to process write events and respond with a number of records to
 * write back to the output topic.
 */
using on_record_written_callback
  = std::function<std::error_code(write_event, record_writer*)>;

/**
 * Register a callback to be fired when a record is written to the input topic.
 *
 * This callback is triggered after the record has been written and fsynced to
 * disk and the producer has been acknowledged.
 *
 * This method blocks and runs forever, it should be called from `main` and any
 * setup that is needed can be done before calling this method.
 *
 * # Example
 *
 * The following example copies every record on the input topic indentically to
 * the output topic.
 *
 * ```cpp
 * int main() {
 *   redpanda::on_record_written(
 *       [](redpanda::write_event event, redpanda::record_writer* writer) {
 *          return writer->write(event.record);
 *       });
 * }
 * ```
 */
[[noreturn]] void on_record_written(const on_record_written_callback& callback);

namespace sr {

/**
 * enum indicating the format of some schema
 * Supported formats:
 *   - avro (fully supported)
 *   - protobuf (fully supported)
 *   - json (redpanda v24.2+ WIP)
 */
enum class schema_format {
    avro = 0,
    protobuf = 1,
    json = 2,
};

/**
 * Type-safe wrapper for a the ID (int32) of some registered schema
 */
using schema_id = detail::simple_named_type<int32_t, struct schema_id_tag>;

/**
 * Type-safe wrapper for a bare schema version (int32)
 */
using schema_version
  = detail::simple_named_type<int32_t, struct schema_version_tag>;

/**
 * Aggregate representing a reference to a schema defined elsewhere.
 * The details are format dependent; e.g Avro schemas will use a different
 * reference syntax from protobuf of JSON schemas. For more information , see:
 *
 *   https://docs.redpanda.com/current/manage/schema-reg/schema-reg-api/#reference-a-schema
 */
struct reference {
    std::string name;
    std::string subject;
    schema_version version;

private:
    friend bool operator==(const reference&, const reference&) = default;
};

/**
 * A schema that can be registered to schema registry
 */
struct schema {
    using reference_container = std::vector<reference>;

    std::string raw_schema;
    schema_format format;
    reference_container references;

    /**
     * Construct a new schema in Avro format
     */
    [[nodiscard]] static schema new_avro(
      std::string schema,
      std::optional<reference_container> refs = std::nullopt);

    /**
     * Construct a new schema in protobuf format
     */
    [[nodiscard]] static schema new_protobuf(
      std::string schema,
      std::optional<reference_container> refs = std::nullopt);

    /**
     * Construct a new schema in json format
     */
    [[nodiscard]] static schema new_json(
      std::string schema,
      std::optional<reference_container> refs = std::nullopt);

    friend bool operator==(const schema&, const schema&) = default;
};

/**
 * A schema along with its subject, version, and ID
 */
struct subject_schema {
    sr::schema schema;
    std::string subject;
    schema_version version;
    schema_id id;

    friend bool operator==(const subject_schema&, const subject_schema&)
      = default;
};

/**
 * A client for interacting with the schema registry within Redpanda.
 */
class schema_registry_client {
public:
    schema_registry_client() = default;
    schema_registry_client(const schema_registry_client&) = delete;
    schema_registry_client& operator=(const schema_registry_client&) = delete;
    schema_registry_client(schema_registry_client&&) noexcept = delete;
    schema_registry_client& operator=(schema_registry_client&&) noexcept
      = delete;
    virtual ~schema_registry_client() = default;

    /**
     * Look up a schema by its global ID
     */
    [[nodiscard]] virtual std::expected<schema, std::error_code>
    lookup_schema_by_id(schema_id id) const = 0;

    /**
     * Look up a schema by subject and specific version
     */
    [[nodiscard]] virtual std::expected<subject_schema, std::error_code>
    lookup_schema_by_version(
      std::string_view subject, schema_version version) const
      = 0;

    /**
     * Look up the latest version of a schema (by subject)
     *
     * Equivalent to calling lookup_schema_by_version with schema_version{-1}
     */
    [[nodiscard]] virtual std::expected<subject_schema, std::error_code>
    lookup_latest_schema(std::string_view subject) const = 0;

    /**
     * Create a schema under the given subject, returning the version and ID
     *
     * If an equivalent schema already exists globally, that schema ID is
     * reused. If an equivalent schema already exists within that subject, this
     * has no effect and returns the existing schema
     */
    [[nodiscard]] virtual std::expected<subject_schema, std::error_code>
    create_schema(std::string_view subject, schema the_schema) = 0;
};

/**
 * Create a new schema registry client
 */
std::unique_ptr<schema_registry_client> new_client();

/**
 * Extract and decode the schema ID from an arbitrary array of bytes.
 * buf must be at least 5B long:
 *   - buf[0]: magic byte (must be 0x00)
 *   - buf[1..5]: the id, in network byte order (big endian)
 */
std::expected<std::pair<schema_id, bytes_view>, std::error_code>
decode_schema_id(bytes_view buf);

/**
 * Encode and prepend the schema ID to a byte array.
 * Creates and returns a copy of the buffer.
 * The result will be at least 5B long.
 */
bytes encode_schema_id(schema_id sid, bytes_view buf);

} // namespace sr

} // namespace redpanda
