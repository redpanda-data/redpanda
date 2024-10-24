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

#include <redpanda/transform_sdk.h>

#include <algorithm>
#include <bit>
#include <chrono>
#include <climits>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <expected>
#include <ranges>
#include <system_error>
#include <utility>

namespace redpanda {
namespace {

// NOLINTBEGIN(*-macro-usage,*-macro-parentheses)

#define RP_CONCAT(x, y) RP_CONCAT_IMPL(x, y)
#define RP_CONCAT_IMPL(x, y) x##y

#define ASSIGN_OR_RETURN(lhs, rexpr)                                           \
    RP_ASSIGN_OR_RETURN_IMPL(RP_CONCAT(expected_tmp_, __LINE__), lhs, rexpr);

#define RP_ASSIGN_OR_RETURN_IMPL(uniq_var_name, lhs, rexpr)                    \
    auto uniq_var_name = (rexpr);                                              \
    if (!uniq_var_name.has_value()) [[unlikely]] {                             \
        return std::unexpected(uniq_var_name.error());                         \
    }                                                                          \
    lhs = std::move(*uniq_var_name)

// NOLINTEND(*-macro-usage,*-macro-parentheses)

/**
 * A simple println that writes to stderr. We don't use std::println because it
 * doubles the compiled size of our SDK for no good reason.
 */
void println(std::string_view str) {
    std::string msg;
    msg.reserve(str.size() + 1);
    msg.append(str);
    msg.append("\n");
    // We don't use C++23 format/print due to it including <iostreams> which
    // blows up the size of the binary
    // NOLINTNEXTLINE
    std::fwrite(msg.c_str(), sizeof(char), msg.size(), stderr);
    // NOLINTNEXTLINE
    std::fflush(stderr);
}

/**
 * A simple abort function that prints then aborts
 */
[[noreturn]] void abort(std::string_view str) {
    println(str);
    std::abort();
}

namespace abi {
#ifdef __wasi__

extern "C" {
#define WASM_IMPORT(mod, name)                                                 \
    __attribute__((import_module(#mod), import_name(#name)))

WASM_IMPORT(redpanda_transform, check_abi_version_1)
void redpanda_transform_check();
constexpr auto check = redpanda_transform_check;

WASM_IMPORT(redpanda_transform, read_batch_header)
int32_t redpanda_transform_read_batch_header(
  int64_t* base_offset,
  int32_t* record_count,
  int32_t* partition_leader_epoch,
  int16_t* attributes,
  int32_t* last_offset_delta,
  int64_t* base_timestamp,
  int64_t* max_timestamp,
  int64_t* producer_id,
  int16_t* producer_epoch,
  int32_t* base_sequence);
constexpr auto read_batch_header = redpanda_transform_read_batch_header;

WASM_IMPORT(redpanda_transform, read_next_record)
int32_t redpanda_transform_read_next_record(
  uint8_t* attributes,
  int64_t* timestamp,
  int64_t* offset,
  uint8_t* buf,
  uint32_t len);
constexpr auto read_next_record = redpanda_transform_read_next_record;

WASM_IMPORT(redpanda_transform, write_record)
int32_t redpanda_transform_write_record(uint8_t* buf, uint32_t len);
constexpr auto write_record = redpanda_transform_write_record;

/**
 * Schema Registry ABI
 */
namespace sr {

WASM_IMPORT(redpanda_schema_registry, check_abi_version_0)
void redpanda_schema_registry_check();
constexpr auto check = redpanda_schema_registry_check;

WASM_IMPORT(redpanda_schema_registry, get_schema_definition_len)
int32_t redpanda_schema_registry_get_schema_definition_len(
  int32_t schemaId, int32_t* len);
constexpr auto get_schema_definition_len
  = redpanda_schema_registry_get_schema_definition_len;

WASM_IMPORT(redpanda_schema_registry, get_schema_definition)
int32_t redpanda_schema_registry_get_schema_definition(
  int32_t schema_id, uint8_t* buf, int32_t len);
constexpr auto get_schema_definition
  = redpanda_schema_registry_get_schema_definition;

WASM_IMPORT(redpanda_schema_registry, get_subject_schema_len)
int32_t redpanda_schema_registry_get_subject_schema_len(
  const uint8_t* subject, int32_t subject_len, int32_t version, int32_t* len);
constexpr auto get_subject_schema_len
  = redpanda_schema_registry_get_subject_schema_len;

WASM_IMPORT(redpanda_schema_registry, get_subject_schema)
int32_t redpanda_schema_registry_get_subject_schema(
  const uint8_t* subject,
  int32_t subject_len,
  int32_t version,
  uint8_t* buf,
  int32_t len);
constexpr auto get_subject_schema = redpanda_schema_registry_get_subject_schema;

WASM_IMPORT(redpanda_schema_registry, create_subject_schema)
int32_t redpanda_schema_registry_create_subject_schema(
  const uint8_t* subject,
  int32_t subject_len,
  uint8_t* buf,
  int32_t len,
  int32_t* schema_id_out);
constexpr auto create_subject_schema
  = redpanda_schema_registry_create_subject_schema;

} // namespace sr

#undef WASM_IMPORT
}

#else

void check() { abort("check_abi - stub"); }

int32_t read_batch_header(
  int64_t* /*unused*/,
  int32_t* /*unused*/,
  int32_t* /*unused*/,
  int16_t* /*unused*/,
  int32_t* /*unused*/,
  int64_t* /*unused*/,
  int64_t* /*unused*/,
  int64_t* /*unused*/,
  int16_t* /*unused*/,
  int32_t* /*unused*/) {
    abort("read_batch_header - stub");
}

int32_t read_next_record(
  uint8_t* /*unused*/,
  int64_t* /*unused*/,
  int64_t* /*unused*/,
  uint8_t* /*unused*/,
  uint32_t /*unused*/) {
    abort("read_next_record - stub");
}

int32_t write_record(const uint8_t* /*unused*/, uint32_t /*unused*/) {
    abort("write_record - stub");
}

namespace sr {

void check() { abort("sr_check_abi - stub"); }

int32_t get_schema_definition_len(int32_t /*unused*/, int32_t* /*unused*/) {
    abort("get_schema_definition_len - stub");
}

int32_t get_schema_definition(
  int32_t /*unused*/, uint8_t* /*unused*/, int32_t /*unused*/) {
    abort("get_schema_definition - stub");
}

int32_t get_subject_schema_len(
  const uint8_t* /*unused*/,
  int32_t /*unused*/,
  int32_t /*unused*/,
  int32_t* /*unused*/) {
    abort("get_subject_schema_len - stub");
}

int32_t get_subject_schema(
  const uint8_t* /*unused*/,
  int32_t /*unused*/,
  int32_t /*unused*/,
  uint8_t* /*unused*/,
  int32_t /*unused*/) {
    abort("get_subject_schema - stub");
}

int32_t create_subject_schema(
  const uint8_t* /*unused*/,
  int32_t /*unused*/,
  uint8_t* /*unused*/,
  int32_t /*unused*/,
  int32_t* /*unused*/) {
    abort("create_subject_schema - stub");
}
} // namespace sr

#endif
} // namespace abi

namespace varint {

constexpr size_t MAX_LENGTH = 10;

template<typename T>
struct decoded {
    T value;
    size_t read;
};

uint64_t zigzag_encode(int64_t num) {
    constexpr unsigned int signbit_shift = (sizeof(int64_t) * CHAR_BIT) - 1;
    // NOLINTNEXTLINE
    return (static_cast<uint64_t>(num) << 1) ^ (num >> signbit_shift);
}

int64_t zigzag_decode(uint64_t num) {
    // NOLINTNEXTLINE
    return static_cast<int64_t>(num >> 1) ^ -static_cast<int64_t>(num & 1);
}

std::expected<decoded<uint64_t>, std::error_code>
read_unsigned(bytes_view payload) {
    uint64_t value = 0;
    unsigned int shift = 0;
    for (size_t i = 0; i < payload.size(); ++i) {
        const uint8_t byte = payload[i];
        if (i >= MAX_LENGTH) {
            return std::unexpected(
              std::make_error_code(std::errc::value_too_large));
        }
        constexpr unsigned int unsigned_bits = 0x7F;
        value |= static_cast<uint64_t>(byte & unsigned_bits) << shift;
        constexpr unsigned int sign_bit = 0x80;
        if ((byte & sign_bit) == 0) {
            return {{
              .value = value,
              .read = i + 1,
            }};
        }
        shift += CHAR_BIT - 1;
    }
    return std::unexpected(
      std::make_error_code(std::errc::illegal_byte_sequence));
}

std::expected<decoded<int64_t>, std::error_code> read(bytes_view payload) {
    ASSIGN_OR_RETURN(const decoded<uint64_t> dec, read_unsigned(payload));
    return {{
      .value = zigzag_decode(dec.value),
      .read = dec.read,
    }};
}

std::expected<decoded<std::optional<bytes_view>>, std::error_code>
read_sized_buffer(bytes_view payload) {
    ASSIGN_OR_RETURN(const decoded<int64_t> result, read(payload));
    if (result.value < 0) {
        return {{.value = std::nullopt, .read = result.read}};
    }
    payload = payload.subview(result.read);
    auto buf_size = static_cast<size_t>(result.value);
    if (buf_size > payload.size()) [[unlikely]] {
        return std::unexpected(
          std::make_error_code(std::errc::illegal_byte_sequence));
    }
    return {{
      .value = payload.subview(0, buf_size),
      .read = result.read + buf_size,
    }};
}

void write_unsigned(bytes* payload, uint64_t val) {
    constexpr unsigned int msb = 0x80;
    while (val >= msb) {
        auto byte = static_cast<uint8_t>(val) | msb;
        val >>= CHAR_BIT - 1;
        payload->push_back(static_cast<char>(byte));
    }
    constexpr unsigned int byte_mask = 0xFF;
    payload->push_back(static_cast<char>(val & byte_mask));
}

void write(bytes* payload, int64_t val) {
    write_unsigned(payload, zigzag_encode(val));
}

template<typename B>
void write_sized_buffer(bytes* payload, const std::optional<B>& buf) {
    if (!buf.has_value()) {
        write(payload, -1);
        return;
    }
    write(payload, static_cast<int64_t>(buf->size()));
    payload->append_range(buf.value());
}

} // namespace varint

namespace decode {

using kv_pair = std::pair<std::optional<bytes_view>, std::optional<bytes_view>>;

std::expected<varint::decoded<kv_pair>, std::error_code>
read_kv(bytes_view payload) {
    ASSIGN_OR_RETURN(auto key, varint::read_sized_buffer(payload));
    payload = payload.subview(key.read);
    ASSIGN_OR_RETURN(auto value, varint::read_sized_buffer(payload));
    return {{
      .value = std::make_pair(key.value, value.value),
      .read = key.read + value.read,
    }};
}

std::expected<record_view, std::error_code>
read_record_view(bytes_view payload) {
    ASSIGN_OR_RETURN(auto kv_result, read_kv(payload));
    payload = payload.subview(kv_result.read);
    ASSIGN_OR_RETURN(auto header_count_result, varint::read(payload));
    payload = payload.subview(header_count_result.read);
    std::vector<header_view> headers;
    headers.reserve(header_count_result.value);
    for (int64_t i = 0; i < header_count_result.value; ++i) {
        ASSIGN_OR_RETURN(auto kv_result, read_kv(payload));
        payload = payload.subview(kv_result.read);
        auto [key_opt, value] = kv_result.value;
        const std::string_view key = key_opt
                                       .transform([](bytes_view buf) {
                                           return std::string_view{buf};
                                       })
                                       .value_or(std::string_view{});
        headers.emplace_back(key, value);
    }
    auto [key, value] = kv_result.value;
    return {{
      .key = key,
      .value = value,
      .headers = std::move(headers),
    }};
}

void write_record(bytes* payload, const redpanda::record_view& rec) {
    varint::write_sized_buffer(payload, rec.key);
    varint::write_sized_buffer(payload, rec.value);
    varint::write(payload, static_cast<int64_t>(rec.headers.size()));
    for (const auto& header : rec.headers) {
        varint::write_sized_buffer(payload, std::make_optional(header.key));
        varint::write_sized_buffer(payload, header.value);
    }
}

struct batch_header {
    int64_t base_offset;
    int32_t record_count;
    int32_t partition_leader_epoch;
    int16_t attributes;
    int32_t last_offset_delta;
    int64_t base_timestamp;
    int64_t max_timestamp;
    int64_t producer_id;
    int16_t producer_epoch;
    int32_t base_sequence;
};

std::expected<sr::schema, std::error_code>
read_schema_def(redpanda::bytes_view payload) {
    ASSIGN_OR_RETURN(auto fmt_d, varint::read(payload));
    ASSIGN_OR_RETURN(
      auto format,
      ([&fmt_d]() -> std::expected<sr::schema_format, std::error_code> {
          switch (fmt_d.value) {
          case 0:
              return sr::schema_format::avro;
          case 1:
              return sr::schema_format::protobuf;
          case 2:
              return sr::schema_format::json;
          default:
              return std::unexpected(
                std::make_error_code(std::errc::illegal_byte_sequence));
          }
      }()));

    payload = payload.subview(fmt_d.read);
    ASSIGN_OR_RETURN(auto schema_d, varint::read_sized_buffer(payload));
    payload = payload.subview(schema_d.read);

    const std::string_view schema = schema_d.value
                                      .transform([](bytes_view buf) {
                                          return std::string_view{buf};
                                      })
                                      .value_or(std::string_view{});

    ASSIGN_OR_RETURN(auto refs_d, varint::read(payload));
    payload = payload.subview(refs_d.read);

    std::vector<sr::reference> refs;
    refs.reserve(refs_d.value);

    for ([[maybe_unused]] auto idx : std::views::iota(0, refs_d.value)) {
        ASSIGN_OR_RETURN(auto name_d, varint::read_sized_buffer(payload));
        payload = payload.subview(name_d.read);
        const std::string_view name = name_d.value
                                        .transform([](bytes_view buf) {
                                            return std::string_view{buf};
                                        })
                                        .value_or(std::string_view{});
        ASSIGN_OR_RETURN(auto subject_d, varint::read_sized_buffer(payload));
        payload = payload.subview(subject_d.read);
        const std::string_view subject = subject_d.value
                                           .transform([](bytes_view buf) {
                                               return std::string_view{buf};
                                           })
                                           .value_or(std::string_view{});
        ASSIGN_OR_RETURN(auto version_d, varint::read(payload));
        payload = payload.subview(version_d.read);
        refs.emplace_back(
          std::string{name.data(), name.size()},
          std::string{subject.data(), subject.size()},
          sr::schema_version{static_cast<int32_t>(version_d.value)});
    };

    return sr::schema{
      std::string{schema.data(), schema.size()}, format, std::move(refs)};
}

std::expected<sr::subject_schema, std::error_code>
read_schema(std::string_view subject, redpanda::bytes_view payload) {
    ASSIGN_OR_RETURN(auto id_d, varint::read(payload));
    payload = payload.subview(id_d.read);
    ASSIGN_OR_RETURN(auto version_d, varint::read(payload));
    payload = payload.subview(version_d.read);
    ASSIGN_OR_RETURN(auto schema, read_schema_def(payload));
    return sr::subject_schema{
      std::move(schema),
      std::string{subject},
      sr::schema_version{static_cast<int32_t>(version_d.value)},
      sr::schema_id{static_cast<int32_t>(id_d.value)}};
}

void write_schema_def(bytes* payload, const sr::schema& schema) {
    varint::write(payload, static_cast<int64_t>(schema.format));
    varint::write_sized_buffer(
      payload, std::make_optional<bytes_view>(schema.raw_schema));
    varint::write(payload, static_cast<int64_t>(schema.references.size()));
    for (const auto& ref : schema.references) {
        varint::write_sized_buffer(
          payload, std::make_optional<bytes_view>(ref.name));
        varint::write_sized_buffer(
          payload, std::make_optional<bytes_view>(ref.subject));
        varint::write(payload, static_cast<int64_t>(ref.version));
    }
}

} // namespace decode

class abi_record_writer : public record_writer {
public:
    std::error_code write(record_view record) final {
        _output_buffer.clear();
        decode::write_record(&_output_buffer, record);
        const int32_t errno_or_amt = abi::write_record(
          _output_buffer.data(), _output_buffer.size());
        if (errno_or_amt != _output_buffer.size()) [[unlikely]] {
            return std::make_error_code(std::errc::io_error);
        }
        return {};
    }

private:
    bytes _output_buffer;
};

void process_batch(const on_record_written_callback& callback) {
    decode::batch_header header{};
    const int32_t errno_or_buf_size = abi::read_batch_header(
      &header.base_offset,
      &header.record_count,
      &header.partition_leader_epoch,
      &header.attributes,
      &header.last_offset_delta,
      &header.base_timestamp,
      &header.max_timestamp,
      &header.producer_id,
      &header.producer_epoch,
      &header.base_sequence);
    if (errno_or_buf_size < 0) [[unlikely]] {
        abort(
          "failed to read batch header (errno: "
          + std::to_string(errno_or_buf_size) + ")");
    }
    const size_t buf_size = errno_or_buf_size;

    bytes input_buffer;
    input_buffer.resize(buf_size, 0);
    abi_record_writer writer;
    for (int32_t i = 0; i < header.record_count; ++i) {
        uint8_t raw_attr = 0;
        int64_t raw_timestamp = 0;
        int64_t raw_offset = 0;
        const int32_t errno_or_amt = abi::read_next_record(
          &raw_attr,
          &raw_timestamp,
          &raw_offset,
          input_buffer.data(),
          input_buffer.size());
        if (errno_or_amt < 0) [[unlikely]] {
            abort(
              "reading record failed (errno: " + std::to_string(errno_or_amt)
              + ", buffer_size: " + std::to_string(buf_size) + ")");
        }
        const size_t amt = errno_or_amt;
        const std::chrono::system_clock::time_point timestamp{
          std::chrono::milliseconds{raw_timestamp}};
        auto record = decode::read_record_view({input_buffer.begin(), amt});
        if (!record.has_value()) [[unlikely]] {
            abort("deserializing record failed: " + record.error().message());
        }
        written_record written = {
          .key = record.value().key,
          .value = record.value().value,
          .headers = std::move(record.value().headers),
          .timestamp = timestamp};
        const std::error_code err = callback(
          write_event{.record = std::move(written)}, &writer);
        if (err) [[unlikely]] {
            abort("transforming record failed: " + err.message());
        }
    }
}

} // namespace

bytes_view::bytes_view(const bytes& buf)
  : bytes_view(buf.begin(), buf.end()) {}

bytes_view::bytes_view(const std::string& str)
  : bytes_view(std::string_view{str}) {}

bytes_view::bytes_view(std::string_view str)
  // NOLINTNEXTLINE(*-reinterpret-cast)
  : bytes_view(reinterpret_cast<const uint8_t*>(str.data()), str.size()) {}

bytes_view::bytes_view(bytes::const_pointer start, size_t size)
  : bytes_view(start, start + static_cast<std::ptrdiff_t>(size)) {}

bytes_view::bytes_view(bytes::const_iterator start, size_t size)
  : bytes_view(start, start + static_cast<std::ptrdiff_t>(size)) {}

bytes_view::bytes_view(bytes::const_iterator start, bytes::const_iterator end)
  : bytes_view(std::to_address(start), std::to_address(end)) {}

bytes_view::bytes_view(bytes::const_pointer start, bytes::const_pointer end)
  : _start(start)
  , _end(end) {}

bytes::const_pointer bytes_view::begin() const { return _start; }

bytes::const_pointer bytes_view::end() const { return _end; }

bytes_view bytes_view::subview(size_t offset, size_t count) const {
    if (count == std::dynamic_extent) {
        return {_start + offset, _end};
    }
    return {_start + offset, _start + offset + count};
}

bytes::value_type bytes_view::operator[](size_t n) const { return _start[n]; }

bool bytes_view::operator==(const bytes_view& other) const {
    return std::ranges::equal(*this, other);
}
bytes_view::operator std::string_view() const {
    // NOLINTNEXTLINE(*-reinterpret-cast)
    return {reinterpret_cast<const char*>(data()), size()};
}

header::operator header_view() const {
    return header_view{.key = key, .value = value};
}

record::operator record_view() const {
    std::vector<header_view> view_headers;
    view_headers.reserve(headers.size());
    for (const header& header : headers) {
        view_headers.push_back(header);
    }
    return record_view{
      .key = key,
      .value = value,
      .headers = std::move(view_headers),
    };
}

written_record::operator record_view() const {
    return record_view{
      .key = key,
      .value = value,
      .headers = headers,
    };
}

[[noreturn]] void
on_record_written(const on_record_written_callback& callback) {
    abi::check();
    while (true) {
        process_batch(callback);
    }
}

namespace sr {

schema
schema::new_avro(std::string schema, std::optional<reference_container> refs) {
    return {
      std::move(schema),
      schema_format::avro,
      std::move(refs).value_or(reference_container{})};
}

schema schema::new_protobuf(
  std::string schema, std::optional<reference_container> refs) {
    return {
      std::move(schema),
      schema_format::protobuf,
      std::move(refs).value_or(reference_container{})};
}

schema
schema::new_json(std::string schema, std::optional<reference_container> refs) {
    return {
      std::move(schema),
      schema_format::json,
      std::move(refs).value_or(reference_container{})};
}

namespace {

// NOLINTBEGIN(*-enum-size)
enum class errc : int32_t {
    success = 0,
    not_enabled = -1,
    schema_registry_error = -2,
};
// NOLINTEND(*-enum-size)

struct errc_category final : public std::error_category {
    [[nodiscard]] const char* name() const noexcept final {
        return "redpanda::sr::errc";
    }

    [[nodiscard]] std::string message(int code) const final {
        switch (static_cast<errc>(code)) {
        case errc::success:
            return "redpanda::sr: Success";
        case errc::not_enabled:
            return "redpanda::sr: Schema Registry is not enabled";
        case errc::schema_registry_error:
            return "redpanda::sr: Schema Registry internal error";
        }
        return "redpanda::sr: Unknown error";
    }
};

inline const std::error_category& error_category() noexcept {
    static const errc_category cat;
    return cat;
}

inline std::error_code make_error_code(errc code) noexcept {
    return {static_cast<int>(code), error_category()};
}
} // namespace

class abi_schema_registry_client final : public schema_registry_client {
public:
    abi_schema_registry_client() { abi::sr::check(); }

    [[nodiscard]] std::expected<schema, std::error_code>
    lookup_schema_by_id(schema_id sid) const final {
        int32_t length = 0;
        auto errc = abi::sr::get_schema_definition_len(sid.value(), &length);
        if (errc != 0) {
            return std::unexpected{
              sr::make_error_code(static_cast<sr::errc>(errc))};
        }
        redpanda::bytes buf;
        buf.resize(length, 0);
        auto ec_or_n = abi::sr::get_schema_definition(
          sid.value(), buf.data(), static_cast<int32_t>(buf.size()));
        if (ec_or_n < 0) {
            return std::unexpected{
              sr::make_error_code(static_cast<sr::errc>(ec_or_n))};
        }
        return decode::read_schema_def(
          redpanda::bytes_view{buf.data(), static_cast<size_t>(ec_or_n)});
    }

    [[nodiscard]] std::expected<subject_schema, std::error_code>
    lookup_schema_by_version(
      std::string_view subject, schema_version version) const final {
        int32_t length = 0;
        auto errc = abi::sr::get_subject_schema_len(
          bytes_view{subject}.begin(),
          static_cast<int32_t>(subject.size()),
          version.value(),
          &length);
        if (errc != 0) {
            return std::unexpected{
              sr::make_error_code(static_cast<sr::errc>(errc))};
        }
        redpanda::bytes buf;
        buf.resize(length, 0);
        auto ec_or_n = abi::sr::get_subject_schema(
          bytes_view{subject}.begin(),
          static_cast<int32_t>(subject.size()),
          version.value(),
          buf.data(),
          static_cast<int32_t>(buf.size()));
        if (ec_or_n < 0) {
            return std::unexpected{
              sr::make_error_code(static_cast<sr::errc>(ec_or_n))};
        }
        return decode::read_schema(
          subject, bytes_view{buf.data(), static_cast<size_t>(ec_or_n)});
    }

    [[nodiscard]] std::expected<subject_schema, std::error_code>
    lookup_latest_schema(std::string_view subject) const final {
        return lookup_schema_by_version(subject, schema_version{-1});
    }

    [[nodiscard]] std::expected<subject_schema, std::error_code>
    create_schema(std::string_view subject, sr::schema schema) final {
        bytes buf;
        buf.reserve(schema.raw_schema.size() + varint::MAX_LENGTH);
        decode::write_schema_def(&buf, schema);
        sr::schema_id sid{0};
        // NOTE: A planned ABI update will add a `version` output parameter to
        // create_subject_schema
        const sr::schema_version version{0};
        auto errc = abi::sr::create_subject_schema(
          bytes_view{subject}.begin(),
          static_cast<int32_t>(subject.size()),
          buf.data(),
          static_cast<int32_t>(buf.size()),
          sid.data());
        if (errc != 0) {
            return std::unexpected{
              sr::make_error_code(static_cast<sr::errc>(errc))};
        }
        return sr::subject_schema{
          std::move(schema), std::string{subject}, version, sid};
    };
};

std::unique_ptr<schema_registry_client> new_client() {
    return std::make_unique<abi_schema_registry_client>();
}

std::expected<std::pair<schema_id, bytes_view>, std::error_code>
decode_schema_id(bytes_view buf) {
    constexpr size_t HEADER_LEN = 5U;
    static const bytes MAGIC_BYTES{0x00};
    if (
      buf.subview(0, MAGIC_BYTES.size()) != MAGIC_BYTES
      || buf.size() < HEADER_LEN) {
        return std::unexpected{
          std::make_error_code(std::errc::illegal_byte_sequence)};
    }
    // NOLINTBEGIN(*-magic-numbers)
    auto id_bytes = buf.subview(1, HEADER_LEN - 1);
    const uint32_t raw_id = (static_cast<uint32_t>(id_bytes[3]) << 0U)
                            | (static_cast<uint32_t>(id_bytes[2]) << 8U)
                            | (static_cast<uint32_t>(id_bytes[1]) << 16U)
                            | (static_cast<uint32_t>(id_bytes[0]) << 24U);
    // NOLINTEND(*-magic-numbers)
    if (
      raw_id > static_cast<uint32_t>(
        std::numeric_limits<sr::schema_id::type>::max())) {
        return std::unexpected{
          std::make_error_code(std::errc::illegal_byte_sequence)};
    }
    return std::make_pair(
      schema_id{static_cast<int32_t>(raw_id)}, buf.subview(HEADER_LEN));
}

bytes encode_schema_id(schema_id sid, bytes_view buf) {
    static const bytes MAGIC_BYTES{0x00};
    bytes be_id;
    be_id.resize(sizeof(schema_id::type));
    auto reversed = std::byteswap(sid.value());
    std::memcpy(be_id.data(), &reversed, be_id.size());
    bytes result = MAGIC_BYTES;
    result.append_range(be_id);
    result.append_range(buf);
    return result;
}

} // namespace sr

} // namespace redpanda

namespace std {
template<>
struct is_error_code_enum<redpanda::sr::errc> : true_type {};
} // namespace std

#ifdef REDPANDA_TRANSFORM_SDK_ENABLE_TESTING

#include <algorithm>
#include <format>
#include <print>
#include <random>

using random_bytes_engine
  = std::independent_bits_engine<std::default_random_engine, CHAR_BIT, uint8_t>;

namespace redpanda {

bytes make_bytes(random_bytes_engine* rng, size_t length) {
    bytes dat(length, '\0');
    std::generate(dat.begin(), dat.end(), *rng);
    return dat;
}

std::string make_string(random_bytes_engine* rng, size_t length) {
    std::string dat(length, '\0');
    std::generate(dat.begin(), dat.end(), *rng);
    return dat;
}

template<class... Args>
void assert(bool cond, std::format_string<Args...> fmt, Args&&... args) {
    if (!cond) {
        abort(std::format(fmt, std::forward<Args...>(args)...));
    }
}

// Ignore this lint check, as it can't tell our custom `assert` method
// is a check.
// NOLINTBEGIN(*-unchecked-optional-access)

void test_zigzag_roundtrip() {
    constexpr auto testcases = std::to_array<int64_t>(
      {0,
       1,
       -1,
       42,
       -42,
       -std::numeric_limits<int64_t>::max(),
       std::numeric_limits<int64_t>::min(),
       std::numeric_limits<int64_t>::max()});
    for (int64_t val : testcases) {
        auto encoded = varint::zigzag_encode(val);
        auto decoded = varint::zigzag_decode(encoded);
        assert(decoded == val, "zigzag roundtrip failed: {}", val);
    }
}

void test_varint_roundtrip() {
    constexpr auto testcases = std::to_array<int64_t>(
      {0,
       1,
       -1,
       42,
       -42,
       -std::numeric_limits<int64_t>::max(),
       std::numeric_limits<int64_t>::min(),
       std::numeric_limits<int64_t>::max()});
    for (const int64_t val : testcases) {
        bytes encoded;
        varint::write(&encoded, val);
        auto decoded = varint::read(encoded);
        assert(decoded.has_value(), "varint read failed");
        assert(
          decoded.value().read == encoded.size(),
          "varint roundtrip failed: {}",
          val);
        assert(
          decoded.value().value == val, "varint roundtrip failed: {}", val);
    }
}

void test_null_buffer_roundtrip() {
    bytes encoded;
    varint::write_sized_buffer<bytes>(&encoded, std::nullopt);
    auto decoded = varint::read_sized_buffer(encoded);
    assert(decoded.has_value(), "varint read failed");
    assert(decoded.value().read == encoded.size(), "nullbuf roundtrip failed");
    assert(!decoded.value().value, "nullbuf roundtrip failed");
}

void test_sized_buffer_roundtrip(random_bytes_engine* rng) {
    for (const size_t length : {0, 1, 9, 42, 64, 1024}) {
        bytes original = make_bytes(rng, length);
        bytes encoded;
        varint::write_sized_buffer<bytes>(&encoded, original);
        auto decoded = varint::read_sized_buffer(encoded);
        assert(decoded.has_value(), "varint read failed");
        assert(
          decoded.value().read == encoded.size(),
          "varbuf roundtrip failed: {}",
          original.size());
        assert(
          decoded.value().value.has_value(),
          "varbuf roundtrip failed: {}",
          original.size());
        assert(
          std::ranges::equal(decoded.value().value.value(), original),
          "varbuf roundtrip failed: {}",
          original.size());
    }
}

void test_record_roundtrip(random_bytes_engine* rng) {
    constexpr size_t small = 42;
    constexpr size_t big = 256;
    const record empty{
      .key = std::nullopt,
      .value = std::nullopt,
      .headers = {},
    };
    const record key_only{
      .key = make_bytes(rng, small),
      .value = std::nullopt,
      .headers = {},
    };
    const record value_only{
      .key = std::nullopt,
      .value = make_bytes(rng, small),
      .headers = {},
    };
    const record headers_only{
      .key = std::nullopt,
      .value = std::nullopt,
      .headers = {
        {.key = make_string(rng, small), .value = make_bytes(rng, small)},
        {.key = make_string(rng, small), .value = make_bytes(rng, big)},
        {.key = make_string(rng, small), .value = std::nullopt}}};
    const record keyval{
      .key = make_bytes(rng, small),
      .value = make_bytes(rng, big),
      .headers = {},
    };
    const record full{
      .key = make_bytes(rng, small),
      .value = make_bytes(rng, big),
      .headers = {
        {.key = make_string(rng, small), .value = make_bytes(rng, small)},
        {.key = make_string(rng, small), .value = make_bytes(rng, big)},
        {.key = make_string(rng, small), .value = std::nullopt}}};
    for (const record& rec :
         {empty, key_only, value_only, headers_only, keyval, full}) {
        bytes encoded;
        decode::write_record(&encoded, rec);
        auto result = decode::read_record_view(encoded);
        assert(result.has_value(), "expected value");
        assert(result.value() == rec, "record mismatch");
    }
}

void test_schema_def_roundtrip(random_bytes_engine* rng) {
    constexpr size_t small = 42;
    constexpr size_t big = 1028;
    const auto schema_no_refs = sr::schema::new_avro(make_string(rng, big));
    const auto schema_with_refs = sr::schema::new_avro(
      make_string(rng, big),
      sr::schema::reference_container{
        sr::reference{
          .name = make_string(rng, small),
          .subject = make_string(rng, small),
          .version = sr::schema_version{1},
        },
        sr::reference{
          .name = make_string(rng, small),
          .subject = make_string(rng, small),
          .version = sr::schema_version{1},
        },
      });
    for (const sr::schema& schema : {schema_no_refs, schema_with_refs}) {
        bytes encoded;
        decode::write_schema_def(&encoded, schema);
        auto result = decode::read_schema_def(encoded);
        assert(result.has_value(), "expected value");
        assert(result.value() == schema, "schema mismatch");
    }
}

void test_schema_id_codec_roundtrip(random_bytes_engine* rng) {
    constexpr size_t len = 1024;
    constexpr size_t tiny = 4;
    static const sr::schema_id sid{
      std::numeric_limits<sr::schema_id::type>::max()};
    auto buf = make_bytes(rng, len);
    auto encoded = sr::encode_schema_id(sid, buf);
    auto result = sr::decode_schema_id(encoded);
    assert(result.has_value(), "decode failed");
    auto [decoded, rest] = result.value();
    assert(decoded == sid, "schema id mismatch");
    assert(rest == buf, "buffer mismatch");

    buf = make_bytes(rng, tiny);
    result = sr::decode_schema_id(buf);
    assert(!result.has_value(), "decode should fail on too small buffer");

    // NOLINTNEXTLINE(*-magic-numbers)
    buf = bytes{0x00, 0xFF, 0xFF, 0xFF, 0xFF};
    result = sr::decode_schema_id(buf);
    assert(!result.has_value(), "decode should fail if ID exceeds INT_MAX");

    // NOLINTNEXTLINE(*-magic-numbers)
    buf = bytes{0x01, 0x00, 0x00, 0x00, 0x01};
    result = sr::decode_schema_id(buf);
    assert(
      !result.has_value(), "decode should fail if magic byte is incorrect");
}

// NOLINTEND(*-unchecked-optional-access)

void run_test_suite() {
    unsigned seed = std::random_device()();
    std::println("using seed: {}", seed);
    random_bytes_engine rng(seed);
    test_zigzag_roundtrip();
    test_varint_roundtrip();
    test_null_buffer_roundtrip();
    test_sized_buffer_roundtrip(&rng);
    test_record_roundtrip(&rng);
    test_schema_def_roundtrip(&rng);
    test_schema_id_codec_roundtrip(&rng);
    std::println("tests successful");
}

} // namespace redpanda

int main() {
    redpanda::run_test_suite();
    return 0;
}

#endif
