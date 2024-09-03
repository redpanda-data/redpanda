// Copyright 2024 Redpanda Data, Inc.
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

#include "js_vm.h"

#include <redpanda/transform_sdk.h>

#include <cstdlib>
#include <cstring>
#include <expected>
#include <memory>
#include <optional>
#include <print>
#include <quickjs.h>
#include <utility>
#include <variant>
#include <vector>

namespace redpanda::js {

extern "C" {

// The following functions are how the user's code is injected into the Wasm
// binary. We leave this imported functions, and RPK will inject these symbols
// in `rpk transform build` after esbuild runs.

#ifdef __wasi__

#define WASM_IMPORT(mod, name)                                                 \
    __attribute__((import_module(#mod), import_name(#name)))

WASM_IMPORT(redpanda_js_provider, file_length)
uint32_t redpanda_js_source_file_length();

WASM_IMPORT(redpanda_js_provider, get_file)
void redpanda_js_source_get_file(char* dst);

#else

constexpr std::string_view test_source_file = R"(
import {onRecordWritten} from "@redpanda-data/transform-sdk";

onRecordWritten((event, writer) => {
  return writer.write(event.record);
});
)";

uint32_t redpanda_js_source_file_length() { return test_source_file.size(); }

void redpanda_js_source_get_file(char* dst) {
    std::memcpy(dst, test_source_file.data(), test_source_file.size());
}

#endif
}

/**
 * A custom JS class holding opaque bytes, easily convertable into common JS
 * types.
 */
class record_data {
public:
    explicit record_data(bytes_view data)
      : _data(data) {}
    record_data(const record_data&) = delete;
    record_data& operator=(const record_data&) = delete;
    record_data(record_data&&) = default;
    record_data& operator=(record_data&&) = default;

    std::expected<qjs::value, qjs::exception>
    text(JSContext* ctx, std::span<qjs::value> /*params*/) {
        return qjs::value::string(ctx, std::string_view{_data});
    }

    std::expected<qjs::value, qjs::exception>
    json(JSContext* ctx, std::span<qjs::value> /*params*/) {
        // TODO(rockwood): This is going to be the most common case, this needs
        // to be zero copy.
        std::string str;
        str.append_range(_data);
        return qjs::value::parse_json(ctx, str);
    }

    std::expected<qjs::value, qjs::exception>
    array(JSContext* ctx, std::span<qjs::value> /*params*/) {
        const std::span data_view = {// NOLINTNEXTLINE(*-const-cast)
                                     const_cast<uint8_t*>(_data.data()),
                                     _data.size()};
        auto array = qjs::value::uint8_array(ctx, data_view);
        // This memory isn't copied so we need to make sure we
        // invalid these arrays when the memory is gone.
        _arrays.push_back(array);
        return array;
    }

    ~record_data() {
        for (qjs::value array : _arrays) {
            std::ignore = array.detach_uint8_array();
        }
    }

    [[nodiscard]] bytes_view data() const { return _data; }

private:
    bytes_view _data;
    std::vector<qjs::value> _arrays;
};

/**
 * A JS class representation of redpanda::record_writer
 */
class record_writer {
public:
    explicit record_writer(
      redpanda::record_writer* writer,
      qjs::class_factory<record_data>* record_data)
      : _writer(writer)
      , _record_data(record_data) {}

    std::expected<qjs::value, qjs::exception>
    write(JSContext* ctx, std::span<qjs::value> params) {
        if (params.size() != 1) [[unlikely]] {
            return std::unexpected(qjs::exception::make(
              ctx,
              std::format(
                "invalid number of parameters to writer.write, got: {}, "
                "expected: 1",
                params.size())));
        }
        auto& param = params.front();
        if (!param.is_object()) [[unlikely]] {
            return std::unexpected(qjs::exception::make(
              ctx, "expected only object parameters to writer.write"));
        }
        auto key = extract_data(ctx, param.get_property("key"));
        if (!key.has_value()) [[unlikely]] {
            return std::unexpected(key.error());
        }
        auto value = extract_data(ctx, param.get_property("value"));
        if (!value.has_value()) [[unlikely]] {
            return std::unexpected(value.error());
        }
        auto headers = extract_headers(ctx, param.get_property("headers"));
        if (!headers.has_value()) [[unlikely]] {
            return std::unexpected(headers.error());
        }
        auto errc = _writer->write({
          .key = *key,
          .value = *value,
          .headers = *headers,
        });
        _strings.clear(); // free any allocated strings
        if (errc) [[unlikely]] {
            return std::unexpected(qjs::exception::make(
              ctx, std::format("error writing record: {}", errc.message())));
        }
        return qjs::value::undefined(ctx);
    }

    std::expected<std::vector<redpanda::header_view>, qjs::exception>
    extract_headers(JSContext* ctx, const qjs::value& val) {
        std::vector<redpanda::header_view> headers;
        if (val.is_undefined() || val.is_null()) {
            return headers;
        }
        if (!val.is_array()) {
            return std::unexpected(
              qjs::exception::make(ctx, "unexpected type for headers"));
        }
        const size_t len = val.array_length();
        headers.reserve(len);
        for (size_t i = 0; i < len; ++i) {
            auto elem = val.get_element(i);
            if (!elem.is_object()) [[unlikely]] {
                return std::unexpected(qjs::exception::make(
                  ctx, "expected only objects as headers"));
            }
            auto key = extract_data(ctx, elem.get_property("key"));
            if (!key.has_value()) [[unlikely]] {
                return std::unexpected(key.error());
            }
            auto value = extract_data(ctx, elem.get_property("value"));
            if (!value.has_value()) [[unlikely]] {
                return std::unexpected(value.error());
            }
            auto key_str = std::string_view{
              key->value_or(redpanda::bytes_view{})};
            headers.emplace_back(key_str, *value);
        }
        return headers;
    }

    std::expected<std::optional<redpanda::bytes_view>, qjs::exception>
    extract_data(JSContext* ctx, const qjs::value& val) {
        if (val.is_string()) {
            _strings.emplace_back(val.string_data());
            const auto& data = _strings.back();
            return redpanda::bytes_view(data.view());
        }
        if (val.is_uint8_array()) {
            auto data = val.uint8_array_data();
            if (data.data() == nullptr) [[unlikely]] {
                return std::unexpected(qjs::exception::current(ctx));
            }
            return redpanda::bytes_view(data.data(), data.size());
        }
        if (val.is_array_buffer()) {
            auto data = val.array_buffer_data();
            if (data.data() == nullptr) [[unlikely]] {
                return std::unexpected(qjs::exception::current(ctx));
            }
            return redpanda::bytes_view(data.data(), data.size());
        }
        if (val.is_null() || val.is_undefined()) {
            return std::nullopt;
        }
        record_data* record_data = _record_data->get_opaque(val);
        if (record_data == nullptr) [[unlikely]] {
            return std::unexpected(
              qjs::exception::make(ctx, "unexpected type for record data"));
        }
        return record_data->data();
    }

private:
    redpanda::record_writer* _writer;
    qjs::class_factory<record_data>* _record_data;
    std::vector<qjs::cstring> _strings; // a place to temporarily hold data.
};

qjs::class_factory<record_writer>
make_record_writer_class(qjs::runtime* runtime) {
    qjs::class_builder<record_writer> builder(
      runtime->context(), "RecordWriter");
    builder.method<&record_writer::write>("write");
    return builder.build();
}

qjs::class_factory<record_data> make_record_data_class(qjs::runtime* runtime) {
    qjs::class_builder<record_data> builder(runtime->context(), "RecordData");
    builder.method<&record_data::json>("json");
    builder.method<&record_data::text>("text");
    builder.method<&record_data::array>("array");
    return builder.build();
}

std::expected<qjs::value, qjs::exception> make_write_event(
  JSContext* ctx,
  const write_event& evt,
  qjs::class_factory<record_data>* data_factory) {
    auto make_kv = [ctx, data_factory](
                     std::optional<redpanda::bytes_view> key,
                     std::optional<redpanda::bytes_view> val)
      -> std::expected<qjs::value, qjs::exception> {
        qjs::value obj = qjs::value::object(ctx);
        std::expected<std::monostate, qjs::exception> result;
        if (key) {
            result = obj.set_property(
              "key", data_factory->create(std::make_unique<record_data>(*key)));
        } else {
            result = obj.set_property("key", qjs::value::null(ctx));
        }
        if (!result.has_value()) [[unlikely]] {
            return std::unexpected(result.error());
        }
        if (val) {
            result = obj.set_property(
              "value",
              data_factory->create(std::make_unique<record_data>(*val)));
        } else {
            result = obj.set_property("value", qjs::value::null(ctx));
        }
        if (!result.has_value()) [[unlikely]] {
            return std::unexpected(result.error());
        }
        return obj;
    };
    auto maybe_record = make_kv(evt.record.key, evt.record.value);
    if (!maybe_record.has_value()) [[unlikely]] {
        return std::unexpected(maybe_record.error());
    }
    auto record = maybe_record.value();
    auto headers = qjs::value::array(ctx);
    for (const auto& header : evt.record.headers) {
        auto maybe_header = make_kv(
          redpanda::bytes_view(header.key), header.value);
        if (!maybe_header.has_value()) [[unlikely]] {
            return std::unexpected(maybe_header.error());
        }
        auto result = headers.push_back(*maybe_header);
        if (!result.has_value()) [[unlikely]] {
            return std::unexpected(result.error());
        }
    }
    auto result = record.set_property("headers", headers);
    if (!result.has_value()) [[unlikely]] {
        return std::unexpected(result.error());
    }
    qjs::value write_event = qjs::value::object(ctx);
    result = write_event.set_property("record", record);
    if (!result.has_value()) [[unlikely]] {
        return std::unexpected(result.error());
    }
    return write_event;
}

class schema_registry_client {
public:
    explicit schema_registry_client(
      std::unique_ptr<redpanda::sr::schema_registry_client> client)
      : _client(std::move(client)) {}
    schema_registry_client(const schema_registry_client&) = delete;
    schema_registry_client& operator=(const schema_registry_client&) = delete;
    schema_registry_client(schema_registry_client&&) = default;
    schema_registry_client& operator=(schema_registry_client&&) = default;
    ~schema_registry_client() = default;

    std::expected<qjs::value, qjs::exception>
    lookup_schema_by_id(JSContext* ctx, std::span<qjs::value> params) {
        if (params.size() != 1) [[unlikely]] {
            return std::unexpected(qjs::exception::make(
              ctx,
              std::format(
                "invalid number of parameters to "
                "SchemaRegistryClient.lookupSchemaById, got: {}, "
                "expected: 1",
                params.size())));
        }
        auto& param = params.front();
        if (!param.is_number()) [[unlikely]] {
            return std::unexpected(qjs::exception::make(
              ctx,
              "expected integer for 'id' param to "
              "SchemaRegistryClient.lookupSchemaById"));
        }

        return _client
          ->lookup_schema_by_id(redpanda::sr::schema_id{
            static_cast<redpanda::sr::schema_id::type>(param.as_integer())})
          .transform_error([ctx](std::error_code err) {
              return qjs::exception::make(
                ctx, std::format("schema lookup failed: {}", err.message()));
          })
          .and_then([ctx](const redpanda::sr::schema& schema) {
              return make_schema(ctx, schema);
          });
    }

    std::expected<qjs::value, qjs::exception>
    lookup_schema_by_version(JSContext* ctx, std::span<qjs::value> params) {
        if (params.size() != 2) [[unlikely]] {
            return std::unexpected(qjs::exception::make(
              ctx,
              std::format(
                "invalid number of parameters to "
                "SchemaRegistryClient.lookupSchemaByVersion, got: "
                "{}, expected: 2",
                params.size())));
        }
        const auto& subject_param = params[0];
        const auto& version_param = params[1];

        if (!subject_param.is_string()) [[unlikely]] {
            return std::unexpected(qjs::exception::make(
              ctx,
              "expected string for 'subject' param in "
              "SchemaRegistryClient.lookupSchemaByVersion"));
        }

        if (!version_param.is_number()) [[unlikely]] {
            return std::unexpected(qjs::exception::make(
              ctx,
              "expected integer for 'version' param in "
              "SchemaRegistryClient.lookupSchemaByVersion"));
        }

        return _client
          ->lookup_schema_by_version(
            subject_param.string_data().view(),
            redpanda::sr::schema_version{
              static_cast<redpanda::sr::schema_version::type>(
                version_param.as_integer())})
          .transform_error([ctx](std::error_code err) {
              return qjs::exception::make(
                ctx, std::format("error looking up schema: {}", err.message()));
          })
          .and_then([ctx](const redpanda::sr::subject_schema& subj_schema) {
              return make_subject_schema(ctx, subj_schema);
          });
    }

    std::expected<qjs::value, qjs::exception>
    lookup_latest_schema(JSContext* ctx, std::span<qjs::value> params) {
        if (params.size() != 1) [[unlikely]] {
            return std::unexpected(qjs::exception::make(
              ctx,
              std::format(
                "invalid number of parameters to "
                "SchemaRegistryClient.lookupLatestSchema, got: {}, "
                "expected: 1",
                params.size())));
        }
        const auto& subject_param = params[0];
        if (!subject_param.is_string()) [[unlikely]] {
            return std::unexpected(qjs::exception::make(
              ctx,
              "expected string for 'subject' param in "
              "SchemaRegistryClient.lookupLatestSchema"));
        }

        return _client->lookup_latest_schema(subject_param.string_data().view())
          .transform_error([ctx](std::error_code err) {
              return qjs::exception::make(
                ctx, std::format("error looking up schema: {}", err.message()));
          })
          .and_then([ctx](const redpanda::sr::subject_schema& subj_schema) {
              return make_subject_schema(ctx, subj_schema);
          });
    }

    std::expected<qjs::value, qjs::exception>
    create_schema(JSContext* ctx, std::span<qjs::value> params) {
        if (params.size() != 2) [[unlikely]] {
            return std::unexpected(qjs::exception::make(
              ctx,
              std::format(
                "invalid number of parameters to "
                "SchemaRegistryClient.createSchema, got: {}, expected: 2",
                params.size())));
        }
        const auto& subject_param = params[0];
        const auto& schema_param = params[1];

        if (!subject_param.is_string()) [[unlikely]] {
            return std::unexpected(qjs::exception::make(
              ctx,
              "expected string for 'subject' param in "
              "SchemaRegistryClient.createSchema"));
        }
        if (!schema_param.is_object()) [[unlikely]] {
            return std::unexpected(qjs::exception::make(
              ctx,
              "expected an object for 'schema' param in "
              "SchemaRegistryClient.createSchema"));
        }

        return extract_schema(ctx, schema_param)
          .and_then([this, ctx, &subject_param](redpanda::sr::schema schema) {
              // NOTE(oren): sort of annoying that the unexpected<error_code>
              // can't be transformed at the top level of the pipeline
              return _client
                ->create_schema(
                  subject_param.string_data().view(), std::move(schema))
                .transform_error([ctx](std::error_code err) {
                    return qjs::exception::make(
                      ctx,
                      std::format("error creating schema: {}", err.message()));
                });
          })
          .and_then([ctx](const redpanda::sr::subject_schema& subj_schema) {
              return make_subject_schema(ctx, subj_schema);
          });
    }

private:
    std::unique_ptr<redpanda::sr::schema_registry_client> _client;

    static std::expected<redpanda::sr::schema, qjs::exception>
    extract_schema(JSContext* ctx, const qjs::value& val) {
        auto raw_schema = val.get_property("schema");
        if (!raw_schema.is_string()) {
            return std::unexpected(qjs::exception::make(
              ctx, "malformed schema def: expected string for 'schema'"));
        }
        auto format = val.get_property("format");
        if (!format.is_number()) {
            return std::unexpected(qjs::exception::make(
              ctx, "malformed schema def: expected int for 'format'"));
        }
        redpanda::sr::schema::reference_container native_refs;
        auto refs = val.get_property("references");
        if (!refs.is_null() && !refs.is_undefined()) {
            if (!refs.is_array()) {
                return std::unexpected(qjs::exception::make(
                  ctx,
                  "malformed schema def: expected array for 'references'"));
            }

            native_refs.reserve(refs.array_length());
            for (size_t i = 0; i < refs.array_length(); ++i) {
                auto ref = refs.get_element(i);
                if (!ref.is_object()) {
                    return std::unexpected(qjs::exception::make(
                      ctx,
                      "malformed schema def: expected array "
                      "of objects for 'references'"));
                }
                auto name = ref.get_property("name");
                if (!name.is_string()) {
                    return std::unexpected(qjs::exception::make(
                      ctx,
                      "malformed schema def: bad reference: "
                      "'name' should be a string"));
                }
                auto subj = ref.get_property("subject");
                if (!subj.is_string()) {
                    return std::unexpected(qjs::exception::make(
                      ctx,
                      "malformed schema def: bad reference: "
                      "'subject' should be a string"));
                }
                auto vers = ref.get_property("version");
                if (!vers.is_number()) {
                    return std::unexpected(qjs::exception::make(
                      ctx,
                      "malformed schema def: bad reference: "
                      "'version' should be a number"));
                }
                native_refs.emplace_back(
                  std::string{name.string_data().view()},
                  std::string{subj.string_data().view()},
                  redpanda::sr::schema_version{
                    static_cast<redpanda::sr::schema_version::type>(
                      vers.as_integer())});
            }
        }

        switch (auto fmt = format.as_integer()) {
        case static_cast<int32_t>(redpanda::sr::schema_format::avro):
            return redpanda::sr::schema::new_avro(
              std::string{raw_schema.string_data().view()},
              std::make_optional(std::move(native_refs)));
        case static_cast<int32_t>(redpanda::sr::schema_format::protobuf):
            return redpanda::sr::schema::new_protobuf(
              std::string{raw_schema.string_data().view()},
              std::make_optional(std::move(native_refs)));
        case static_cast<int32_t>(redpanda::sr::schema_format::json):
            return redpanda::sr::schema::new_json(
              std::string{raw_schema.string_data().view()},
              std::make_optional(std::move(native_refs)));
        default:
            return std::unexpected(qjs::exception::make(
              ctx,
              std::format(
                "malformed schema def: 'format' out of range, got {}", fmt)));
        }
    }

    static std::expected<qjs::value, qjs::exception> make_subject_schema(
      JSContext* ctx, const redpanda::sr::subject_schema& subj_schema) {
        qjs::value obj = qjs::value::object(ctx);
        return make_schema(ctx, subj_schema.schema)
          .and_then([&obj](const qjs::value& schema) {
              return obj.set_property("schema", schema);
          })
          .and_then([ctx, &subj_schema](std::monostate) {
              return qjs::value::string(ctx, subj_schema.subject);
          })
          .and_then([&obj](const qjs::value& subject) {
              return obj.set_property("subject", subject);
          })
          .and_then([ctx, &subj_schema, &obj](std::monostate) {
              return obj.set_property(
                "version", qjs::value::integer(ctx, subj_schema.version));
          })
          .and_then([ctx, &subj_schema, &obj](std::monostate) {
              return obj.set_property(
                "id", qjs::value::integer(ctx, subj_schema.id));
          })
          .transform([&obj](std::monostate) { return std::move(obj); });
    }

    static std::expected<qjs::value, qjs::exception>
    make_schema(JSContext* ctx, const redpanda::sr::schema& the_schema) {
        qjs::value obj = qjs::value::object(ctx);
        return qjs::value::string(ctx, the_schema.raw_schema)
          .and_then([&obj](const qjs::value& schema) {
              return obj.set_property("schema", schema);
          })
          .and_then([ctx, &obj, &the_schema](std::monostate) {
              return obj.set_property(
                "format",
                qjs::value::integer(ctx, static_cast<int>(the_schema.format)));
          })
          .and_then(
            [ctx, &the_schema](
              std::monostate) -> std::expected<qjs::value, qjs::exception> {
                auto refs = qjs::value::array(ctx);
                for (const auto& ref : the_schema.references) {
                    auto result = make_reference(ctx, ref).and_then(
                      [&refs](const qjs::value& ref) {
                          return refs.push_back(ref);
                      });
                    if (!result.has_value()) {
                        return std::unexpected(result.error());
                    }
                }
                return std::move(refs);
            })
          .and_then([&obj](const qjs::value& refs) {
              return obj.set_property("references", refs);
          })
          .transform([&obj](std::monostate) { return std::move(obj); });
    }

    static std::expected<qjs::value, qjs::exception>
    make_reference(JSContext* ctx, const redpanda::sr::reference& ref) {
        auto obj = qjs::value::object(ctx);
        return qjs::value::string(ctx, ref.name)
          .and_then([&obj](const qjs::value& name) {
              return obj.set_property("name", name);
          })
          .and_then([ctx, &ref](std::monostate) {
              return qjs::value::string(ctx, ref.subject);
          })
          .and_then([&obj](const qjs::value& subj) {
              return obj.set_property("subject", subj);
          })
          .and_then([ctx, &obj, &ref](std::monostate) {
              return obj.set_property(
                "version", qjs::value::integer(ctx, ref.version));
          })
          .transform([&obj](std::monostate) { return std::move(obj); });
    }
};

qjs::class_factory<schema_registry_client>
make_schema_registry_client_class(qjs::runtime* runtime) {
    qjs::class_builder<schema_registry_client> builder(
      runtime->context(), "SchemaRegistryClient");
    builder.method<&schema_registry_client::lookup_schema_by_id>(
      "lookupSchemaById");
    builder.method<&schema_registry_client::lookup_schema_by_version>(
      "lookupSchemaByVersion");
    builder.method<&schema_registry_client::lookup_latest_schema>(
      "lookupLatestSchema");
    builder.method<&schema_registry_client::create_schema>("createSchema");
    return builder.build();
}

redpanda::bytes_view value_as_bytes_view(const qjs::value& val) {
    if (val.is_string()) {
        auto data = val.string_data();
        return redpanda::bytes_view{data.view()};
    }
    if (val.is_array_buffer()) {
        auto data = val.array_buffer_data();
        return redpanda::bytes_view{data.data(), data.size()};
    }
    if (val.is_uint8_array()) {
        auto data = val.uint8_array_data();
        return redpanda::bytes_view{data.data(), data.size()};
    }
    return redpanda::bytes_view{};
}

std::expected<qjs::value, qjs::exception>
decode_schema_id_impl(JSContext* ctx, qjs::value& arg) {
    auto data = value_as_bytes_view(arg);
    auto obj = qjs::value::object(ctx);
    return redpanda::sr::decode_schema_id(data)
      .transform_error([ctx](std::error_code err) {
          return qjs::exception::make(
            ctx, std::format("Failed to decode schema ID: {}", err.message()));
      })
      .and_then(
        [ctx,
         &obj](std::pair<redpanda::sr::schema_id, redpanda::bytes_view> sid) {
            return obj.set_property("id", qjs::value::integer(ctx, sid.first));
        })
      .and_then(
        [ctx,
         &arg](std::monostate) -> std::expected<qjs::value, qjs::exception> {
            constexpr size_t HEADER_SIZE = 5;
            // TODO(perf): Each of these takes a full copy of the input buffer
            // to avoid any memory ownership issues in client code.
            // This is a pessimization - with a bit of effort, we should be
            // able to return subviews that keep the input buffer alive without
            // requiring the runtime to allocate us a whole new buffer.
            if (arg.is_string()) {
                return qjs::value::string(
                  ctx, arg.string_data().view().substr(HEADER_SIZE));
            }
            if (arg.is_array_buffer()) {
                return qjs::value::array_buffer_copy(
                  ctx, arg.array_buffer_data().subspan(HEADER_SIZE));
            }
            if (arg.is_uint8_array()) {
                return qjs::value::uint8_array_copy(
                  ctx, arg.uint8_array_data().subspan(HEADER_SIZE));
            }
            // NOTE(oren): we should never reach here (covered by earlier
            // checks)
            return std::unexpected(qjs::exception::make(
              ctx, "Unexpected type for schema ID decode"));
        })
      .and_then([&obj](const qjs::value& rest) {
          return obj.set_property("rest", rest);
      })
      .transform([&obj](std::monostate) { return std::move(obj); });
}

std::expected<qjs::value, qjs::exception> encode_schema_id_impl(
  JSContext* ctx,
  const qjs::value& sid, // NOLINT(*-swappable-parameters)
  const qjs::value& buf  // NOLINT(*-swappable-parameters)
) {
    auto orig = value_as_bytes_view(buf);
    redpanda::bytes encoded = redpanda::sr::encode_schema_id(
      redpanda::sr::schema_id{sid.as_integer()}, orig);
    // TODO(perf): This  takes a full copy of the input buffer to avoid
    // any memory ownership issues in client code.
    // This is a pessimization - with a bit of effort, we should be
    // able to return subviews that keep the input buffer alive without
    // requiring the runtime to allocate us a whole new buffer.
    return qjs::value::uint8_array_copy(ctx, encoded);
}

std::expected<std::monostate, qjs::exception> initial_native_modules(
  qjs::runtime* runtime,
  qjs::value* user_callback,
  qjs::class_factory<schema_registry_client>* sr_client_factory) {
    auto mod = qjs::module_builder("@redpanda-data/transform-sdk");
    mod.add_function(
      "onRecordWritten",
      [user_callback](
        JSContext* ctx, const qjs::value&, std::span<qjs::value> args)
        -> std::expected<qjs::value, qjs::exception> {
          if (args.size() != 1 || !args.front().is_function()) [[unlikely]] {
              return std::unexpected(qjs::exception::make(
                ctx,
                "invalid argument, onRecordWritten must take only a single "
                "function as an argument"));
          }
          return {std::exchange(*user_callback, std::move(args.front()))};
      });
    auto sr_mod = qjs::module_builder("@redpanda-data/transform-sdk-sr");
    sr_mod.add_function(
      "newClient",
      [&sr_client_factory](
        JSContext* ctx, const qjs::value&, std::span<qjs::value> args)
        -> std::expected<qjs::value, qjs::exception> {
          if (!args.empty()) [[unlikely]] {
              return std::unexpected(
                qjs::exception::make(ctx, "Unexpected arguments to newClient"));
          }
          return sr_client_factory->create(
            std::make_unique<schema_registry_client>(
              redpanda::sr::new_client()));
      });
    sr_mod.add_function(
      "decodeSchemaID",
      [](JSContext* ctx, const qjs::value&, std::span<qjs::value> args)
        -> std::expected<qjs::value, qjs::exception> {
          if (args.size() != 1) [[unlikely]] {
              return std::unexpected(qjs::exception::make(
                ctx,
                std::format(
                  "wrong number of arguments to decodeSchemaID: expected 1 got "
                  "{}",
                  args.size())));
          }
          if (!(args.front().is_string() || args.front().is_array_buffer()
                || args.front().is_uint8_array())) [[unlikely]] {
              return std::unexpected(qjs::exception::make(
                ctx,
                "Illegal argument to decodeSchemaID: Expected one of "
                "string, ArrayBuffer, or Uint8Array"));
          }
          return decode_schema_id_impl(ctx, args.front());
      });
    sr_mod.add_function(
      "encodeSchemaID",
      [](JSContext* ctx, const qjs::value&, std::span<qjs::value> args)
        -> std::expected<qjs::value, qjs::exception> {
          if (args.size() != 2) [[unlikely]] {
              return std::unexpected(qjs::exception::make(
                ctx,
                std::format(
                  "wrong number of arguments to encodeSchemaID: expected 2 got "
                  "{}",
                  args.size())));
          }
          if (!args[0].is_number()) {
              return std::unexpected(qjs::exception::make(
                ctx,
                "expected a numeric SchemaID for 'id' param in "
                "encodeSchemaID"));
          }
          if (!(args[1].is_string() || args[1].is_array_buffer()
                || args[1].is_uint8_array())) {
              return std::unexpected(qjs::exception::make(
                ctx,
                "expected one of string, ArrayBuffer, or Uint8Array for 'buf' "
                "param in encodeSchemaID"));
          }
          return encode_schema_id_impl(ctx, args[0], args[1]);
      });

    qjs::object_builder schema_format;
    schema_format.add_i32("Avro", 0).add_i32("Protobuf", 1).add_i32("JSON", 2);
    sr_mod.add_object("SchemaFormat", std::move(schema_format));

    return runtime->add_module(std::move(mod))
      .and_then([runtime, &sr_mod](std::monostate) {
          return runtime->add_module(std::move(sr_mod));
      });
}

std::expected<std::monostate, qjs::exception>
compile_and_load(qjs::runtime* runtime) {
    std::string source;
    source.resize(redpanda_js_source_file_length(), 'a');
    redpanda_js_source_get_file(source.data());
    auto compile_result = runtime->compile(source);
    if (!compile_result.has_value()) [[unlikely]] {
        auto msg = qjs::value::string(
          runtime->context(),
          std::format(
            "unable to compile module: {}",
            compile_result.error().val.debug_string()));
        return std::unexpected(qjs::exception(msg.value()));
    }
    auto load_result = runtime->load(compile_result.value().raw());
    if (!load_result.has_value()) [[unlikely]] {
        auto msg = qjs::value::string(
          runtime->context(),
          std::format(
            "unable to load module: {}",
            load_result.error().val.debug_string()));
        if (!msg) {
            return std::unexpected(msg.error());
        }
        return std::unexpected(qjs::exception(msg.value()));
    }
    return {};
}

int run() {
    qjs::runtime runtime;
    auto result = runtime.create_builtins();
    if (!result) {
        std::println(
          stderr,
          "unable to install globals: {}",
          result.error().val.debug_string());
        return 1;
    }
    auto writer_factory = make_record_writer_class(&runtime);
    auto data_factory = make_record_data_class(&runtime);
    qjs::value record_callback = qjs::value::undefined(runtime.context());
    auto sr_client_factory = make_schema_registry_client_class(&runtime);
    result = initial_native_modules(
      &runtime, &record_callback, &sr_client_factory);
    if (!result) [[unlikely]] {
        std::println(
          stderr,
          "unable to install native modules: {}",
          result.error().val.debug_string());
        return 1;
    }
    result = compile_and_load(&runtime);
    if (!result) [[unlikely]] {
        std::println(
          stderr,
          "unable to load module: {}",
          result.error().val.debug_string());
        return 1;
    }
    if (!record_callback.is_function()) [[unlikely]] {
        std::println(stderr, "module did not call onRecordWritten");
        return 1;
    }
    redpanda::on_record_written(
      [&runtime, &writer_factory, &record_callback, &data_factory](
        const redpanda::write_event& evt, redpanda::record_writer* writer) {
          const qjs::value js_writer = writer_factory.create(
            std::make_unique<record_writer>(writer, &data_factory));
          auto event_result = make_write_event(
            runtime.context(), evt, &data_factory);
          if (!event_result) [[unlikely]] {
              std::println(
                stderr,
                "error creating event: {}",
                event_result.error().val.debug_string());
              return std::make_error_code(std::errc::bad_message);
          }
          auto args = std::to_array({event_result.value(), js_writer});
          auto js_result = record_callback.call(args);
          if (js_result.has_value()) {
              // TODO(rockwood): if this is a promise we should await it.
              return std::error_code();
          }
          std::println(
            stderr,
            "error processing record: {}",
            js_result.error().val.debug_string());
          return std::make_error_code(std::errc::interrupted);
      });
    return 0;
}

} // namespace redpanda::js

int main() { return redpanda::js::run(); }
