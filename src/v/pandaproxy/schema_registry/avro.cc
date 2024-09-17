/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "pandaproxy/schema_registry/avro.h"

#include "bytes/streambuf.h"
#include "json/allocator.h"
#include "json/chunked_input_stream.h"
#include "json/document.h"
#include "json/json.h"
#include "json/types.h"
#include "pandaproxy/schema_registry/compatibility.h"
#include "pandaproxy/schema_registry/error.h"
#include "pandaproxy/schema_registry/errors.h"
#include "pandaproxy/schema_registry/sharded_store.h"
#include "pandaproxy/schema_registry/types.h"
#include "strings/string_switch.h"

#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/exception.hh>
#include <seastar/util/defer.hh>

#include <absl/container/flat_hash_set.h>
#include <avro/Compiler.hh>
#include <avro/Exception.hh>
#include <avro/GenericDatum.hh>
#include <avro/Stream.hh>
#include <avro/Types.hh>
#include <avro/ValidSchema.hh>
#include <boost/outcome/std_result.hpp>
#include <boost/outcome/success_failure.hpp>
#include <fmt/core.h>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <rapidjson/error/en.h>

#include <exception>
#include <stack>
#include <string_view>

namespace pandaproxy::json {
using namespace ::json;
}

namespace pandaproxy::schema_registry {

namespace {

using avro_compatibility_result = raw_compatibility_result;

avro_compatibility_result check_compatible(
  avro::Node& reader, avro::Node& writer, std::filesystem::path p = {}) {
    auto type_to_upper = [](avro::Type t) {
        auto s = toString(t);
        std::transform(s.begin(), s.end(), s.begin(), ::toupper);
        return s;
    };
    avro_compatibility_result compat_result;
    if (reader.type() == writer.type()) {
        // Do some quick checks first
        // These are detectable by the blunt `resolve` check below, but we want
        // to extract as much error info as possible.
        if (reader.type() == avro::Type::AVRO_ARRAY) {
            compat_result.merge(check_compatible(
              *reader.leafAt(0), *writer.leafAt(0), p / "items"));
        } else if (reader.hasName() && reader.name() != writer.name()) {
            // The Avro library doesn't fully support handling schema resolution
            // with name aliases yet. While `equalOrAliasedBy` is available,
            // `writer.resolve(reader)` doesn't take into account the aliases.
            // Once Avro supports name alias resolution, we should only return a
            // name_mismatch when
            // `!writer.name().equalOrAliasedBy(reader.name())`, however, in the
            // meantime it is best to return a more specific error.
            auto suffix = writer.name().equalOrAliasedBy(reader.name())
                            ? " (alias resolution is not yet fully supported)"
                            : "";
            compat_result.emplace<avro_incompatibility>(
              p / "name",
              avro_incompatibility::Type::name_mismatch,
              fmt::format("expected: {}{}", writer.name(), suffix));
        } else if (
          reader.type() == avro::Type::AVRO_FIXED
          && reader.fixedSize() != writer.fixedSize()) {
            compat_result.emplace<avro_incompatibility>(
              p / "size",
              avro_incompatibility::Type::fixed_size_mismatch,
              fmt::format(
                "expected: {}, found: {}",
                writer.fixedSize(),
                reader.fixedSize()));
        } else if (!writer.resolve(reader)) {
            // Everything else is an UNKNOWN error with the current path
            compat_result.emplace<avro_incompatibility>(
              std::move(p), avro_incompatibility::Type::unknown);
            return compat_result;
        }

        if (reader.type() == avro::Type::AVRO_RECORD) {
            // Recursively check fields
            auto fields_p = p / "fields";
            for (size_t r_idx = 0; r_idx < reader.names(); ++r_idx) {
                size_t w_idx{0};
                if (writer.nameIndex(reader.nameAt(int(r_idx)), w_idx)) {
                    // schemas for fields with the same name in both records
                    // are resolved recursively.
                    compat_result.merge(check_compatible(
                      *reader.leafAt(int(r_idx)),
                      *writer.leafAt(int(w_idx)),
                      fields_p / std::to_string(r_idx) / "type"));
                } else if (
                  reader.defaultValueAt(int(r_idx)).type() == avro::AVRO_NULL) {
                    // if the reader's record schema has a field with no default
                    // value, and writer's schema does not have a field with the
                    // same name, an error is signalled.

                    // For union, the default must correspond to the first type.
                    // The default may be null.
                    const auto& r_leaf = reader.leafAt(int(r_idx));
                    if (
                      r_leaf->type() != avro::Type::AVRO_UNION
                      || r_leaf->leafAt(0)->type() != avro::Type::AVRO_NULL) {
                        compat_result.emplace<avro_incompatibility>(
                          fields_p / std::to_string(r_idx),
                          avro_incompatibility::Type::
                            reader_field_missing_default_value,
                          reader.nameAt(r_idx));
                    }
                }
            }
        } else if (reader.type() == avro::AVRO_ENUM) {
            // if the writer's symbol is not present in the reader's enum and
            // the reader has a default value, then that value is used,
            // otherwise an error is signalled.
            if (reader.defaultValueAt(0).type() == avro::AVRO_NULL) {
                std::vector<std::string_view> missing;
                for (size_t w_idx = 0; w_idx < writer.names(); ++w_idx) {
                    size_t r_idx{0};
                    if (const auto& n = writer.nameAt(int(w_idx));
                        !reader.nameIndex(n, r_idx)) {
                        missing.emplace_back(n);
                    }
                }
                if (!missing.empty()) {
                    compat_result.emplace<avro_incompatibility>(
                      p / "symbols",
                      avro_incompatibility::Type::missing_enum_symbols,
                      fmt::format("[{}]", fmt::join(missing, ", ")));
                }
            }
        } else if (reader.type() == avro::AVRO_UNION) {
            // The first schema in the reader's union that matches the selected
            // writer's union schema is recursively resolved against it. if none
            // match, an error is signalled.
            //
            // Alternatively, any reader must match every writer schema
            for (size_t w_idx = 0; w_idx < writer.leaves(); ++w_idx) {
                bool is_compat = false;
                for (size_t r_idx = 0; r_idx < reader.leaves(); ++r_idx) {
                    if (!check_compatible(
                           *reader.leafAt(int(r_idx)),
                           *writer.leafAt(int(w_idx)))
                           .has_error()) {
                        is_compat = true;
                    }
                }
                if (!is_compat) {
                    compat_result.emplace<avro_incompatibility>(
                      p / std::to_string(w_idx),
                      avro_incompatibility::Type::missing_union_branch,
                      fmt::format(
                        "reader union lacking writer type: {}",
                        type_to_upper(writer.leafAt(w_idx)->type())));
                }
            }
        }
    } else if (reader.type() == avro::AVRO_UNION) {
        // The first schema in the reader's union that matches the writer's
        // schema is recursively resolved against it. If none match, an error is
        // signalled.
        //
        // Alternatively, any schema in the reader union must match writer.
        bool is_compat = false;
        for (size_t r_idx = 0; r_idx < reader.leaves(); ++r_idx) {
            if (!check_compatible(*reader.leafAt(int(r_idx)), writer)
                   .has_error()) {
                is_compat = true;
            }
        }
        if (!is_compat) {
            compat_result.emplace<avro_incompatibility>(
              std::move(p),
              avro_incompatibility::Type::missing_union_branch,
              fmt::format(
                "reader union lacking writer type: {}",
                type_to_upper(writer.type())));
        }
    } else if (writer.type() == avro::AVRO_UNION) {
        // If the reader's schema matches the selected writer's schema, it is
        // recursively resolved against it. If they do not match, an error is
        // signalled.
        //
        // Alternatively, reader must match all schema in writer union.
        for (size_t w_idx = 0; w_idx < writer.leaves(); ++w_idx) {
            compat_result.merge(
              check_compatible(reader, *writer.leafAt(int(w_idx)), p));
        }
    } else if (writer.resolve(reader) == avro::RESOLVE_NO_MATCH) {
        compat_result.emplace<avro_incompatibility>(
          std::move(p),
          avro_incompatibility::Type::type_mismatch,
          fmt::format(
            "reader type: {} not compatible with writer type: {}",
            type_to_upper(reader.type()),
            type_to_upper(writer.type())));
    }

    return compat_result;
}

enum class object_type { complex, field };

template<object_type type>
struct member_sorter {
    bool operator()(
      const json::Document::Member& lhs, const json::Document::Member& rhs) {
        constexpr auto order = [](std::string_view name) {
            auto val = string_switch<char>(name)
                         .match("type", type == object_type::complex ? 0 : 1)
                         .match("name", type == object_type::complex ? 1 : 0)
                         .match("namespace", 2)
                         .match("doc", 3)
                         .match("fields", 4)
                         .match("order", 5)
                         .match("symbols", 6)
                         .match("items", 7)
                         .match("values", 8)
                         .match("default", 9)
                         .match("size", 10)
                         .match("aliases", 11)
                         .default_match(std::numeric_limits<char>::max());
            return val;
        };
        constexpr auto as_string_view = [](const json::Value& v) {
            return std::string_view{v.GetString(), v.GetStringLength()};
        };
        return order(as_string_view(lhs.name))
               < order(as_string_view(rhs.name));
    }
};

struct sanitize_context {
    json::MemoryPoolAllocator& alloc;
    // The stack of namespaces, starting with implictly null
    std::stack<ss::sstring> ns{{""}};
};

result<void> sanitize(json::Value& v, sanitize_context& ctx);
result<void> sanitize(json::Value::Object& o, sanitize_context& ctx);
result<void> sanitize(json::Value::Array& a, sanitize_context& ctx);

result<void>
sanitize_union_symbol_name(json::Value& name, sanitize_context& ctx) {
    // A name should have the leading dot stripped iff it's the only one

    if (!name.IsString() || name.GetStringLength() == 0) {
        return error_info{
          error_code::schema_invalid, "Invalid JSON Field \"name\""};
    }

    std::string_view fullname_sv{name.GetString(), name.GetStringLength()};
    auto last_dot = fullname_sv.find_last_of('.');

    if (last_dot == 0) {
        fullname_sv.remove_prefix(1);
        // SetString uses memcpy, take a copy so the range doesn't overlap.
        auto new_name = ss::sstring{fullname_sv};
        name.SetString(new_name.data(), new_name.length(), ctx.alloc);
    }
    return outcome::success();
}

result<void> sanitize_record(json::Value::Object& v, sanitize_context& ctx) {
    auto f_it = v.FindMember("fields");
    if (f_it == v.MemberEnd()) {
        return error_info{
          error_code::schema_invalid, "Missing JSON field \"fields\""};
    }
    if (!f_it->value.IsArray()) {
        return error_info{
          error_code::schema_invalid, "JSON field \"fields\" is not an array"};
    }
    return sanitize(f_it->value, ctx);
}

result<void> sanitize_avro_type(
  json::Value::Object& o, std::string_view type_sv, sanitize_context& ctx) {
    auto type = string_switch<std::optional<avro::Type>>(type_sv)
                  .match("record", avro::Type::AVRO_RECORD)
                  .match("array", avro::Type::AVRO_ARRAY)
                  .match("enum", avro::Type::AVRO_ENUM)
                  .match("map", avro::Type::AVRO_MAP)
                  .match("fixed", avro::Type::AVRO_FIXED)
                  .default_match(std::nullopt);
    if (!type.has_value()) {
        std::sort(o.begin(), o.end(), member_sorter<object_type::field>{});
        return outcome::success();
    }

    switch (type.value()) {
    case avro::AVRO_ARRAY:
    case avro::AVRO_ENUM:
    case avro::AVRO_FIXED:
    case avro::AVRO_MAP:
        std::sort(o.begin(), o.end(), member_sorter<object_type::complex>{});
        for (auto& i : o) {
            if (auto res = sanitize(i.value, ctx); !res.has_value()) {
                return res;
            }
        }
        break;
    case avro::AVRO_RECORD: {
        auto res = sanitize_record(o, ctx);
        std::sort(o.begin(), o.end(), member_sorter<object_type::complex>{});
        return res;
    }
    default:
        break;
    }
    return outcome::success();
}

result<void> sanitize(json::Value& v, sanitize_context& ctx) {
    switch (v.GetType()) {
    case json::Type::kObjectType: {
        auto o = v.GetObject();
        return sanitize(o, ctx);
    }
    case json::Type::kArrayType: {
        auto a = v.GetArray();
        return sanitize(a, ctx);
    }
    case json::Type::kFalseType:
    case json::Type::kTrueType:
    case json::Type::kNullType:
    case json::Type::kNumberType:
    case json::Type::kStringType:
        return outcome::success();
    }
    __builtin_unreachable();
}

result<void> sanitize(json::Value::Object& o, sanitize_context& ctx) {
    auto pop_ns_impl = [&ctx]() { ctx.ns.pop(); };
    std::optional<ss::deferred_action<decltype(pop_ns_impl)>> pop_ns;

    if (auto it = o.FindMember("name"); it != o.MemberEnd()) {
        // Sanitize names and namespaces according to
        // https://avro.apache.org/docs/1.11.1/specification/#names
        //
        // This sanitization:
        // * Is not Parsing Canonical Form
        // * Splits fullnames into a simple name and a namespace
        //   * A namespace attribute is ignored if the name is a fullname
        // * Removes namespaces that are redundant (same as parent scope)

        auto& name = it->value;

        if (!name.IsString() || name.GetStringLength() == 0) {
            return error_info{
              error_code::schema_invalid, "Invalid JSON Field \"name\""};
        }

        std::string_view fullname_sv{name.GetString(), name.GetStringLength()};
        auto last_dot = fullname_sv.find_last_of('.');

        std::optional<ss::sstring> new_namespace;
        if (last_dot != std::string::npos) {
            // Take a copy, fullname_sv will be invalidated when new_name is
            // set, and SetString uses memcpy, the range musn't overlap.
            ss::sstring fullname{fullname_sv};
            fullname_sv = fullname;

            auto new_name{fullname_sv.substr(last_dot + 1)};
            name.SetString(new_name.data(), new_name.length(), ctx.alloc);

            fullname.resize(last_dot);
            new_namespace = std::move(fullname);
        }

        if (!new_namespace.has_value()) {
            if (auto it = o.FindMember("namespace"); it != o.MemberEnd()) {
                if (!it->value.IsString()) {
                    return error_info{
                      error_code::schema_invalid,
                      "Invalid JSON Field \"namespace\""};
                }
                new_namespace.emplace(
                  it->value.GetString(), it->value.GetStringLength());
            }
        }

        if (new_namespace.has_value() && ctx.ns.top() != new_namespace) {
            ctx.ns.emplace(*new_namespace);
            pop_ns.emplace(std::move(pop_ns_impl));
            if (auto it = o.FindMember("namespace"); it != o.MemberEnd()) {
                if (!it->value.IsString()) {
                    return error_info{
                      error_code::schema_invalid,
                      "Invalid JSON Field \"namespace\""};
                }
                std::string_view existing_namespace{
                  it->value.GetString(), it->value.GetStringLength()};
                if (existing_namespace != new_namespace) {
                    it->value.SetString(
                      new_namespace->data(),
                      new_namespace->length(),
                      ctx.alloc);
                }
            } else {
                o.AddMember(
                  json::Value("namespace"),
                  json::Value(
                    new_namespace->data(), new_namespace->length(), ctx.alloc),
                  ctx.alloc);
            }
        } else {
            o.RemoveMember("namespace");
        }
    }

    if (auto t_it = o.FindMember("type"); t_it != o.MemberEnd()) {
        auto res = sanitize(t_it->value, ctx);
        if (res.has_error()) {
            return res.assume_error();
        } else if (t_it->value.GetType() == json::Type::kStringType) {
            std::string_view type_sv = {
              t_it->value.GetString(), t_it->value.GetStringLength()};
            auto res = sanitize_avro_type(o, type_sv, ctx);
            if (res.has_error()) {
                return res.assume_error();
            }
        } else if (t_it->value.GetType() == json::Type::kArrayType) {
            auto a = t_it->value.GetArray();
            for (auto& m : a) {
                if (m.IsString()) {
                    auto res = sanitize_union_symbol_name(m, ctx);
                    if (res.has_error()) {
                        return res.assume_error();
                    }
                }
            }
            auto res = sanitize_avro_type(o, "field", ctx);
            if (res.has_error()) {
                return res.assume_error();
            }
        } else if (t_it->value.GetType() == json::Type::kObjectType) {
            auto res = sanitize_avro_type(o, "field", ctx);
            if (res.has_error()) {
                return res.assume_error();
            }
        }
    }
    return outcome::success();
}

result<void> sanitize(json::Value::Array& a, sanitize_context& ctx) {
    for (auto& m : a) {
        auto s = sanitize(m, ctx);
        if (s.has_error()) {
            return s.assume_error();
        }
    }
    return outcome::success();
}

} // namespace

avro_schema_definition::avro_schema_definition(
  avro::ValidSchema vs, canonical_schema_definition::references refs)
  : _impl(std::move(vs))
  , _refs(std::move(refs)) {}

const avro::ValidSchema& avro_schema_definition::operator()() const {
    return _impl;
}

bool operator==(
  const avro_schema_definition& lhs, const avro_schema_definition& rhs) {
    return lhs.raw() == rhs.raw();
}

std::ostream& operator<<(std::ostream& os, const avro_schema_definition& def) {
    fmt::print(
      os,
      "type: {}, definition: {}",
      to_string_view(def.type()),
      def().toJson(false));
    return os;
}

canonical_schema_definition::raw_string avro_schema_definition::raw() const {
    iobuf_ostream os;
    _impl.toJson(os.ostream());
    return canonical_schema_definition::raw_string{
      json::minify(std::move(os).buf())};
}

ss::sstring avro_schema_definition::name() const {
    return _impl.root()->name().fullname();
};

class collected_schema {
public:
    bool contains(const ss::sstring& name) const {
        return _names.contains(name);
    }
    bool insert(ss::sstring name, canonical_schema_definition def) {
        bool inserted = _names.insert(std::move(name)).second;
        if (inserted) {
            _schemas.push_back(std::move(def).raw());
        }
        return inserted;
    }
    canonical_schema_definition::raw_string flatten() && {
        iobuf out;
        for (auto& s : _schemas) {
            out.append(std::move(s));
            out.append("\n", 1);
        }
        return canonical_schema_definition::raw_string{std::move(out)};
    }

private:
    absl::flat_hash_set<ss::sstring> _names;
    std::vector<canonical_schema_definition::raw_string> _schemas;
};

ss::future<collected_schema> collect_schema(
  sharded_store& store,
  collected_schema collected,
  ss::sstring name,
  canonical_schema schema) {
    for (const auto& ref : schema.def().refs()) {
        if (!collected.contains(ref.name)) {
            auto ss = co_await store.get_subject_schema(
              ref.sub, ref.version, include_deleted::no);
            collected = co_await collect_schema(
              store, std::move(collected), ref.name, std::move(ss.schema));
        }
    }
    collected.insert(std::move(name), std::move(schema).def());
    co_return std::move(collected);
}

ss::future<avro_schema_definition>
make_avro_schema_definition(sharded_store& store, canonical_schema schema) {
    std::optional<avro::Exception> ex;
    try {
        auto name = schema.sub()();
        auto schema_refs = schema.def().refs();
        auto refs = co_await collect_schema(store, {}, name, std::move(schema));
        iobuf_istream sis{std::move(refs).flatten()()};
        auto is = avro::istreamInputStream(sis.istream());
        co_return avro_schema_definition{
          avro::compileJsonSchemaFromStream(*is), std::move(schema_refs)};
    } catch (const avro::Exception& e) {
        ex = e;
    }
    co_return ss::coroutine::exception(
      std::make_exception_ptr(as_exception(error_info{
        error_code::schema_invalid,
        fmt::format("Invalid schema {}", ex->what())})));
}

result<canonical_schema_definition>
sanitize_avro_schema_definition(unparsed_schema_definition def) {
    json::Document doc;
    constexpr auto flags = rapidjson::kParseDefaultFlags
                           | rapidjson::kParseStopWhenDoneFlag;
    if (def.raw()().empty()) {
        auto ec = error_code::schema_empty;
        return error_info{ec, make_error_code(ec).message()};
    }
    json::chunked_input_stream is{def.shared_raw()()};
    doc.ParseStream<flags>(is);
    if (doc.HasParseError()) {
        return error_info{
          error_code::schema_invalid,
          fmt::format(
            "Invalid schema: {} at offset {}",
            rapidjson::GetParseError_En(doc.GetParseError()),
            doc.GetErrorOffset())};
    }
    sanitize_context ctx{.alloc = doc.GetAllocator()};
    auto res = sanitize(doc, ctx);
    if (res.has_error()) {
        // TODO BP: Prevent this linearizaton
        iobuf_parser p(std::move(def).raw()());
        return error_info{
          res.assume_error().code(),
          fmt::format(
            "{} {}",
            res.assume_error().message(),
            p.read_string(p.bytes_left()))};
    }

    json::chunked_buffer buf;
    json::Writer<json::chunked_buffer> w{buf};

    if (!doc.Accept(w)) {
        return error_info{error_code::schema_invalid, "Invalid schema"};
    }

    return canonical_schema_definition{
      canonical_schema_definition::raw_string{std::move(buf).as_iobuf()},
      schema_type::avro,
      def.refs()};
}

compatibility_result check_compatible(
  const avro_schema_definition& reader,
  const avro_schema_definition& writer,
  verbose is_verbose) {
    return check_compatible(
      *reader().root(), *writer().root(), "/")(is_verbose);
}

} // namespace pandaproxy::schema_registry
