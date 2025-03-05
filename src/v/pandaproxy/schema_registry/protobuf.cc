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

#include "pandaproxy/schema_registry/protobuf.h"

#include "base/vlog.h"
#include "bytes/streambuf.h"
#include "kafka/protocol/errors.h"
#include "pandaproxy/logger.h"
#include "pandaproxy/schema_registry/compatibility.h"
#include "pandaproxy/schema_registry/errors.h"
#include "pandaproxy/schema_registry/sharded_store.h"
#include "pandaproxy/schema_registry/types.h"
#include "ssx/sformat.h"
#include "utils/base64.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/sstring.hh>
#include <seastar/util/variant_utils.hh>

#include <absl/container/flat_hash_set.h>
#include <absl/strings/ascii.h>
#include <absl/strings/escaping.h>
#include <boost/algorithm/string/trim.hpp>
#include <boost/container/flat_set.hpp>
#include <boost/range/combine.hpp>
#include <confluent/meta.pb.h>
#include <confluent/types/decimal.pb.h>
#include <fmt/core.h>
#include <fmt/ostream.h>
#include <google/protobuf/any.pb.h>
#include <google/protobuf/api.pb.h>
#include <google/protobuf/compiler/parser.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/descriptor.pb.h>
#include <google/protobuf/descriptor_database.h>
#include <google/protobuf/duration.pb.h>
#include <google/protobuf/empty.pb.h>
#include <google/protobuf/field_mask.pb.h>
#include <google/protobuf/io/tokenizer.h>
#include <google/protobuf/io/zero_copy_stream.h>
#include <google/protobuf/source_context.pb.h>
#include <google/protobuf/struct.pb.h>
#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/type.pb.h>
#include <google/protobuf/util/type_resolver.h>
#include <google/protobuf/wrappers.pb.h>
#include <google/type/calendar_period.pb.h>
#include <google/type/color.pb.h>
#include <google/type/date.pb.h>
#include <google/type/datetime.pb.h>
#include <google/type/dayofweek.pb.h>
#include <google/type/decimal.pb.h>
#include <google/type/expr.pb.h>
#include <google/type/fraction.pb.h>
#include <google/type/interval.pb.h>
#include <google/type/latlng.pb.h>
#include <google/type/localized_text.pb.h>
#include <google/type/money.pb.h>
#include <google/type/month.pb.h>
#include <google/type/phone_number.pb.h>
#include <google/type/postal_address.pb.h>
#include <google/type/quaternion.pb.h>
#include <google/type/timeofday.pb.h>

#include <algorithm>
#include <charconv>
#include <concepts>
#include <cstdint>
#include <functional>
#include <optional>
#include <ranges>
#include <string_view>
#include <unordered_set>

namespace {

// Protobuf string values need to be quoted and escaped
// as they may contain characters like '\'
auto pb_string_value(const std::string_view v) {
    return fmt::format("\"{}\"", absl::CEscape(v));
}

} // namespace

struct indent_formatter : fmt::formatter<std::string_view> {
    using Base = fmt::formatter<std::string_view>;
    constexpr auto parse(fmt::format_parse_context& ctx) {
        auto it = ctx.begin();
        if (it != ctx.end() && *it == ':') {
            ++it;
        }
        if (it != ctx.end() && *it == 'i') {
            auto [ptr, ec] = std::from_chars(std::next(it), ctx.end(), indent);
            if (ec == std::errc{}) {
                it = ptr;
            }
        }
        return Base::parse(ctx);
    }

    template<typename T>
    auto format(const T& t, fmt::format_context& ctx) const
      -> decltype(ctx.out()) {
        if (indent > 0) {
            fmt::format_to(ctx.out(), "{:{}}", "", indent);
        }
        return Base::format(t, ctx);
    }

    size_t indent{0};
};

template<>
struct fmt::formatter<google::protobuf::UninterpretedOption_NamePart>
  : formatter<std::string_view> {
    using formatter<std::string_view>::format;
    auto format(
      const google::protobuf::UninterpretedOption_NamePart& np,
      format_context& ctx) const {
        if (np.has_is_extension() && np.is_extension()) {
            return fmt::format_to(ctx.out(), "({})", np.name_part());
        }
        if (np.has_name_part()) {
            return fmt::format_to(ctx.out(), "{}", np.name_part());
        }

        return ctx.out();
    }
};

template<>
struct fmt::formatter<google::protobuf::UninterpretedOption>
  : indent_formatter {
    using indent_formatter::format;
    using indent_formatter::parse;
    auto format(
      const google::protobuf::UninterpretedOption& option,
      format_context& ctx) const {
        const auto fmt = [&](const auto& val) {
            if constexpr (std::convertible_to<
                            std::decay_t<decltype(val)>,
                            std::string_view>) {
                if (option.has_string_value()) {
                    return fmt::format_to(
                      ctx.out(),
                      "{} = {}",
                      fmt::join(option.name(), "."),
                      pb_string_value(val));
                }
            }
            if (option.has_aggregate_value()) {
                return fmt::format_to(
                  ctx.out(),
                  "{} = {{{}\n{:{}}}}",
                  fmt::join(option.name(), "."),
                  val,
                  "",
                  indent + 2);
            }
            return fmt::format_to(
              ctx.out(), "{} = {}", fmt::join(option.name(), "."), val);
        };
        if (option.has_identifier_value()) {
            return fmt(option.identifier_value());
        } else if (option.has_positive_int_value()) {
            return fmt(option.positive_int_value());
        } else if (option.has_negative_int_value()) {
            return fmt(option.negative_int_value());
        } else if (option.has_double_value()) {
            return fmt(option.double_value());
        } else if (option.has_string_value()) {
            return fmt(option.string_value());
        } else if (option.has_aggregate_value()) {
            return fmt(option.aggregate_value());
        }
        return ctx.out();
    }
};

template<>
struct fmt::formatter<google::protobuf::FieldOptions::OptionRetention>
  : indent_formatter {
    auto format(
      const google::protobuf::FieldOptions::OptionRetention& option,
      format_context& ctx) const {
        fmt::format_to(
          ctx.out(),
          "{}",
          google::protobuf::FieldOptions::OptionRetention_Name(option));
        return ctx.out();
    }
};

template<>
struct fmt::formatter<google::protobuf::FieldOptions::CType>
  : indent_formatter {
    auto format(
      const google::protobuf::FieldOptions::CType& option,
      format_context& ctx) const {
        fmt::format_to(
          ctx.out(), "{}", google::protobuf::FieldOptions::CType_Name(option));
        return ctx.out();
    }
};
template<>
struct fmt::formatter<google::protobuf::FieldOptions::JSType>
  : indent_formatter {
    auto format(
      const google::protobuf::FieldOptions::JSType& option,
      format_context& ctx) const {
        fmt::format_to(
          ctx.out(), "{}", google::protobuf::FieldOptions::JSType_Name(option));
        return ctx.out();
    }
};
template<>
struct fmt::formatter<google::protobuf::ExtensionRangeOptions_VerificationState>
  : indent_formatter {
    auto format(
      const google::protobuf::ExtensionRangeOptions_VerificationState& state,
      format_context& ctx) const {
        fmt::format_to(
          ctx.out(),
          "{}",
          google::protobuf::ExtensionRangeOptions_VerificationState_Name(
            state));
        return ctx.out();
    }
};

namespace pandaproxy::schema_registry {

// Make backporting easier.
using schema_getter = sharded_store;

namespace pb = google::protobuf;

template<typename T>
struct field_option {
    field_option(
      std::string_view name,
      bool (pb::FieldOptions::*check)() const,
      T (pb::FieldOptions::*field)() const)
      : name{name}
      , check{check}
      , field{field} {}
    std::string_view name;
    bool (pb::FieldOptions::*check)() const;
    T (pb::FieldOptions::*field)() const;
};

auto field_options() {
    return std::make_tuple(
      field_option{
        "debug_redact",
        &pb::FieldOptions::has_debug_redact,
        &pb::FieldOptions::debug_redact},
      field_option{
        "deprecated",
        &pb::FieldOptions::has_deprecated,
        &pb::FieldOptions::deprecated},
      field_option{
        "retention",
        &pb::FieldOptions::has_retention,
        &pb::FieldOptions::retention},
      field_option{
        "packed", &pb::FieldOptions::has_packed, &pb::FieldOptions::packed},
      field_option{
        "lazy", &pb::FieldOptions::has_lazy, &pb::FieldOptions::lazy},
      field_option{
        "unverified_lazy",
        &pb::FieldOptions::has_unverified_lazy,
        &pb::FieldOptions::unverified_lazy},
      field_option{
        "weak", &pb::FieldOptions::has_weak, &pb::FieldOptions::weak},
      field_option{
        "ctype", &pb::FieldOptions::has_ctype, &pb::FieldOptions::ctype},
      field_option{
        "jstype", &pb::FieldOptions::has_jstype, &pb::FieldOptions::jstype});
}

struct descriptor_hasher {
    using is_transparent = void;

    std::size_t operator()(const pb::FileDescriptor* s) const {
        return absl::Hash<std::string>()(s->name());
    }
    std::size_t operator()(const ss::sstring& s) const {
        return absl::Hash<ss::sstring>()(s);
    }
};

struct descriptor_equal {
    using is_transparent = void;

    bool operator()(
      const pb::FileDescriptor* lhs, const pb::FileDescriptor* rhs) const {
        return lhs->name() == rhs->name();
    }

    bool
    operator()(const pb::FileDescriptor* lhs, const ss::sstring& rhs) const {
        return ss::sstring(lhs->name()) == rhs;
    }
};

using known_types_set = absl::
  flat_hash_set<const pb::FileDescriptor*, descriptor_hasher, descriptor_equal>;
static const known_types_set known_types{
  confluent::Meta::GetDescriptor()->file(),
  confluent::type::Decimal::GetDescriptor()->file(),
  google::type::CalendarPeriod_descriptor()->file(),
  google::type::Color::GetDescriptor()->file(),
  google::type::Date::GetDescriptor()->file(),
  google::type::DateTime::GetDescriptor()->file(),
  google::type::DayOfWeek_descriptor()->file(),
  google::type::Decimal::GetDescriptor()->file(),
  google::type::Expr::GetDescriptor()->file(),
  google::type::Fraction::GetDescriptor()->file(),
  google::type::Interval::GetDescriptor()->file(),
  google::type::LatLng::GetDescriptor()->file(),
  google::type::LocalizedText::GetDescriptor()->file(),
  google::type::Money::GetDescriptor()->file(),
  google::type::Month_descriptor()->file(),
  google::type::PhoneNumber::GetDescriptor()->file(),
  google::type::PostalAddress::GetDescriptor()->file(),
  google::type::Quaternion::GetDescriptor()->file(),
  google::type::TimeOfDay::GetDescriptor()->file(),
  google::protobuf::SourceContext::GetDescriptor()->file(),
  google::protobuf::Any::GetDescriptor()->file(),
  google::protobuf::Option::GetDescriptor()->file(),
  google::protobuf::DoubleValue::GetDescriptor()->file(),
  google::protobuf::Type::GetDescriptor()->file(),
  google::protobuf::Api::GetDescriptor()->file(),
  google::protobuf::Duration::GetDescriptor()->file(),
  google::protobuf::Empty::GetDescriptor()->file(),
  google::protobuf::FieldMask::GetDescriptor()->file(),
  google::protobuf::Struct::GetDescriptor()->file(),
  google::protobuf::Timestamp::GetDescriptor()->file(),
  google::protobuf::FieldDescriptorProto::GetDescriptor()->file()};

class io_error_collector final : public pb::io::ErrorCollector {
    enum class level {
        error,
        warn,
    };
    struct err {
        level lvl;
        int line;
        int column;
        ss::sstring message;
    };

public:
    void RecordError(int line, int column, std::string_view message) final {
        _errors.emplace_back(
          err{level::error, line, column, ss::sstring{message}});
    }
    void RecordWarning(int line, int column, std::string_view message) final {
        _errors.emplace_back(
          err{level::warn, line, column, ss::sstring{message}});
    }

    error_info error() const;

private:
    friend struct fmt::formatter<err>;

    std::vector<err> _errors;
};

class dp_error_collector final : public pb::DescriptorPool::ErrorCollector {
public:
    void RecordError(
      std::string_view filename,
      std::string_view element_name,
      const pb::Message* descriptor,
      ErrorLocation location,
      std::string_view message) final {
        _errors.emplace_back(err{
          level::error,
          ss::sstring{filename},
          ss::sstring{element_name},
          descriptor,
          location,
          ss::sstring{message}});
    }

    void RecordWarning(
      std::string_view filename,
      std::string_view element_name,
      const pb::Message* descriptor,
      ErrorLocation location,
      std::string_view message) final {
        _errors.emplace_back(err{
          level::warn,
          ss::sstring{filename},
          ss::sstring{element_name},
          descriptor,
          location,
          ss::sstring{message}});
    }

    error_info error(std::string_view sub) const;

private:
    enum class level {
        error,
        warn,
    };
    struct err {
        level lvl;
        ss::sstring filename;
        ss::sstring element_name;
        const pb::Message* descriptor;
        ErrorLocation location;
        ss::sstring message;
    };
    friend struct fmt::formatter<err>;

    std::vector<err> _errors;
};

///\brief Implements ZeroCopyInputStream with a copy of the definition
class schema_def_input_stream : public pb::io::ZeroCopyInputStream {
public:
    explicit schema_def_input_stream(const canonical_schema_definition& def)
      : _is{def.shared_raw()}
      , _impl{&_is.istream()} {}

    bool Next(const void** data, int* size) override {
        return _impl.Next(data, size);
    }
    void BackUp(int count) override { return _impl.BackUp(count); }
    bool Skip(int count) override { return _impl.Skip(count); }
    int64_t ByteCount() const override { return _impl.ByteCount(); }

private:
    iobuf_istream _is;
    pb::io::IstreamInputStream _impl;
};

class parser {
public:
    parser()
      : _parser{}
      , _fdp{} {}

    const pb::FileDescriptorProto& parse(const canonical_schema& schema) {
        schema_def_input_stream is{schema.def()};
        io_error_collector error_collector;
        pb::io::Tokenizer t{&is, &error_collector};
        _parser.RecordErrorsTo(&error_collector);

        // Attempt parse a .proto file
        if (!_parser.Parse(&t, &_fdp)) {
            try {
                // base64 decode the schema
                iobuf_istream is{base64_to_iobuf(schema.def().raw()())};
                // Attempt parse as an encoded FileDescriptorProto.pb
                if (!_fdp.ParseFromIstream(&is.istream())) {
                    throw as_exception(error_collector.error());
                }
            } catch (const base64_decoder_exception&) {
                throw as_exception(error_collector.error());
            }
        }
        const auto& sub = schema.sub()();
        _fdp.set_name(std::string_view(sub));
        return _fdp;
    }

private:
    pb::compiler::Parser _parser;
    pb::FileDescriptorProto _fdp;
};

///\brief Build a FileDescriptor using the DescriptorPool.
///
/// Dependencies are required to be in the DescriptorPool.
const pb::FileDescriptor*
build_file(pb::DescriptorPool& dp, const pb::FileDescriptorProto& fdp) {
    dp_error_collector dp_ec;
    for (const auto& dep : fdp.dependency()) {
        if (!dp.FindFileByName(dep)) {
            if (auto it = known_types.find(dep); it != known_types.end()) {
                google::protobuf::FileDescriptorProto p;
                (*it)->CopyTo(&p);
                build_file(dp, p);
            }
        }
    }
    if (auto fd = dp.BuildFileCollectingErrors(fdp, &dp_ec); fd) {
        return fd;
    }
    throw as_exception(dp_ec.error(fdp.name()));
}

///\brief Build a FileDescriptor and import references from the store.
///
/// Recursively import references into the DescriptorPool, building the
/// files on stack unwind.
ss::future<pb::FileDescriptorProto> build_file_with_refs(
  pb::DescriptorPool& dp, schema_getter& store, canonical_schema schema) {
    for (const auto& ref : schema.def().refs()) {
        if (dp.FindFileByName(ref.name)) {
            continue;
        }
        auto dep = co_await store.get_subject_schema(
          ref.sub, ref.version, include_deleted::no);
        co_await build_file_with_refs(
          dp,
          store,
          canonical_schema{subject{ref.name}, std::move(dep.schema).def()});
    }

    parser p;
    auto new_fdp = p.parse(schema);
    build_file(dp, new_fdp);
    co_return new_fdp;
}

///\brief Import a schema in the DescriptorPool and return the
/// FileDescriptor.
ss::future<pb::FileDescriptorProto> import_schema(
  pb::DescriptorPool& dp, schema_getter& store, canonical_schema schema) {
    try {
        co_return co_await build_file_with_refs(dp, store, schema.share());
    } catch (const exception& e) {
        vlog(
          plog.warn, "Failed to decode schema {}: {}", schema.sub(), e.what());
        throw as_exception(invalid_schema(schema));
    }
}

struct protobuf_schema_definition::impl {
    pb::DescriptorPool _dp;
    const pb::FileDescriptor* fd{};
    pb::FileDescriptorProto fdp{};
    normalize is_normalized{normalize::no};
    protobuf_renderer_v2 v2_renderer{protobuf_renderer_v2::no};

    /**
     * debug_string swaps the order of the import and package lines that
     * DebugString produces, so that it conforms to
     * https://protobuf.dev/programming-guides/style/#file-structure
     *
     * from:
     * syntax
     * imports
     * package
     * messages
     *
     * to:
     * syntax
     * package
     * imports
     * messages
     */
    ss::sstring debug_string() const {
        // TODO BP: Prevent this linearization
        auto s = fd->DebugString();

        // reordering not required if no package or no dependencies
        if (fd->package().empty() || fd->dependency_count() == 0) {
            return s;
        }

        std::string_view sv{s};

        constexpr size_t expected_syntax_len = 18;
        auto syntax_pos = sv.find("syntax = \"proto");
        auto syntax_end = syntax_pos + expected_syntax_len;

        auto package = fmt::format("package {};", fd->package());
        auto package_pos = sv.find(package);

        auto imports_pos = syntax_end;
        auto imports_len = package_pos - syntax_end;

        auto trim = [](std::string_view sv) {
            return boost::algorithm::trim_copy_if(
              sv, [](char c) { return c == '\n'; });
        };
        auto header = trim(sv.substr(0, syntax_end));
        auto imports = trim(sv.substr(imports_pos, imports_len));
        auto footer = trim(sv.substr(package_pos + package.length()));

        // TODO BP: Prevent this linearization
        return ssx::sformat(
          "{}\n{}\n\n{}\n\n{}\n", header, package, imports, footer);
    }

    canonical_schema_definition::raw_string raw() const {
        if (!v2_renderer) {
            return canonical_schema_definition::raw_string{debug_string()};
        }
        iobuf_ostream osb;
        if (is_normalized) {
            pb::FileDescriptorProto tmp_fdp;
            fd->CopyTo(&tmp_fdp);
            copy_custom_options(fdp, tmp_fdp);
            render_proto(osb.ostream(), tmp_fdp, *fd);
        } else {
            render_proto(osb.ostream(), fdp, *fd);
        }
        return canonical_schema_definition::raw_string{std::move(osb).buf()};
    }

    template<std::ranges::range Range>
    struct range_proxy {
    public:
        explicit range_proxy(Range&& r)
          : _r{std::forward<Range>(r)} {}
        explicit range_proxy(const Range& r)
          : _r{std::cref(r)} {}
        auto begin() const { return range().begin(); }
        auto end() const { return range().end(); }
        auto empty() const { return range().empty(); }
        auto size() const { return range().size(); }

    private:
        auto range() const {
            return std::visit(
              ss::make_visitor(
                [](std::reference_wrapper<Range>& r) {
                    return std::ranges::subrange(r.get());
                },
                [](const Range& r) { return std::ranges::subrange(r); }),
              _r);
        }
        template<typename Func>
        auto visit(Func&& func) const {
            return std::visit(
              _r,
              [&func](std::reference_wrapper<const Range>& r) {
                  return std::invoke(std::forward<Func>(func), r.get());
              },
              [&func](const Range& r) {
                  return std::invoke(std::forward<Func>(func), r);
              });
        }

        std::variant<Range, std::reference_wrapper<const Range>> _r;
    };

    template<typename Proto>
    void do_copy_custom_options(const Proto& from, Proto& to) const {
        if (!from.has_options()) {
            return;
        }
        const auto& opts_from = from.options();
        if (opts_from.uninterpreted_option_size() == 0) {
            return;
        }
        const auto& uopts_from = opts_from.uninterpreted_option();

        const auto is_custom_option = [](const pb::UninterpretedOption& opt) {
            const auto& name = opt.name();
            return std::ranges::any_of(name, [](const auto& np) {
                return np.has_is_extension() && np.is_extension();
            });
        };
        std::ranges::copy_if(
          uopts_from,
          RepeatedPtrFieldBackInserter(
            to.mutable_options()->mutable_uninterpreted_option()),
          is_custom_option);
    }

    void copy_custom_options(
      const pb::DescriptorProto& from, pb::DescriptorProto& to) const {
        do_copy_custom_options(from, to);

        for (auto&& [field_from, field_to] :
             boost::combine(from.field(), *to.mutable_field())) {
            copy_custom_options(field_from, field_to);
        }

        // oneof decl
        for (auto&& [oneof_from, oneof_to] :
             boost::combine(from.oneof_decl(), *to.mutable_oneof_decl())) {
            do_copy_custom_options(oneof_from, oneof_to);
        }

        // nested messages
        for (auto&& [nested_from, nested_to] :
             boost::combine(from.nested_type(), *to.mutable_nested_type())) {
            copy_custom_options(nested_from, nested_to);
        }

        // nested enums
        for (auto&& [enum_from, enum_to] :
             boost::combine(from.enum_type(), *to.mutable_enum_type())) {
            copy_custom_options(enum_from, enum_to);
        }

        // nested extentions
        for (auto&& [extension_from, extension_to] :
             boost::combine(from.extension(), *to.mutable_extension())) {
            copy_custom_options(extension_from, extension_to);
        }

        // extentions ranges
        for (auto&& [extension_from, extension_to] : boost::combine(
               from.extension_range(), *to.mutable_extension_range())) {
            copy_custom_options(extension_from, extension_to);
        }
    }

    void copy_custom_options(
      const pb::EnumDescriptorProto& from, pb::EnumDescriptorProto& to) const {
        do_copy_custom_options(from, to);

        for (auto&& [value_from, value_to] :
             boost::combine(from.value(), *to.mutable_value())) {
            do_copy_custom_options(value_from, value_to);
        }
    }

    void copy_custom_options(
      const pb::ServiceDescriptorProto& from,
      pb::ServiceDescriptorProto& to) const {
        do_copy_custom_options(from, to);

        for (auto&& [method_from, method_to] :
             boost::combine(from.method(), *to.mutable_method())) {
            do_copy_custom_options(method_from, method_to);
        }
    }

    void copy_custom_options(
      const pb::FieldDescriptorProto& from,
      pb::FieldDescriptorProto& to) const {
        do_copy_custom_options(from, to);
    }

    void copy_custom_options(
      const pb::DescriptorProto_ExtensionRange& from,
      pb::DescriptorProto_ExtensionRange& to) const {
        do_copy_custom_options(from, to);
    }

    void copy_custom_options(
      const pb::FileDescriptorProto& from, pb::FileDescriptorProto& to) const {
        do_copy_custom_options(from, to);

        for (auto&& [message_from, message_to] :
             boost::combine(from.message_type(), *to.mutable_message_type())) {
            copy_custom_options(message_from, message_to);
        }

        for (auto&& [enum_from, enum_to] :
             boost::combine(from.enum_type(), *to.mutable_enum_type())) {
            copy_custom_options(enum_from, enum_to);
        }

        for (auto&& [extension_from, extension_to] :
             boost::combine(from.extension(), *to.mutable_extension())) {
            copy_custom_options(extension_from, extension_to);
        }

        for (auto&& [service_from, service_to] :
             boost::combine(from.service(), *to.mutable_service())) {
            copy_custom_options(service_from, service_to);
        }
    }

    template<
      std::ranges::random_access_range Range,
      typename Comp = std::ranges::less,
      typename Proj = std::identity>
    const range_proxy<Range> maybe_sorted(
      const Range& range, Comp comp = Comp{}, Proj proj = Proj{}) const {
        if (!is_normalized) {
            return range_proxy<Range>{range};
        }
        std::decay_t<Range> copy = range;
        std::ranges::sort(copy, comp, proj);
        return range_proxy<Range>{std::move(copy)};
    }

    auto maybe_sorted_uninterpreted_options(
      const pb::RepeatedPtrField<pb::UninterpretedOption>& options) const {
        return maybe_sorted(
          options, std::less{}, [](const pb::UninterpretedOption& o) {
              return fmt::format("{}", o);
          });
    }

    void render_field(
      std::ostream& os,
      pb::Edition edition,
      const pb::FieldDescriptorProto& field,
      const pb::FieldDescriptor* descriptor,
      int indent,
      bool is_group = false) const {
        const auto type_name = [&]() -> std::string_view {
            if (field.has_type_name()) {
                std::string_view name{field.type_name()};
                if (is_group) {
                    // Remove the prefix, the type is always in the immediate
                    // scope.
                    size_t pos = name.find_last_of('.');
                    if (pos != std::string::npos) {
                        name = name.substr(pos + 1);
                    }
                }
                return name;
            }
            switch (field.type()) {
            case pb::FieldDescriptorProto::TYPE_DOUBLE:
                return "double";
            case pb::FieldDescriptorProto::TYPE_FLOAT:
                return "float";
            case pb::FieldDescriptorProto::TYPE_INT64:
                return "int64";
            case pb::FieldDescriptorProto::TYPE_UINT64:
                return "uint64";
            case pb::FieldDescriptorProto::TYPE_INT32:
                return "int32";
            case pb::FieldDescriptorProto::TYPE_FIXED64:
                return "fixed64";
            case pb::FieldDescriptorProto::TYPE_FIXED32:
                return "fixed32";
            case pb::FieldDescriptorProto::TYPE_BOOL:
                return "bool";
            case pb::FieldDescriptorProto::TYPE_STRING:
                return "string";
            case pb::FieldDescriptorProto::TYPE_GROUP:
                return "group";
            case pb::FieldDescriptorProto::TYPE_MESSAGE:
                return "message";
            case pb::FieldDescriptorProto::TYPE_BYTES:
                return "bytes";
            case pb::FieldDescriptorProto::TYPE_UINT32:
                return "uint32";
            case pb::FieldDescriptorProto::TYPE_ENUM:
                return "enum";
            case pb::FieldDescriptorProto::TYPE_SFIXED32:
                return "sfixed32";
            case pb::FieldDescriptorProto::TYPE_SFIXED64:
                return "sfixed64";
            case pb::FieldDescriptorProto::TYPE_SINT32:
                return "sint32";
            case pb::FieldDescriptorProto::TYPE_SINT64:
                return "sint64";
            }
            return "unknown";
        };

        const auto label = [&]() {
            bool is_proto2 = edition == pb::Edition::EDITION_PROTO2;
            if(descriptor && 
                (descriptor->is_map() || descriptor->real_containing_oneof() ||
                ((descriptor->is_optional() && !field.proto3_optional()) && !(is_proto2 && !descriptor->containing_oneof())))) {
                return "";
            }
            switch (field.label()) {
            case pb::FieldDescriptorProto::LABEL_OPTIONAL:
                return "optional ";
            case pb::FieldDescriptorProto::LABEL_REPEATED:
                return "repeated ";
            case pb::FieldDescriptorProto::LABEL_REQUIRED:
                return is_proto2 ? "required " : "";
            }
            return "";
        };

        if (descriptor && descriptor->is_map()) {
            const auto name_for =
              [&](const pb::FieldDescriptor* fd) -> std::string_view {
                switch (fd->type()) {
                case pb::FieldDescriptor::TYPE_GROUP:
                case pb::FieldDescriptor::TYPE_MESSAGE:
                    return fd->message_type()->full_name();
                case pb::FieldDescriptor::TYPE_ENUM:
                    return fd->enum_type()->full_name();
                default:
                    return fd->type_name();
                };
            };
            fmt::print(
              os,
              "{:{}}{}map<{}, {}> {} = {}",
              "",
              indent,
              label(),
              name_for(descriptor->message_type()->field(0)),
              name_for(descriptor->message_type()->field(1)),
              field.name(),
              field.number());
        } else {
            fmt::print(
              os,
              "{:{}}{}{} {} = {}",
              "",
              indent,
              label(),
              is_group ? "group" : type_name(),
              is_group ? type_name() : field.name(),
              field.number());
        }

        size_t count = [&field]() {
            return static_cast<size_t>(field.has_default_value()) +
                   static_cast<size_t>(field.has_json_name()) +
                   (field.has_options()
                     ? std::apply(
                         [&field](const auto&... field_option) {
                             return (
                               static_cast<size_t>(
                                 (field.options().*field_option.check)())
                               + ...);
                         },
                         field_options())
                         + field.options().uninterpreted_option_size()
                     : 0);
        }();

        bool first = true;
        auto maybe_print_seperator = [&]() {
            if (count > 1) {
                const auto prefix = first ? " [" : ",";
                fmt::print(os, "{}\n{:{}}", prefix, "", indent + 2);
                first = false;
            } else if (first) {
                fmt::print(os, " [");
            }
        };

        if (field.has_default_value()) {
            maybe_print_seperator();
            fmt::print(os, "default = {}", field.default_value());
        }
        if (field.has_json_name()) {
            maybe_print_seperator();
            fmt::print(
              os, "json_name = {}", pb_string_value(field.json_name()));
        }
        if (field.has_options()) {
            const auto& options = field.options();

            std::apply(
              [&](const auto&... field_option) {
                  (
                    [&](const auto& field_option) {
                        if ((options.*(field_option.check))()) {
                            maybe_print_seperator();
                            fmt::print(
                              os,
                              "{} = {}",
                              field_option.name,
                              (options.*(field_option.field))());
                        }
                    }(field_option),
                    ...);
              },
              field_options());

            auto uninterpreted_options = maybe_sorted_uninterpreted_options(
              options.uninterpreted_option());
            for (const auto& option : uninterpreted_options) {
                maybe_print_seperator();
                fmt::print(os, "{}", option);
            }
        }
        if (count > 1 && !first) {
            fmt::print(os, "\n{:{}}]", "", indent);
        } else if (count == 1) {
            fmt::print(os, "]");
        }
        if (is_group) {
            fmt::print(os, " {{\n");
        } else {
            fmt::print(os, ";\n");
        }
    }

    template<typename Descriptor>
    void render_extensions(
      std::ostream& os,
      pb::Edition edition,
      const pb::RepeatedPtrField<pb::FieldDescriptorProto>& raw_extensions,
      const Descriptor& descriptor,
      int indent) const {
        auto extensions = maybe_sorted(
          raw_extensions, std::less{}, [](const auto& extension) {
              return std::make_pair(extension.extendee(), extension.number());
          });

        const auto start_section = [indent, &os](std::string_view ext) {
            fmt::print(os, "{:{}}extend {} {{\n", "", indent, ext);
        };
        const auto close_section = [indent, &os]() {
            fmt::print(os, "{:{}}}}\n", "", indent);
        };

        std::string active_extendee{};
        bool open_section = false;
        for (const auto& extension : extensions) {
            auto d = descriptor.FindExtensionByName(extension.name());

            const auto& extendee = extension.extendee();
            if (active_extendee != extendee) {
                active_extendee = extendee;
                if (open_section) {
                    close_section();
                }
                start_section(extendee);
                open_section = true;
            }
            render_field(os, edition, extension, d, indent + 2);
        }
        if (open_section) {
            close_section();
        }
    }

    void render_extension_range(
      std::ostream& os,
      const pb::DescriptorProto_ExtensionRange& range,
      int indent) const {
        fmt::print(
          os,
          "{:{}}extensions {} to {}",
          "",
          indent,
          range.start(),
          range.end() - 1);

        if (range.has_options()) {
            const auto& options = range.options();
            size_t count = [&options]() {
                return static_cast<size_t>(options.declaration_size())
                       + static_cast<size_t>(options.has_verification())
                       + static_cast<size_t>(
                         options.uninterpreted_option_size());
            }();

            bool first = true;
            auto maybe_print_seperator = [&]() {
                if (count > 1) {
                    const auto prefix = first ? " [" : ",";
                    fmt::print(os, "{}\n{:{}}", prefix, "", indent + 2);
                    first = false;
                } else if (first) {
                    fmt::print(os, " [");
                }
            };

            const int decl_indent = count > 1 ? indent + 2 : indent;
            for (const auto& decl : options.declaration()) {
                maybe_print_seperator();
                render_declaration(os, decl, decl_indent);
            }
            if (options.has_verification()) {
                maybe_print_seperator();
                fmt::print(os, "verification = {}", options.verification());
            }
            auto uninterpreted_options = maybe_sorted_uninterpreted_options(
              options.uninterpreted_option());
            for (const auto& option : uninterpreted_options) {
                maybe_print_seperator();
                fmt::print(os, "{}", option);
            }

            if (count > 1 && !first) {
                fmt::print(os, "\n{:{}}]", "", indent);
            } else if (count == 1) {
                fmt::print(os, "]");
            }
        }

        fmt::print(os, ";\n");
    }

    void render_declaration(
      std::ostream& os,
      const pb::ExtensionRangeOptions_Declaration& decl,
      int indent) const {
        fmt::print(os, "declaration = {{");

        // declarations need to have at least 'full_name' and 'type'
        // set. This means that always "count >= 2".
        bool first = true;
        auto maybe_print_seperator = [&]() {
            const auto prefix = first ? "" : ",";
            fmt::print(os, "{}\n{:{}}", prefix, "", indent + 2);
            first = false;
        };

        if (decl.has_full_name()) {
            maybe_print_seperator();
            fmt::print(
              os, "{}: {}", "full_name", pb_string_value(decl.full_name()));
        }
        if (decl.has_type()) {
            maybe_print_seperator();
            fmt::print(os, "{}: {}", "type", pb_string_value(decl.type()));
        }
        if (decl.has_number()) {
            maybe_print_seperator();
            fmt::print(os, "{}: {}", "number", decl.number());
        }
        if (decl.has_reserved()) {
            maybe_print_seperator();
            fmt::print(os, "{}: {}", "reserved", decl.reserved());
        }
        if (decl.has_repeated()) {
            maybe_print_seperator();
            fmt::print(os, "{}: {}", "repeated", decl.repeated());
        }

        fmt::print(os, "}}");
    }

    // Render a message, including nested messages
    void render_nested(
      std::ostream& os,
      pb::Edition edition,
      const std::optional<pb::FieldDescriptorProto>& field,
      const pb::FieldDescriptor* field_descriptor,
      const pb::DescriptorProto& message,
      const pb::Descriptor* descriptor,
      int indent) const {
        auto type = field.has_value() ? field->type()
                                      : pb::FieldDescriptorProto::TYPE_MESSAGE;
        if (type == pb::FieldDescriptorProto::TYPE_GROUP) {
            bool is_group = true;
            render_field(
              os, edition, *field, field_descriptor, indent, is_group);
        } else {
            fmt::print(os, "{:{}}message {} {{\n", "", indent, message.name());
        }

        if (message.has_options()) {
            if (message.options().has_deprecated()) {
                fmt::print(
                  os,
                  "{:{}}option deprecated = {};\n",
                  "",
                  indent + 2,
                  message.options().deprecated());
            }
            if (message.options().has_message_set_wire_format()) {
                fmt::print(
                  os,
                  "{:{}}option message_set_wire_format = {};\n",
                  "",
                  indent + 2,
                  message.options().message_set_wire_format());
            }
            if (message.options().has_no_standard_descriptor_accessor()) {
                fmt::print(
                  os,
                  "{:{}}option no_standard_descriptor_accessor = {};\n",
                  "",
                  indent + 2,
                  message.options().no_standard_descriptor_accessor());
            }
        }
        auto uninterepreted_options = maybe_sorted_uninterpreted_options(
          message.options().uninterpreted_option());
        for (const auto& option : uninterepreted_options) {
            fmt::print(os, "{:{}}option {};\n", "", indent + 2, option);
        }

        auto reserved_range = maybe_sorted(
          message.reserved_range(),
          std::less{},
          &pb::DescriptorProto_ReservedRange::start);
        for (const auto& value : reserved_range) {
            fmt::print(os, "{:{}}reserved {}", "", indent + 2, value.start());
            if (value.has_end() && value.end() != value.start() + 1) {
                fmt::print(os, " to {}", value.end() - 1);
            }
            fmt::print(os, ";\n");
        }
        auto reserved_names = maybe_sorted(message.reserved_name());
        if (!reserved_names.empty()) {
            const auto to_debug_string = [](const std::string_view strv) {
                return fmt::format("{}", pb_string_value(strv));
            };
            fmt::print(
              os,
              "{:{}}reserved {};\n",
              "",
              indent + 2,
              fmt::join(
                reserved_names | std::views::transform(to_debug_string), ", "));
        }
        if (!reserved_range.empty() || !reserved_names.empty()) {
            fmt::print(os, "\n");
        }

        bool has_fields = false;
        auto fields_ = maybe_sorted(
          message.field(),
          std::ranges::less{},
          &pb::FieldDescriptorProto::number);
        auto fields = std::views::filter(fields_, [](const auto& f) {
            return f.type() != pb::FieldDescriptorProto::TYPE_GROUP;
        });

        // Each oneof section needs to start with the lowest field number, which
        // may be different to the order of oneof_decl in the message.
        std::vector<int> oneofs;

        // Render non oneof fields, and record correct order of oneof indices.
        for (const auto& field : fields) {
            if (
              field.has_oneof_index()
              && !(field.has_proto3_optional() && field.proto3_optional())) {
                bool has_oneof = std::ranges::find(oneofs, field.oneof_index())
                                 != oneofs.end();
                if (!has_oneof) {
                    oneofs.push_back(field.oneof_index());
                }
            } else {
                has_fields = true;
                auto d = descriptor->FindFieldByName(field.name());
                render_field(os, edition, field, d, indent + 2);
            }
        }
        if (has_fields && !oneofs.empty()) {
            fmt::print(os, "\n");
        }
        // Render oneof fields
        for (const int i : oneofs) {
            const auto& decl = message.oneof_decl(i);
            fmt::print(os, "{:{}}oneof {} {{\n", "", indent + 2, decl.name());
            auto uninterpreted_options = maybe_sorted_uninterpreted_options(
              decl.options().uninterpreted_option());
            for (const auto& option : uninterpreted_options) {
                fmt::print(os, "{:{}}option {};\n", "", indent + 4, option);
            }
            auto fields = maybe_sorted(
              message.field(),
              std::ranges::less{},
              &pb::FieldDescriptorProto::number);
            for (const auto& field : fields) {
                if (field.has_oneof_index() && field.oneof_index() == i) {
                    auto d = descriptor->FindFieldByName(field.name());
                    render_field(os, edition, field, d, indent + 4);
                }
            }
            fmt::print(os, "{:{}}}}\n", "", indent + 2);
        }

        // Render extension ranges
        for (const auto& range : message.extension_range()) {
            render_extension_range(os, range, indent + 2);
        }

        render_extensions(
          os, edition, message.extension(), *descriptor, indent + 2);

        auto nested_messages = std::views::filter(
          message.nested_type(),
          [](const auto& m) { return !m.options().has_map_entry(); });
        if (!nested_messages.empty() || !message.enum_type().empty()) {
            fmt::print(os, "\n");
        }

        // Render nested types
        for (const auto& nested : nested_messages) {
            auto it = std::ranges::find_if(
              message.field(), [&nested](const auto& f) {
                  return f.name() == absl::AsciiStrToLower(nested.name());
              });

            auto field = (it != message.field().end())
                           ? std::optional<pb::FieldDescriptorProto>{*it}
                           : std::nullopt;
            render_nested(
              os,
              edition,
              field,
              field_descriptor,
              nested,
              descriptor->FindNestedTypeByName(nested.name()),
              indent + 2);
        }

        // Render nested enums
        for (const auto& nested : message.enum_type()) {
            render_enum(os, nested, indent + 2);
        }

        fmt::print(os, "{:{}}}}\n", "", indent);
    }

    // Render an enum
    void render_enum(
      std::ostream& os,
      const pb::EnumDescriptorProto& enum_proto,
      int indent) const {
        fmt::print(os, "{:{}}enum {} {{\n", "", indent, enum_proto.name());
        auto reserved_range = maybe_sorted(
          enum_proto.reserved_range(),
          std::less{},
          &pb::EnumDescriptorProto_EnumReservedRange::start);
        for (const auto& value : reserved_range) {
            fmt::print(os, "{:{}}reserved {}", "", indent + 2, value.start());
            if (value.has_end() && value.end() != value.start()) {
                fmt::print(os, " to {}", value.end());
            }
            fmt::print(os, ";\n");
        }
        for (const auto& value : maybe_sorted(enum_proto.reserved_name())) {
            fmt::print(
              os,
              "{:{}}reserved {};\n",
              "",
              indent + 2,
              pb_string_value(value));
        }
        if (enum_proto.options().has_allow_alias()) {
            fmt::print(
              os,
              "{:{}}option allow_alias = {};\n",
              "",
              indent + 2,
              enum_proto.options().allow_alias());
        }
        if (enum_proto.options().has_deprecated()) {
            fmt::print(
              os,
              "{:{}}option deprecated = {};\n",
              "",
              indent + 2,
              enum_proto.options().deprecated());
        }
        auto uninterpreted_options = maybe_sorted_uninterpreted_options(
          enum_proto.options().uninterpreted_option());
        for (const auto& option : uninterpreted_options) {
            fmt::print(os, "{:{}}option {};\n", "", indent + 2, option);
        }
        const auto values = maybe_sorted(
          enum_proto.value(), std::less{}, [](const auto& v) {
              // In proto3, enums are open and open enums need to
              // have the first field being equal to zero. By casting
              // to an unsigned integer for sorting, all the negative
              // fields will be at the end, after all the positives.
              return std::pair<uint32_t, std::string_view>{
                static_cast<uint32_t>(v.number()), v.name()};
          });
        for (const auto& value : values) {
            fmt::print(
              os, "{:{}}{} = {}", "", indent + 2, value.name(), value.number());
            if (value.has_options()) {
                fmt::print(os, " [");
                bool first_option = true;
                const auto maybe_print_comma = [&]() {
                    if (!first_option) {
                        fmt::print(os, ", ");
                    }
                    first_option = false;
                };
                if (value.options().has_deprecated()) {
                    maybe_print_comma();
                    fmt::print(
                      os, "deprecated = {}", value.options().deprecated());
                }
                if (value.options().has_debug_redact()) {
                    maybe_print_comma();
                    fmt::print(
                      os, "debug_redact = {}", value.options().debug_redact());
                }
                if (!value.options().uninterpreted_option().empty()) {
                    maybe_print_comma();
                    auto uninterpreted_options
                      = maybe_sorted_uninterpreted_options(
                        value.options().uninterpreted_option());
                    fmt::print(
                      os, "{}", fmt::join(uninterpreted_options, ", "));
                }
                fmt::print(os, "]");
            }
            fmt::print(os, ";\n");
        }
        fmt::print(os, "{:{}}}}\n", "", indent);
    }

    // Render a service and its RPC methods
    void render_service(
      std::ostream& os,
      const pb::ServiceDescriptorProto& service,
      int indent) const {
        fmt::print(os, "{:{}}service {} {{\n", "", indent, service.name());
        if (service.has_options()) {
            if (service.options().has_deprecated()) {
                fmt::print(
                  os,
                  "{:{}}option deprecated = {};\n",
                  "",
                  indent + 2,
                  service.options().deprecated());
            }
            auto uninterpreted_options = maybe_sorted_uninterpreted_options(
              service.options().uninterpreted_option());
            for (const auto& option : uninterpreted_options) {
                fmt::print(os, "{:{}}option {};\n", "", indent + 2, option);
            }
        }
        for (const auto& method : service.method()) {
            fmt::print(
              os,
              "{:{}}rpc {} ({}{}) returns ({}{})",
              "",
              indent + 2,
              method.name(),
              method.client_streaming() ? "stream " : "",
              method.input_type(),
              method.server_streaming() ? "stream " : "",
              method.output_type());
            if (method.has_options()) {
                fmt::print(os, " {{\n");
                if (method.options().has_deprecated()) {
                    fmt::print(
                      os,
                      "{:{}}option deprecated = {};\n",
                      "",
                      indent + 4,
                      method.options().deprecated());
                }
                if (method.options().has_idempotency_level()) {
                    fmt::print(
                      os,
                      "{:{}}option idempotency_level = {};\n",
                      "",
                      indent + 4,
                      pb::MethodOptions_IdempotencyLevel_Name(
                        method.options().idempotency_level()));
                }
                auto uninterpreted_options = maybe_sorted_uninterpreted_options(
                  method.options().uninterpreted_option());
                for (const auto& option : uninterpreted_options) {
                    fmt::print(os, "{:{}}option {};\n", "", indent + 4, option);
                }
                fmt::print(os, "{:{}}}}\n", "", indent + 2);
            } else {
                fmt::print(os, ";\n");
            }
        }
        fmt::print(os, "{:{}}}}\n", "", indent);
    }

    // Render the FileOptions (if any are set)
    void render_file_options(
      const pb::FileOptions& options, std::ostream& os) const {
        bool first_option = true;
        auto printv = [&](std::string_view name, const auto& val) {
            fmt::print(os, "option {} = {};\n", name, val);
            first_option = false;
        };
        auto prints = [&](std::string_view name, const auto& val) {
            fmt::print(os, "option {} = {};\n", name, pb_string_value(val));
            first_option = false;
        };
        if (options.has_cc_enable_arenas()) {
            printv("cc_enable_arenas", options.cc_enable_arenas());
        }
        if (options.has_cc_generic_services()) {
            printv("cc_generic_services", options.cc_generic_services());
        }
        if (options.has_csharp_namespace()) {
            prints("csharp_namespace", options.csharp_namespace());
        }
        if (options.has_deprecated()) {
            printv("deprecated", options.deprecated());
        }
        if (options.has_go_package()) {
            prints("go_package", options.go_package());
        }
        if (options.has_java_generic_services()) {
            printv("java_generic_services", options.java_generic_services());
        }
        if (options.has_java_multiple_files()) {
            printv("java_multiple_files", options.java_multiple_files());
        }
        if (options.has_java_outer_classname()) {
            prints("java_outer_classname", options.java_outer_classname());
        }
        if (options.has_java_package()) {
            prints("java_package", options.java_package());
        }
        if (options.has_java_string_check_utf8()) {
            printv("java_string_check_utf8", options.java_string_check_utf8());
        }
        if (options.has_objc_class_prefix()) {
            prints("objc_class_prefix", options.objc_class_prefix());
        }
        if (options.has_optimize_for()) {
            printv(
              "optimize_for",
              FileOptions_OptimizeMode_Name(options.optimize_for()));
        }
        if (options.has_ruby_package()) {
            prints("ruby_package", options.ruby_package());
        }
        if (options.has_swift_prefix()) {
            prints("swift_prefix", options.swift_prefix());
        }
        if (options.has_php_class_prefix()) {
            prints("php_class_prefix", options.php_class_prefix());
        }
        if (options.has_php_metadata_namespace()) {
            prints("php_metadata_namespace", options.php_metadata_namespace());
        }
        if (options.has_php_namespace()) {
            prints("php_namespace", options.php_namespace());
        }
        if (options.has_py_generic_services()) {
            printv("py_generic_services", options.py_generic_services());
        }
        auto uninterpreted_options = maybe_sorted_uninterpreted_options(
          options.uninterpreted_option());
        for (const auto& option : uninterpreted_options) {
            first_option = false;
            fmt::print(os, "option {};\n", option);
        }

        if (!first_option) {
            fmt::print(os, "\n");
        }
    }

    void
    render_imports(std::ostream& os, const pb::FileDescriptorProto& fdp) const {
        std::vector<std::string_view> all_deps{
          fdp.dependency().begin(), fdp.dependency().end()};

        auto is_public = [&](const auto& dep) {
            return std::ranges::any_of(fdp.public_dependency(), [&](int j) {
                return fdp.dependency()[j] == dep;
            });
        };
        auto is_weak = [&](const auto& dep) {
            return std::ranges::any_of(fdp.weak_dependency(), [&](int j) {
                return fdp.dependency()[j] == dep;
            });
        };

        // return a range that matches the predicate
        constexpr auto partition =
          [](auto begin, auto end, auto pred, normalize norm) {
              return std::ranges::subrange(
                begin,
                norm ? std::partition(begin, end, pred)
                     : std::stable_partition(begin, end, pred));
          };

        auto public_deps = partition(
          all_deps.begin(), all_deps.end(), is_public, is_normalized);
        auto weak_deps = partition(
          public_deps.end(), all_deps.end(), is_weak, is_normalized);
        auto private_deps = std::ranges::subrange(
          weak_deps.end(), all_deps.end());

        if (is_normalized) {
            constexpr auto sort_and_unique = [](auto& deps) {
                std::ranges::sort(deps);
                deps = std::ranges::subrange(
                  deps.begin(), std::ranges::unique(deps).begin());
            };

            sort_and_unique(weak_deps);
            sort_and_unique(private_deps);
            sort_and_unique(public_deps);
        }

        auto print_deps = [&](const auto& view, std::string_view type) {
            for (const auto& dep : view) {
                fmt::print(os, "import {}{};\n", type, pb_string_value(dep));
            }
        };

        print_deps(private_deps, "");
        print_deps(weak_deps, "weak ");
        print_deps(public_deps, "public ");
    }

    void render_proto(
      std::ostream& os,
      const pb::FileDescriptorProto& fdp,
      const pb::FileDescriptor& descriptor) const {
        auto edition = fdp.edition();
        if (edition == pb::Edition::EDITION_UNKNOWN) {
            auto syntax = fdp.has_syntax() ? fdp.syntax() : "proto2";
            edition = syntax == "proto3" ? pb::Edition::EDITION_PROTO3
                                         : pb::Edition::EDITION_PROTO2;
            fmt::print(os, "syntax = {};\n", pb_string_value(syntax));
        } else {
            fmt::print(
              os,
              "edition = {};\n",
              pb_string_value(Edition_Name(fdp.edition())));
        }

        if (fdp.has_package() && !fdp.package().empty()) {
            fmt::print(os, "package {};\n", fdp.package());
        }
        fmt::print(os, "\n");

        if (!fdp.dependency().empty()) {
            render_imports(os, fdp);
            fmt::print(os, "\n");
        }

        render_file_options(fdp.options(), os);

        // Render messages
        for (const auto& message : fdp.message_type()) {
            auto d = descriptor.FindMessageTypeByName(message.name());
            render_nested(os, edition, std::nullopt, nullptr, message, d, 0);
        }

        // Render enums
        for (const auto& enum_proto : fdp.enum_type()) {
            render_enum(os, enum_proto, 0);
        }

        render_extensions(os, edition, fdp.extension(), descriptor, 0);

        if ((fdp.message_type_size() + fdp.enum_type_size()) != 0) {
            fmt::print(os, "\n");
        }

        // Render services
        for (const auto& service : fdp.service()) {
            render_service(os, service, 0);
            fmt::print(os, "\n");
        }
    }
};

canonical_schema_definition::raw_string
protobuf_schema_definition::raw() const {
    return _impl->raw();
}

::result<ss::sstring, kafka::error_code>
protobuf_schema_definition::name(std::vector<int> const& fields) const {
    if (fields.empty()) {
        return kafka::error_code::invalid_record;
    }
    auto f = fields.begin();
    auto d = _impl->fd->message_type(*f++);
    while (fields.end() != f && d) {
        d = d->nested_type(*f++);
    }
    if (!d) {
        return kafka::error_code::invalid_record;
    }
    return d->full_name();
}

bool operator==(
  const protobuf_schema_definition& lhs,
  const protobuf_schema_definition& rhs) {
    return lhs.raw() == rhs.raw();
}

std::ostream&
operator<<(std::ostream& os, const protobuf_schema_definition& def) {
    fmt::print(
      os, "type: {}, definition: {}", to_string_view(def.type()), def.raw()());
    return os;
}

ss::future<protobuf_schema_definition> make_protobuf_schema_definition(
  schema_getter& store,
  canonical_schema schema,
  normalize norm,
  bool allow_v2_renderer) {
    auto refs = schema.def().refs();
    auto impl = ss::make_shared<protobuf_schema_definition::impl>();
    auto subj = schema.sub();
    impl->fdp = co_await import_schema(impl->_dp, store, std::move(schema));
    impl->fd = impl->_dp.FindFileByName(impl->fdp.name());
    if (allow_v2_renderer) {
        if (auto* s = dynamic_cast<const sharded_store*>(&store);
            s != nullptr) {
            impl->v2_renderer = s->protobuf_v2_renderer();
        }
    }
    impl->is_normalized = norm;
    if (norm) {
        std::sort(refs.begin(), refs.end());
        auto uniq = std::ranges::unique(refs);
        refs.erase(uniq.begin(), uniq.end());
    }
    const auto ps = protobuf_schema_definition{
      std::move(impl), std::move(refs)};
    if (!allow_v2_renderer) {
        co_return ps;
    }
    canonical_schema cs = canonical_schema{
      subj, {ps.raw(), schema_type::protobuf, ps.refs()}};
    co_return co_await make_protobuf_schema_definition(
      store, std::move(cs), norm, false);
}

ss::future<canonical_schema_definition> validate_protobuf_schema(
  sharded_store& store, canonical_schema schema, normalize norm) {
    auto res = co_await make_protobuf_schema_definition(
      store, std::move(schema), norm);
    co_return canonical_schema_definition{std::move(res)};
}

ss::future<canonical_schema> make_canonical_protobuf_schema(
  sharded_store& store, unparsed_schema schema, normalize norm) {
    auto [sub, unparsed] = std::move(schema).destructure();
    auto [def, type, refs] = std::move(unparsed).destructure();
    canonical_schema temp{
      sub,
      {canonical_schema_definition::raw_string{std::move(def)()},
       type,
       std::move(refs)}};

    co_return canonical_schema{
      std::move(sub),
      co_await validate_protobuf_schema(store, std::move(temp), norm)};
}

namespace {

enum class encoding {
    struct_ = 0,
    varint,
    zigzag,
    bytes,
    int32,
    int64,
    float_,
    double_,
};

encoding get_encoding(pb::FieldDescriptor::Type type) {
    switch (type) {
    case pb::FieldDescriptor::Type::TYPE_MESSAGE:
    case pb::FieldDescriptor::Type::TYPE_GROUP:
        return encoding::struct_;
    case pb::FieldDescriptor::Type::TYPE_FLOAT:
        return encoding::float_;
    case pb::FieldDescriptor::Type::TYPE_DOUBLE:
        return encoding::double_;
    case pb::FieldDescriptor::Type::TYPE_INT64:
    case pb::FieldDescriptor::Type::TYPE_UINT64:
    case pb::FieldDescriptor::Type::TYPE_INT32:
    case pb::FieldDescriptor::Type::TYPE_UINT32:
    case pb::FieldDescriptor::Type::TYPE_BOOL:
    case pb::FieldDescriptor::Type::TYPE_ENUM:
        return encoding::varint;
    case pb::FieldDescriptor::Type::TYPE_SINT32:
    case pb::FieldDescriptor::Type::TYPE_SINT64:
        return encoding::zigzag;
    case pb::FieldDescriptor::Type::TYPE_STRING:
    case pb::FieldDescriptor::Type::TYPE_BYTES:
        return encoding::bytes;
    case pb::FieldDescriptor::Type::TYPE_FIXED32:
    case pb::FieldDescriptor::Type::TYPE_SFIXED32:
        return encoding::int32;
    case pb::FieldDescriptor::Type::TYPE_FIXED64:
    case pb::FieldDescriptor::Type::TYPE_SFIXED64:
        return encoding::int64;
    }
    __builtin_unreachable();
}

using proto_compatibility_result = raw_compatibility_result;

struct compatibility_checker {
    proto_compatibility_result check_compatible(std::filesystem::path p) {
        return check_compatible(_writer.fd, std::move(p));
    }

    proto_compatibility_result check_compatible(
      const pb::FileDescriptor* writer, std::filesystem::path p) {
        // There must be a compatible reader message for every writer message
        proto_compatibility_result compat_result;
        for (int i = 0; i < writer->message_type_count(); ++i) {
            auto w = writer->message_type(i);
            auto r = _reader._dp.FindMessageTypeByName(w->full_name());

            if (!r) {
                compat_result.emplace<proto_incompatibility>(
                  p / w->name(), proto_incompatibility::Type::message_removed);
            } else {
                compat_result.merge(check_compatible(r, w, p / w->name()));
            }
        }
        return compat_result;
    }

    proto_compatibility_result check_compatible(
      const pb::Descriptor* reader,
      const pb::Descriptor* writer,
      std::filesystem::path p) {
        proto_compatibility_result compat_result;
        if (!_seen_descriptors.insert(reader).second) {
            return compat_result;
        }

        for (int i = 0; i < writer->nested_type_count(); ++i) {
            auto w = writer->nested_type(i);
            auto r = reader->FindNestedTypeByName(w->name());
            if (!r) {
                compat_result.emplace<proto_incompatibility>(
                  p / w->name(), proto_incompatibility::Type::message_removed);
            } else {
                compat_result.merge(check_compatible(r, w, p / w->name()));
            }
        }

        for (int i = 0; i < writer->real_oneof_decl_count(); ++i) {
            auto w = writer->oneof_decl(i);
            compat_result.merge(check_compatible(reader, w, p / w->name()));
        }

        for (int i = 0; i < reader->real_oneof_decl_count(); ++i) {
            auto r = reader->oneof_decl(i);
            compat_result.merge(check_compatible(r, writer, p / r->name()));
        }

        // check writer fields
        for (int i = 0; i < writer->field_count(); ++i) {
            auto w = writer->field(i);
            int number = w->number();
            auto r = reader->FindFieldByNumber(number);
            // A reader may ignore a writer field iff it is not `required`
            if (!r && w->is_required()) {
                compat_result.emplace<proto_incompatibility>(
                  p / std::to_string(w->number()),
                  proto_incompatibility::Type::required_field_removed);
            } else if (r) {
                auto oneof = r->containing_oneof();
                compat_result.merge(check_compatible(
                  r,
                  w,
                  p / (oneof ? oneof->name() : "")
                    / std::to_string(w->number())));
            }
        }

        // check reader required fields
        for (int i = 0; i < reader->field_count(); ++i) {
            auto r = reader->field(i);
            int number = r->number();
            auto w = writer->FindFieldByNumber(number);
            // A writer may ignore a reader field iff it is not `required`
            if ((!w || !w->is_required()) && r->is_required()) {
                compat_result.emplace<proto_incompatibility>(
                  p / std::to_string(number),
                  proto_incompatibility::Type::required_field_added);
            }
        }
        return compat_result;
    }

    proto_compatibility_result check_compatible(
      const pb::Descriptor* reader,
      const pb::OneofDescriptor* writer,
      std::filesystem::path p) {
        proto_compatibility_result compat_result;

        // If the oneof in question doesn't appear in the reader descriptor,
        // then we don't need to account for any difference in fields.
        if (!reader->FindOneofByName(writer->name())) {
            return compat_result;
        }

        for (int i = 0; i < writer->field_count(); ++i) {
            auto w = writer->field(i);
            auto r = reader->FindFieldByNumber(w->number());

            if (!r || !r->real_containing_oneof()) {
                compat_result.emplace<proto_incompatibility>(
                  p / std::to_string(w->number()),
                  proto_incompatibility::Type::oneof_field_removed);
            }
        }
        return compat_result;
    }

    proto_compatibility_result check_compatible(
      const pb::OneofDescriptor* reader,
      const pb::Descriptor* writer,
      std::filesystem::path p) {
        proto_compatibility_result compat_result;

        size_t count = 0;
        for (int i = 0; i < reader->field_count(); ++i) {
            auto r = reader->field(i);
            auto w = writer->FindFieldByNumber(r->number());
            if (w && !w->real_containing_oneof()) {
                ++count;
            }
        }
        if (count > 1) {
            compat_result.emplace<proto_incompatibility>(
              std::move(p),
              proto_incompatibility::Type::multiple_fields_moved_to_oneof);
        }
        return compat_result;
    }

    proto_compatibility_result check_compatible(
      const pb::FieldDescriptor* reader,
      const pb::FieldDescriptor* writer,
      std::filesystem::path p) {
        proto_compatibility_result compat_result;
        switch (writer->type()) {
        case pb::FieldDescriptor::Type::TYPE_MESSAGE:
        case pb::FieldDescriptor::Type::TYPE_GROUP: {
            bool type_is_compat = reader->type()
                                    == pb::FieldDescriptor::Type::TYPE_MESSAGE
                                  || reader->type()
                                       == pb::FieldDescriptor::Type::TYPE_GROUP;
            if (!type_is_compat) {
                compat_result.emplace<proto_incompatibility>(
                  std::move(p),
                  proto_incompatibility::Type::field_kind_changed);
            } else if (
              reader->message_type()->name()
              != writer->message_type()->name()) {
                compat_result.emplace<proto_incompatibility>(
                  std::move(p),
                  proto_incompatibility::Type::field_named_type_changed);
            } else {
                compat_result.merge(check_compatible(
                  reader->message_type(),
                  writer->message_type(),
                  std::move(p)));
            }
            break;
        }
        case pb::FieldDescriptor::Type::TYPE_FLOAT:
        case pb::FieldDescriptor::Type::TYPE_DOUBLE:
        case pb::FieldDescriptor::Type::TYPE_INT64:
        case pb::FieldDescriptor::Type::TYPE_UINT64:
        case pb::FieldDescriptor::Type::TYPE_INT32:
        case pb::FieldDescriptor::Type::TYPE_UINT32:
        case pb::FieldDescriptor::Type::TYPE_BOOL:
        case pb::FieldDescriptor::Type::TYPE_ENUM:
        case pb::FieldDescriptor::Type::TYPE_SINT32:
        case pb::FieldDescriptor::Type::TYPE_SINT64:
        case pb::FieldDescriptor::Type::TYPE_STRING:
        case pb::FieldDescriptor::Type::TYPE_BYTES:
        case pb::FieldDescriptor::Type::TYPE_FIXED32:
        case pb::FieldDescriptor::Type::TYPE_SFIXED32:
        case pb::FieldDescriptor::Type::TYPE_FIXED64:
        case pb::FieldDescriptor::Type::TYPE_SFIXED64:
            compat_result.merge(check_compatible(
              get_encoding(reader->type()),
              get_encoding(writer->type()),
              std::move(p)));
        }
        return compat_result;
    }

    proto_compatibility_result check_compatible(
      encoding reader, encoding writer, std::filesystem::path p) {
        proto_compatibility_result compat_result;
        // we know writer has scalar encoding because of the switch stmt above
        if (reader == encoding::struct_) {
            compat_result.emplace<proto_incompatibility>(
              std::move(p), proto_incompatibility::Type::field_kind_changed);
        } else if (reader != writer) {
            compat_result.emplace<proto_incompatibility>(
              std::move(p),
              proto_incompatibility::Type::field_scalar_kind_changed);
        }
        return compat_result;
    }

    const protobuf_schema_definition::impl& _reader;
    const protobuf_schema_definition::impl& _writer;
    std::unordered_set<const pb::Descriptor*> _seen_descriptors;
};

} // namespace

compatibility_result check_compatible(
  const protobuf_schema_definition& reader,
  const protobuf_schema_definition& writer,
  verbose is_verbose) {
    compatibility_checker checker{reader(), writer()};
    return checker.check_compatible("#/")(is_verbose);
}

} // namespace pandaproxy::schema_registry

template<>
struct fmt::formatter<pandaproxy::schema_registry::io_error_collector::err> {
    using type = pandaproxy::schema_registry::io_error_collector;

    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

    template<typename FormatContext>
    auto format(const type::err& e, FormatContext& ctx) const {
        return fmt::format_to(
          ctx.out(),
          "{}: line: '{}', col: '{}', msg: '{}'",
          e.lvl == type::level::error ? "error" : "warn",
          e.line,
          e.column,
          e.message);
    }
};

template<>
struct fmt::formatter<pandaproxy::schema_registry::dp_error_collector::err> {
    using type = pandaproxy::schema_registry::dp_error_collector;

    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

    template<typename FormatContext>
    auto format(const type::err& e, FormatContext& ctx) const {
        return fmt::format_to(
          ctx.out(),
          "{}: subject: '{}', element_name: '{}', descriptor: '{}', location: "
          "'{}', msg: '{}'",
          e.lvl == type::level::error ? "error" : "warn",
          e.filename,
          e.element_name,
          e.descriptor->DebugString(),
          e.location,
          e.message);
    }
};

namespace pandaproxy::schema_registry {

error_info io_error_collector::error() const {
    return error_info{
      error_code::schema_invalid, fmt::format("{}", fmt::join(_errors, "; "))};
}

error_info dp_error_collector::error(std::string_view sub) const {
    return error_info{
      error_code::schema_invalid,
      fmt::format("{}:{}", sub, fmt::join(_errors, "; "))};
}

} // namespace pandaproxy::schema_registry
