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
#include "src/v/pandaproxy/schema_registry/protobuf/confluent/meta.pb.h"
#include "src/v/pandaproxy/schema_registry/protobuf/confluent/types/decimal.pb.h"
#include "src/v/pandaproxy/schema_registry/protobuf/google/type/calendar_period.pb.h"
#include "src/v/pandaproxy/schema_registry/protobuf/google/type/color.pb.h"
#include "src/v/pandaproxy/schema_registry/protobuf/google/type/date.pb.h"
#include "src/v/pandaproxy/schema_registry/protobuf/google/type/datetime.pb.h"
#include "src/v/pandaproxy/schema_registry/protobuf/google/type/dayofweek.pb.h"
#include "src/v/pandaproxy/schema_registry/protobuf/google/type/decimal.pb.h"
#include "src/v/pandaproxy/schema_registry/protobuf/google/type/expr.pb.h"
#include "src/v/pandaproxy/schema_registry/protobuf/google/type/fraction.pb.h"
#include "src/v/pandaproxy/schema_registry/protobuf/google/type/interval.pb.h"
#include "src/v/pandaproxy/schema_registry/protobuf/google/type/latlng.pb.h"
#include "src/v/pandaproxy/schema_registry/protobuf/google/type/localized_text.pb.h"
#include "src/v/pandaproxy/schema_registry/protobuf/google/type/money.pb.h"
#include "src/v/pandaproxy/schema_registry/protobuf/google/type/month.pb.h"
#include "src/v/pandaproxy/schema_registry/protobuf/google/type/phone_number.pb.h"
#include "src/v/pandaproxy/schema_registry/protobuf/google/type/postal_address.pb.h"
#include "src/v/pandaproxy/schema_registry/protobuf/google/type/quaternion.pb.h"
#include "src/v/pandaproxy/schema_registry/protobuf/google/type/timeofday.pb.h"
#include "ssx/sformat.h"
#include "thirdparty/protobuf/descriptor.h"
#include "thirdparty/protobuf/descriptor.pb.h"
#include "utils/base64.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/sstring.hh>

#include <absl/container/flat_hash_set.h>
#include <boost/algorithm/string/trim.hpp>
#include <fmt/core.h>
#include <fmt/ostream.h>
#include <google/protobuf/any.pb.h>
#include <google/protobuf/api.pb.h>
#include <google/protobuf/compiler/parser.h>
#include <google/protobuf/duration.pb.h>
#include <google/protobuf/empty.pb.h>
#include <google/protobuf/field_mask.pb.h>
#include <google/protobuf/io/tokenizer.h>
#include <google/protobuf/io/zero_copy_stream.h>
#include <google/protobuf/source_context.pb.h>
#include <google/protobuf/struct.pb.h>
#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/type.pb.h>
#include <google/protobuf/wrappers.pb.h>

#include <string_view>
#include <unordered_set>

namespace pandaproxy::schema_registry {

namespace pb = google::protobuf;

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
#if PROTOBUF_VERSION < 5027000
    void AddError(int line, int column, const std::string& message) final {
        _errors.emplace_back(err{level::error, line, column, message});
    }
    void AddWarning(int line, int column, const std::string& message) final {
        _errors.emplace_back(err{level::warn, line, column, message});
    }
#else
    void RecordError(int line, int column, std::string_view message) final {
        _errors.emplace_back(
          err{level::error, line, column, ss::sstring{message}});
    }
    void RecordWarning(int line, int column, std::string_view message) final {
        _errors.emplace_back(
          err{level::warn, line, column, ss::sstring{message}});
    }
#endif

    error_info error() const;

private:
    friend struct fmt::formatter<err>;

    std::vector<err> _errors;
};

class dp_error_collector final : public pb::DescriptorPool::ErrorCollector {
public:
#if PROTOBUF_VERSION < 5027000
    void AddError(
      const std::string& filename,
      const std::string& element_name,
      const pb::Message* descriptor,
      ErrorLocation location,
      const std::string& message) final {
        _errors.emplace_back(err{
          level::error,
          ss::sstring{filename},
          ss::sstring{element_name},
          descriptor,
          location,
          ss::sstring {
              message
          }});
    }

    void AddWarning(
      const std::string& filename,
      const std::string& element_name,
      const pb::Message* descriptor,
      ErrorLocation location,
      const std::string& message) final {
        _errors.emplace_back(err{
          level::warn,
          ss::sstring{filename},
          ss::sstring{element_name},
          descriptor,
          location,
          ss::sstring {
              message
          }});
    }
#else
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
#endif

    error_info error() const;

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
#if PROTOBUF_VERSION < 5027000
        _fdp.set_name(sub.data(), sub.size());
#else
        _fdp.set_name(std::string_view(sub));
#endif
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
    throw as_exception(dp_ec.error());
}

///\brief Build a FileDescriptor and import references from the store.
///
/// Recursively import references into the DescriptorPool, building the
/// files on stack unwind.
ss::future<const pb::FileDescriptor*> build_file_with_refs(
  pb::DescriptorPool& dp, sharded_store& store, canonical_schema schema) {
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
    co_return build_file(dp, p.parse(schema));
}

///\brief Import a schema in the DescriptorPool and return the
/// FileDescriptor.
ss::future<const pb::FileDescriptor*> import_schema(
  pb::DescriptorPool& dp, sharded_store& store, canonical_schema schema) {
    try {
        co_return co_await build_file_with_refs(dp, store, schema.share());
    } catch (const exception& e) {
        vlog(plog.warn, "Failed to decode schema: {}", e.what());
        throw as_exception(invalid_schema(schema));
    }
}

struct protobuf_schema_definition::impl {
    pb::DescriptorPool _dp;
    const pb::FileDescriptor* fd{};

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
};

canonical_schema_definition::raw_string
protobuf_schema_definition::raw() const {
    return canonical_schema_definition::raw_string{_impl->debug_string()};
}

::result<ss::sstring, kafka::error_code>
protobuf_schema_definition::name(const std::vector<int>& fields) const {
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

ss::future<protobuf_schema_definition>
make_protobuf_schema_definition(sharded_store& store, canonical_schema schema) {
    auto impl = ss::make_shared<protobuf_schema_definition::impl>();
    auto refs = schema.def().refs();
    impl->fd = co_await import_schema(impl->_dp, store, std::move(schema));
    co_return protobuf_schema_definition{std::move(impl), std::move(refs)};
}

ss::future<canonical_schema_definition>
validate_protobuf_schema(sharded_store& store, canonical_schema schema) {
    auto res = co_await make_protobuf_schema_definition(
      store, std::move(schema));
    co_return canonical_schema_definition{std::move(res)};
}

ss::future<canonical_schema>
make_canonical_protobuf_schema(sharded_store& store, unparsed_schema schema) {
    auto [sub, unparsed] = std::move(schema).destructure();
    auto [def, type, refs] = std::move(unparsed).destructure();
    canonical_schema temp{
      sub,
      {canonical_schema_definition::raw_string{std::move(def)()},
       type,
       std::move(refs)}};

    co_return canonical_schema{
      std::move(sub),
      co_await validate_protobuf_schema(store, std::move(temp))};
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
        // There must be a compatible reader message for every writer
        // message
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

error_info dp_error_collector::error() const {
    return error_info{
      error_code::schema_invalid, fmt::format("{}", fmt::join(_errors, "; "))};
}

} // namespace pandaproxy::schema_registry
