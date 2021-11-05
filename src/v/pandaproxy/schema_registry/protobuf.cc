/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "pandaproxy/schema_registry/protobuf.h"

#include "pandaproxy/schema_registry/errors.h"
#include "pandaproxy/schema_registry/sharded_store.h"

#include <seastar/core/coroutine.hh>

#include <fmt/ostream.h>
#include <google/protobuf/compiler/parser.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/io/tokenizer.h>
#include <google/protobuf/io/zero_copy_stream.h>

namespace pandaproxy::schema_registry {

namespace {

namespace pb = google::protobuf;

}

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
    void AddError(int line, int column, const std::string& message) final {
        _errors.emplace_back(err{level::error, line, column, message});
    }
    void AddWarning(int line, int column, const std::string& message) final {
        _errors.emplace_back(err{level::warn, line, column, message});
    }

    error_info error() const {
        return error_info{
          error_code::schema_invalid,
          fmt::format("{}", fmt::join(_errors, "; "))};
    }

private:
    friend struct fmt::formatter<err>;

    std::vector<err> _errors;
};

class dp_error_collector final : public pb::DescriptorPool::ErrorCollector {
public:
    void AddError(
      const std::string& filename,
      const std::string& element_name,
      const pb::Message* descriptor,
      ErrorLocation location,
      const std::string& message) final {
        _errors.emplace_back(err{
          level::error, filename, element_name, descriptor, location, message});
    }
    void AddWarning(
      const std::string& filename,
      const std::string& element_name,
      const pb::Message* descriptor,
      ErrorLocation location,
      const std::string& message) final {
        _errors.emplace_back(err{
          level::warn, filename, element_name, descriptor, location, message});
    }

    error_info error() const {
        return error_info{
          error_code::schema_invalid,
          fmt::format("{}", fmt::join(_errors, "; "))};
    }

private:
    enum class level {
        error,
        warn,
    };
    struct err {
        level lvl;
        std::string filename;
        std::string element_name;
        const pb::Message* descriptor;
        ErrorLocation location;
        std::string message;
    };
    friend struct fmt::formatter<err>;

    std::vector<err> _errors;
};

///\brief Implements ZeroCopyInputStream with a copy of the definition
class schema_def_input_stream : public pb::io::ZeroCopyInputStream {
public:
    explicit schema_def_input_stream(const canonical_schema_definition& def)
      : _str(def.raw())
      , _impl{_str().data(), static_cast<int>(_str().size())} {}

    bool Next(const void** data, int* size) override {
        return _impl.Next(data, size);
    }
    void BackUp(int count) override { return _impl.BackUp(count); }
    bool Skip(int count) override { return _impl.Skip(count); }
    int64_t ByteCount() const override { return _impl.ByteCount(); }

private:
    canonical_schema_definition::raw_string _str;
    pb::io::ArrayInputStream _impl;
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

        if (!_parser.Parse(&t, &_fdp)) {
            throw as_exception(error_collector.error());
        }

        _fdp.set_name(schema.sub()());
        return _fdp;
    }

private:
    pb::compiler::Parser _parser;
    pb::FileDescriptorProto _fdp;
};

ss::future<const pb::FileDescriptor*> build_file_with_deps(
  pb::DescriptorPool& dp, sharded_store& store, const canonical_schema& schema);

ss::future<const pb::FileDescriptor*> build_file_with_refs(
  pb::DescriptorPool& dp, sharded_store& store, const canonical_schema& schema);

///\brief Build a FileDescriptor using the DescriptorPool.
///
/// Dependencies are required to be in the DescriptorPool.
const pb::FileDescriptor*
build_file(pb::DescriptorPool& dp, const pb::FileDescriptorProto& fdp) {
    dp_error_collector dp_ec;
    if (auto fd = dp.BuildFileCollectingErrors(fdp, &dp_ec); fd) {
        return fd;
    }
    throw as_exception(dp_ec.error());
}

///\brief Import a schema in the DescriptorPool and return the FileDescriptor.
ss::future<const pb::FileDescriptor*> import_schema(
  pb::DescriptorPool& dp,
  sharded_store& store,
  const canonical_schema& schema) {
    try {
        co_return co_await build_file_with_refs(dp, store, schema);
    } catch (const exception& e) {
        throw as_exception(invalid_schema(schema));
    }
}

///\brief Build a FileDescriptor and import references from the store.
///
/// Recursively import references into the DescriptorPool, building the files
/// on stack unwind.
ss::future<const pb::FileDescriptor*> build_file_with_refs(
  pb::DescriptorPool& dp,
  sharded_store& store,
  const canonical_schema& schema) {
    for (const auto& ref : schema.refs()) {
        auto dep = co_await store.get_subject_schema(
          ref.sub, ref.version, include_deleted::no);
        co_await build_file_with_refs(
          dp,
          store,
          canonical_schema{
            subject{ref.name},
            std::move(dep.schema).def(),
            std::move(dep.schema).refs()});
    }
    co_return co_await build_file_with_deps(dp, store, schema);
}

///\brief Build a FileDescriptor and import dependencies from the store.
///
/// Recursively import dependencies into the DescriptorPool, building the files
/// on stack unwind.
ss::future<const pb::FileDescriptor*> build_file_with_deps(
  pb::DescriptorPool& dp,
  sharded_store& store,
  const canonical_schema& schema) {
    parser p;
    auto fdp = p.parse(schema);

    const auto dependency_size = fdp.dependency_size();
    for (int i = 0; i < dependency_size; ++i) {
        auto sub = subject{fdp.dependency(i)};
        if (auto fd = dp.FindFileByName(sub()); !fd) {
            auto sub_schema = co_await store.get_subject_schema(
              sub, std::nullopt, include_deleted::no);

            co_await build_file_with_refs(
              dp,
              store,
              canonical_schema{
                sub,
                std::move(sub_schema.schema).def(),
                std::move(sub_schema.schema).refs()});
        }
    }

    co_return build_file(dp, fdp);
}

struct protobuf_schema_definition::impl {
    pb::DescriptorPool _dp;
    const pb::FileDescriptor* fd{};
};

canonical_schema_definition::raw_string
protobuf_schema_definition::raw() const {
    return canonical_schema_definition::raw_string{_impl->fd->DebugString()};
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
  sharded_store& store, const canonical_schema& schema) {
    auto impl = ss::make_shared<protobuf_schema_definition::impl>();
    impl->fd = co_await import_schema(impl->_dp, store, schema);
    co_return protobuf_schema_definition{std::move(impl)};
}

ss::future<canonical_schema_definition>
validate_protobuf_schema(sharded_store& store, const canonical_schema& schema) {
    auto res = co_await make_protobuf_schema_definition(store, schema);
    co_return canonical_schema_definition{std::move(res)};
}

ss::future<canonical_schema>
make_canonical_protobuf_schema(sharded_store& store, unparsed_schema schema) {
    canonical_schema temp{
      std::move(schema).sub(),
      {canonical_schema_definition::raw_string{schema.def().raw()()},
       schema.def().type()},
      std::move(schema).refs()};

    auto validated = co_await validate_protobuf_schema(store, temp);
    co_return canonical_schema{
      std::move(temp).sub(), std::move(validated), std::move(temp).refs()};
}

} // namespace pandaproxy::schema_registry

template<>
struct fmt::formatter<pandaproxy::schema_registry::io_error_collector::err> {
    using type = pandaproxy::schema_registry::io_error_collector;

    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

    template<typename FormatContext>
    auto format(const type::err& e, FormatContext& ctx) {
        return format_to(
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
    auto format(const type::err& e, FormatContext& ctx) {
        return format_to(
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
