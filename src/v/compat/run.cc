/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "compat/run.h"

#include "compat/check.h"
#include "compat/raft_compat.h"
#include "json/document.h"
#include "json/prettywriter.h"
#include "json/writer.h"
#include "seastarx.h"
#include "utils/base64.h"
#include "utils/file_io.h"

#include <seastar/core/thread.hh>

namespace compat {

template<typename... Types>
struct type_list {};

using compat_checks
  = type_list<raft::timeout_now_request, raft::timeout_now_reply>;

struct compat_error final : public std::runtime_error {
public:
    explicit compat_error(std::string_view name)
      : std::runtime_error(
        fmt::format("compat check failed for {{{}}}", name)) {}
};

template<typename T>
struct corpus_helper {
    using checker = compat_check<T>;

    /*
     * Builds a test case for an instance of T.
     *
     * {
     *   "version": 0,
     *   "name": "raft::some_request",
     *   "fields": { test case field values },
     *   "binaries": [
     *     {
     *       "name": "serde",
     *       "data": "base64 encoding",
     *     },
     *   ]
     * }
     */
    static void write_test_case(T t, json::Writer<json::StringBuffer>& w) {
        auto&& [ta, tb] = compat_copy(std::move(t));

        w.StartObject();
        w.Key("version");
        w.Int(0);

        w.Key("name");
        w.String(checker::name.data());

        w.Key("fields");
        w.StartObject();
        checker::to_json(std::move(ta), w);
        w.EndObject();

        w.Key("binaries");
        w.StartArray();
        for (auto& b : checker::to_binary(std::move(tb))) {
            w.StartObject();
            w.Key("name");
            w.String(b.name);
            w.Key("data");
            w.String(iobuf_to_base64(b.data));
            w.EndObject();
        }
        w.EndArray();
        w.EndObject();
    }

    /*
     * Writes all test cases to the provided directory.
     */
    static ss::future<> write(const std::filesystem::path& dir) {
        size_t instance = 0;

        for (auto& test : checker::create_test_cases()) {
            // json encoded test case
            auto buf = json::StringBuffer{};
            auto writer = json::Writer<json::StringBuffer>{buf};
            write_test_case(std::move(test), writer);

            // save test case to file
            iobuf data;
            data.append(buf.GetString(), buf.GetSize());
            auto fn = fmt::format("{}_{}.json", checker::name, instance++);
            write_fully(dir / fn, std::move(data)).get();
        }

        return ss::now();
    }

    /*
     * Check a test case.
     */
    static void check(json::Document doc) {
        vassert(doc.HasMember("version"), "document doesn't contain version");
        vassert(doc["version"].IsInt(), "version is not an int");
        auto version = doc["version"].GetInt();
        vassert(version == 0, "expected version to be 0 != {}", version);

        // fields is the content of the test instance
        vassert(doc.HasMember("fields"), "document doesn't contain fields");
        vassert(doc["fields"].IsObject(), "fields is not an object");
        auto instance = checker::from_json(doc["fields"].GetObject());

        // binary encodings of the test instance
        vassert(doc.HasMember("binaries"), "document doesn't contain binaries");
        vassert(doc["binaries"].IsArray(), "binaries is not an array");
        auto binaries = doc["binaries"].GetArray();

        for (const auto& encoding : binaries) {
            vassert(encoding.IsObject(), "binaries entry is not an object");
            auto binary = encoding.GetObject();

            vassert(binary.HasMember("name"), "encoding doesn't have name");
            vassert(binary["name"].IsString(), "encoding name is not string");
            vassert(binary.HasMember("data"), "encoding doesn't have data");
            vassert(binary["data"].IsString(), "encoding data is not string");

            compat_binary data(
              binary["name"].GetString(),
              bytes_to_iobuf(base64_to_bytes(binary["data"].GetString())));

            // keep copy for next round
            auto&& [ia, ib] = compat_copy(std::move(instance));
            instance = std::move(ia);

            if (!checker::check(std::move(ib), std::move(data))) {
                json::StringBuffer buf;
                json::PrettyWriter<json::StringBuffer> writer(buf);
                doc.Accept(writer);
                fmt::print("Input JSON: {}\n", buf.GetString());
                throw compat_error(checker::name);
            }
        }
    }
};

template<typename T>
static void maybe_check(const std::string& name, json::Document& doc) {
    using type = corpus_helper<T>;
    if (type::checker::name == name) {
        type::check(std::move(doc));
    }
}

template<typename... Types>
static void
check_types(type_list<Types...>, std::string name, json::Document& doc) {
    /*
     * check for duplicate test names
     */
    const auto names_arr = std::to_array(
      {corpus_helper<Types>::checker::name...});
    const std::set<std::string> names_set{
      std::string(corpus_helper<Types>::checker::name)...};
    vassert(
      names_arr.size() == names_set.size(), "duplicate test name detected");

    /*
     * check that target test name exists
     */
    vassert(names_set.contains(name), "test {} not found", name);

    (maybe_check<Types>(name, doc), ...);
}

static void check(json::Document doc) {
    vassert(doc.HasMember("name"), "doc doesn't have name");
    vassert(doc["name"].IsString(), "name is doc is not a string");
    auto name = doc["name"].GetString();
    check_types(compat_checks{}, name, doc);
}

ss::future<> write_corpus(const std::filesystem::path& dir) {
    return ss::async([dir] {
        [dir]<typename... Types>(type_list<Types...>) {
            (corpus_helper<Types>::write(dir).get(), ...);
        }(compat_checks{});
    });
}

ss::future<> check_type(const std::filesystem::path& file) {
    return read_fully_to_string(file).then([file](auto data) {
        json::Document doc;
        doc.Parse(data);
        vassert(!doc.HasParseError(), "JSON {} has parse errors", file);
        check(std::move(doc));
    });
}

} // namespace compat
