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

#include "base/seastarx.h"
#include "cluster/metadata_dissemination_types.h"
#include "cluster/partition_balancer_types.h"
#include "cluster/types.h"
#include "compat/abort_group_tx_compat.h"
#include "compat/abort_tx_compat.h"
#include "compat/acls_compat.h"
#include "compat/begin_group_tx_compat.h"
#include "compat/begin_tx_compat.h"
#include "compat/check.h"
#include "compat/cluster_compat.h"
#include "compat/commit_group_tx_compat.h"
#include "compat/commit_tx_compat.h"
#include "compat/get_cluster_health_compat.h"
#include "compat/get_node_health_compat.h"
#include "compat/id_allocator_compat.h"
#include "compat/init_tm_tx_compat.h"
#include "compat/metadata_dissemination_compat.h"
#include "compat/model_compat.h"
#include "compat/partition_balancer_compat.h"
#include "compat/raft_compat.h"
#include "compat/try_abort_compat.h"
#include "json/document.h"
#include "json/prettywriter.h"
#include "json/writer.h"
#include "model/record.h"
#include "utils/base64.h"
#include "utils/file_io.h"

#include <seastar/core/thread.hh>

namespace compat {

template<typename... Types>
struct type_list {};

using compat_checks = type_list<
  raft::timeout_now_request,
  raft::timeout_now_reply,
  raft::transfer_leadership_request,
  raft::transfer_leadership_reply,
  raft::install_snapshot_request,
  raft::install_snapshot_reply,
  raft::vote_request,
  raft::vote_reply,
  raft::heartbeat_request,
  raft::heartbeat_reply,
  raft::append_entries_request,
  raft::append_entries_reply,
  cluster::join_node_request,
  cluster::join_node_reply,
  cluster::decommission_node_request,
  cluster::decommission_node_reply,
  cluster::recommission_node_request,
  cluster::recommission_node_reply,
  cluster::finish_reallocation_request,
  cluster::finish_reallocation_reply,
  cluster::set_maintenance_mode_request,
  cluster::set_maintenance_mode_reply,

  cluster::config_status,
  cluster::config_status_request,
  cluster::config_status_reply,
  cluster::cluster_property_kv,
  cluster::config_update_request,
  cluster::config_update_reply,
  cluster::hello_request,
  cluster::hello_reply,
  cluster::feature_update_action,
  cluster::feature_action_request,
  cluster::feature_action_response,
  cluster::feature_barrier_request,
  cluster::feature_barrier_response,
  cluster::begin_tx_request,
  cluster::begin_tx_reply,
  cluster::init_tm_tx_request,
  cluster::init_tm_tx_reply,
  cluster::try_abort_request,
  cluster::try_abort_reply,
  cluster::allocate_id_request,
  cluster::allocate_id_reply,
  cluster::update_leadership_request_v2,
  cluster::update_leadership_reply,
  cluster::get_leadership_request,
  cluster::get_leadership_reply,
  cluster::finish_partition_update_request,
  cluster::finish_partition_update_reply,
  cluster::cancel_all_partition_movements_request,
  cluster::cancel_node_partition_movements_request,
  cluster::cancel_partition_movements_reply,
  cluster::abort_tx_request,
  cluster::abort_tx_reply,
  cluster::begin_group_tx_request,
  cluster::begin_group_tx_reply,
  cluster::commit_tx_request,
  cluster::commit_tx_reply,
  cluster::create_acls_request,
  cluster::create_acls_reply,
  cluster::create_topics_request,
  cluster::create_topics_reply,
  cluster::reconciliation_state_request,
  cluster::reconciliation_state_reply,
  cluster::partition_balancer_overview_request,
  cluster::partition_balancer_overview_reply,
  cluster::delete_acls_request,
  cluster::delete_acls_reply,
  cluster::commit_group_tx_request,
  cluster::commit_group_tx_reply,
  cluster::abort_group_tx_request,
  cluster::abort_group_tx_reply,
  cluster::get_node_health_request,
  cluster::get_node_health_reply,
  cluster::get_cluster_health_request,
  cluster::get_cluster_health_reply,
  cluster::configuration_update_request,
  cluster::configuration_update_reply,
  cluster::remote_topic_properties,
  cluster::topic_properties,
  cluster::topic_configuration,
  model::partition_metadata,
  model::topic_metadata,
  v8_engine::data_policy,
  cluster::incremental_topic_custom_updates,
  cluster::incremental_topic_updates,
  cluster::topic_properties_update,
  cluster::update_topic_properties_request,
  cluster::update_topic_properties_reply,
  model::record_batch_header,
  model::record_batch>;

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
     *       "crc32c": "data checksum",
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
        auto binaries = checker::to_binary(std::move(tb));
        vassert(!binaries.empty(), "No binaries found for {}", checker::name);
        for (auto& b : binaries) {
            crc::crc32c crc;
            crc_extend_iobuf(crc, b.data);
            w.StartObject();
            w.Key("name");
            w.String(b.name);
            w.Key("data");
            w.String(iobuf_to_base64(b.data));
            w.Key("crc32c");
            w.Uint(crc.value());
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

        auto test_cases = checker::create_test_cases();
        vassert(!test_cases.empty(), "No test cases for {}", checker::name);
        for (auto& test : test_cases) {
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

        vassert(!binaries.Empty(), "No binaries found for {}", checker::name);
        for (const auto& encoding : binaries) {
            vassert(encoding.IsObject(), "binaries entry is not an object");
            auto binary = encoding.GetObject();

            vassert(binary.HasMember("name"), "encoding doesn't have name");
            vassert(binary["name"].IsString(), "encoding name is not string");
            vassert(binary.HasMember("data"), "encoding doesn't have data");
            vassert(binary["data"].IsString(), "encoding data is not string");
            vassert(binary.HasMember("crc32c"), "encoding doesn't have crc32c");
            vassert(
              binary["crc32c"].IsUint(), "encoding crc32c is not integer");

            const auto binary_data = bytes_to_iobuf(
              base64_to_bytes(binary["data"].GetString()));

            crc::crc32c crc;
            crc_extend_iobuf(crc, binary_data);

            if (crc.value() != binary["crc32c"].GetUint()) {
                throw compat_error(fmt::format(
                  "Test {} has invalid crc {} expected {}",
                  checker::name,
                  crc.value(),
                  binary["crc32c"].GetUint()));
            }

            compat_binary data(
              binary["name"].GetString(),
              bytes_to_iobuf(base64_to_bytes(binary["data"].GetString())));

            // keep copy for next round
            auto&& [ia, ib] = compat_copy(std::move(instance));
            instance = std::move(ia);

            try {
                checker::check(std::move(ib), std::move(data));
            } catch (const compat_error& e) {
                json::StringBuffer buf;
                json::PrettyWriter<json::StringBuffer> writer(buf);
                doc.Accept(writer);
                throw compat_error(fmt::format(
                  "compat check failed for {}\nInput {}\n{}\n",
                  checker::name,
                  buf.GetString(),
                  e.what()));
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
    std::set<std::string> names_set;
    for (const auto& name : names_arr) {
        auto res = names_set.emplace(name);
        if (!res.second) {
            vassert(false, "Duplicate test name {} detected", name);
        }
    }
    vassert(names_arr.size() == names_set.size(), "duplicate detected");

    /*
     * check that target test name exists
     */
    vassert(names_set.contains(name), "test {} not found", name);

    (maybe_check<Types>(name, doc), ...);
}

ss::future<> check_type(json::Document doc) {
    return ss::async([doc = std::move(doc)]() mutable {
        vassert(doc.HasMember("name"), "doc doesn't have name");
        vassert(doc["name"].IsString(), "name is doc is not a string");
        auto name = doc["name"].GetString();
        check_types(compat_checks{}, name, doc);
    });
}

ss::future<> write_corpus(const std::filesystem::path& dir) {
    return ss::async([dir] {
        [dir]<typename... Types>(type_list<Types...>) {
            (corpus_helper<Types>::write(dir).get(), ...);
        }(compat_checks{});
    });
}

ss::future<json::Document> parse_type(const std::filesystem::path& file) {
    return read_fully_to_string(file).then([file](auto data) {
        json::Document doc;
        doc.Parse(data);
        vassert(!doc.HasParseError(), "JSON {} has parse errors", file);
        return doc;
    });
}

} // namespace compat
