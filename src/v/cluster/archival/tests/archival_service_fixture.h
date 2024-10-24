// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cloud_storage_clients/types.h"
#include "cluster/archival/archiver_manager.h"
#include "cluster/archival/tests/service_fixture.h"
#include "cluster/archival/types.h"
#include "cluster/controller_api.h"
#include "cluster/errc.h"
#include "cluster/fwd.h"
#include "cluster/shard_table.h"
#include "cluster/tests/cluster_test_fixture.h"
#include "cluster/tests/utils.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "config/property.h"
#include "container/fragmented_vector.h"
#include "errc.h"
#include "http/tests/http_imposter.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "model/timeout_clock.h"
#include "random/generators.h"
#include "test_utils/async.h"
#include "test_utils/fixture.h"

#include <seastar/core/loop.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/timed_out_error.hh>

#include <boost/test/tools/old/interface.hpp>

#include <algorithm>
#include <type_traits>

namespace {
constexpr int16_t fixture_port_number = 7676;
inline ss::logger arch_fixture_log("archival_service_fixture");
} // namespace

class archiver_cluster_fixture
  : public cluster_test_fixture
  , public http_imposter_fixture {
    static constexpr int kafka_port_base = 9092;
    static constexpr int rpc_port_base = 11000;
    static constexpr int proxy_port_base = 8082;
    static constexpr int schema_reg_port_base = 8081;

    auto get_configurations() {
        net::unresolved_address server_addr(
          ss::sstring(httpd_host_name), httpd_port_number());
        cloud_storage_clients::s3_configuration s3_conf;
        s3_conf.uri = cloud_storage_clients::access_point_uri(httpd_host_name);
        s3_conf.access_key = cloud_roles::public_key_str("access-key");
        s3_conf.secret_key = cloud_roles::private_key_str("secret-key");
        s3_conf.region = cloud_roles::aws_region_name("us-east-1");
        s3_conf.url_style = cloud_storage_clients::s3_url_style::virtual_host;
        s3_conf._probe = ss::make_shared<cloud_storage_clients::client_probe>(
          net::metrics_disabled::yes,
          net::public_metrics_disabled::yes,
          cloud_roles::aws_region_name{},
          cloud_storage_clients::endpoint_url{});
        s3_conf.server_addr = server_addr;

        archival::configuration a_conf{
          .cloud_storage_initial_backoff = config::mock_binding(100ms),
          .segment_upload_timeout = config::mock_binding(1000ms),
          .manifest_upload_timeout = config::mock_binding(1000ms),
          .garbage_collect_timeout = config::mock_binding(1000ms),
          .upload_loop_initial_backoff = config::mock_binding(100ms),
          .upload_loop_max_backoff = config::mock_binding(5000ms)};
        a_conf.bucket_name = cloud_storage_clients::bucket_name("test-bucket");
        a_conf.ntp_metrics_disabled = archival::per_ntp_metrics_disabled::yes;
        a_conf.svc_metrics_disabled = archival::service_metrics_disabled::yes;
        a_conf.time_limit = std::nullopt;

        cloud_storage::configuration c_conf;
        c_conf.client_config = s3_conf;
        c_conf.bucket_name = cloud_storage_clients::bucket_name("test-bucket");
        c_conf.connection_limit = archival::connection_limit(2);
        c_conf.cloud_credentials_source
          = model::cloud_credentials_source::config_file;
        return std::make_tuple(
          std::move(s3_conf),
          ss::make_lw_shared<archival::configuration>(std::move(a_conf)),
          c_conf);
    }

public:
    model::node_id add_node() {
        auto id = model::node_id((int)apps.size());

        auto [s3_conf, a_conf, cs_conf] = get_configurations();
        auto node = create_node_application(
          id,
          kafka_port_base,
          rpc_port_base,
          proxy_port_base,
          schema_reg_port_base,
          configure_node_id::yes,
          empty_seed_starts_cluster::yes,
          s3_conf,
          std::move(*a_conf),
          cs_conf,
          false);

        apps.insert(std::make_pair(id, node));
        return id;
    }

    /// Invoke functor on every node. Invocations are serialized.
    template<class Fn>
    void invoke_on_all_nodes(Fn fn) {
        for (auto [id, app] : apps) {
            fn(app, id);
        }
    }

    application* get(model::node_id id) { return apps[id]; }

    archiver_cluster_fixture()
      : cluster_test_fixture()
      , http_imposter_fixture(fixture_port_number) {}

    fragmented_vector<model::ntp> create_topic(
      model::topic id,
      int num_partitions,
      std::vector<model::node_id> replicas) {
        fragmented_vector<model::ntp> ntp_list;
        cluster::topic_configuration cfg(
          model::kafka_namespace, id, num_partitions, (int16_t)replicas.size());

        cluster::custom_assignable_topic_configuration cat_cfg{cfg};
        for (int i = 0; i < num_partitions; i++) {
            cat_cfg.custom_assignments.emplace_back(
              cluster::custom_partition_assignment{
                .id = model::partition_id{i},
                .replicas = replicas,
              });
            model::ntp ntp(model::kafka_namespace, id, model::partition_id(i));
            ntp_list.emplace_back(ntp);
        }

        auto node = controller_leader();
        auto results = node->controller->get_topics_frontend()
                         .local()
                         .create_topics({cat_cfg}, model::no_timeout)
                         .get();

        for (const cluster::topic_result& res : results) {
            vlog(
              arch_fixture_log.info,
              "create topic {} result {}",
              res.tp_ns.tp,
              res.ec);
            BOOST_REQUIRE(res.ec == cluster::errc::success);
        }
        node->controller->get_api()
          .local()
          .wait_for_topic(
            model::topic_namespace(model::kafka_namespace, id),
            model::timeout_clock::now() + 30s)
          .get();
        return ntp_list;
    }

    void wait_partition_movement_complete(
      model::ntp ntp, model::timeout_clock::duration timeout = 30s) {
        auto deadline = model::timeout_clock::now() + timeout;
        auto node = controller_leader();
        // wait for the partition movement to complete
        while (
          node->controller->get_topics_state().local().is_update_in_progress(
            ntp)) {
            if (model::timeout_clock::now() < deadline) {
                ss::sleep(20ms).get();
            } else {
                vlog(
                  arch_fixture_log.error,
                  "wait_partition_movement_complete timed out");
                BOOST_FAIL("wait_partition_movement_complete timed out");
            }
        }
    }

    void wait_partition_movement_complete(
      model::ntp ntp,
      const std::vector<model::broker_shard>& replicas,
      model::timeout_clock::duration timeout = 30s) {
        auto deadline = model::timeout_clock::now() + timeout;
        wait_partition_movement_complete(ntp, timeout);
        // wait for all cross-shard movements to complete
        for (const auto [node_id, shard] : replicas) {
            auto n = get(node_id);
            auto expected = shard;
            auto& st = n->controller->get_shard_table().local();
            auto actual = st.shard_for(ntp);
            if (actual == expected) {
                break;
            } else {
                if (model::timeout_clock::now() < deadline) {
                    ss::sleep(10ms).get();
                } else {
                    vlog(
                      arch_fixture_log.error,
                      "wait_partition_movement_complete x-shard timed out");
                    BOOST_FAIL(
                      "wait_partition_movement_complete x-shard timed out");
                }
            }
        }
    }

    application* controller_leader() {
        auto timeout = 60s;
        auto start = ss::lowres_clock::now();
        while (ss::lowres_clock::now() - start < timeout) {
            wait_for_controller_leadership(model::node_id(0)).get();
            auto [rpf, _] = this->get_leader(model::controller_ntp);
            if (rpf == nullptr) {
                continue;
            }
            auto node = &rpf->app;
            return node;
        }
        BOOST_REQUIRE(false);
        return nullptr;
    }

    /// Move partition from source node to the target node
    // NOLINTNEXTLINE
    void move_partition(model::ntp ntp, std::vector<model::node_id> replicas) {
        static constexpr int num_retries = 4;
        std::error_code errc;
        for (int i = 0; i < num_retries; i++) {
            auto node = controller_leader();
            BOOST_REQUIRE(node != nullptr);
            errc = node->controller->get_topics_frontend()
                     .local()
                     .move_partition_replicas(
                       ntp,
                       replicas,
                       cluster::reconfiguration_policy::min_local_retention,
                       model::no_timeout)
                     .get();
            vlog(
              arch_fixture_log.info,
              "move_partition_replicas result {}",
              errc.message());

            if (errc == raft::errc::not_leader) {
                vlog(
                  arch_fixture_log.info,
                  "move_partition_replicas controller leadership has moved, "
                  "retrying");
                continue;
            }
            break;
        }
        BOOST_REQUIRE(errc == cluster::errc::success);
    }

    /// Move partition from source node to the target node
    // NOLINTNEXTLINE
    void
    move_partition(model::ntp ntp, std::vector<model::broker_shard> replicas) {
        static constexpr int num_retries = 4;
        std::error_code errc;
        for (int i = 0; i < num_retries; i++) {
            auto node = controller_leader();
            BOOST_REQUIRE(node != nullptr);
            errc = node->controller->get_topics_frontend()
                     .local()
                     .move_partition_replicas(
                       ntp,
                       replicas,
                       cluster::reconfiguration_policy::min_local_retention,
                       model::no_timeout)
                     .get();
            vlog(
              arch_fixture_log.info,
              "move_partition_replicas (x-shard) result {}",
              errc.message());

            if (errc == raft::errc::not_leader) {
                vlog(
                  arch_fixture_log.info,
                  "move_partition_replicas (x-shard) controller leadership has "
                  "moved, retrying");
                continue;
            }
            break;
        }
        BOOST_REQUIRE(errc == cluster::errc::success);
    }

    /// Return list of nodes that have a replica of the partition
    std::vector<model::broker_shard>
    replica_locations(model::ntp ntp, application* node = nullptr) {
        if (node == nullptr) {
            node = controller_leader();
        }
        auto pa = node->controller->get_topics_state()
                    .local()
                    .get_partition_assignment(ntp);
        if (!pa.has_value()) {
            vlog(
              arch_fixture_log.error, "get_partition_assignment returned null");
            BOOST_REQUIRE(pa.has_value());
        }
        std::vector<model::broker_shard> result;
        for (const auto& r : pa->replicas) {
            result.push_back(r);
        }
        return result;
    }

    void delete_topic(model::topic id) {
        vlog(arch_fixture_log.info, "Deleting topic {}", id);
        auto node = controller_leader();
        BOOST_REQUIRE(node != nullptr);
        std::vector<model::topic_namespace> query = {
          model::topic_namespace(model::kafka_namespace, std::move(id)),
        };
        node->controller->get_topics_frontend()
          .local()
          .delete_topics(std::move(query), model::no_timeout)
          .get();
    }

    /// Wait until archiver_service is started managing all all partitions
    /// on every node.
    /// Returns number of managed replicas.
    void wait_all_partitions_managed(
      const fragmented_vector<model::ntp>& all_ntp,
      model::node_id node_id,
      ss::lowres_clock::duration timeout = 30s) {
        std::set<model::ntp> expected(all_ntp.begin(), all_ntp.end());
        auto deadline = ss::lowres_clock::now() + timeout;
        while (ss::lowres_clock::now() < deadline) {
            auto actual = set_of_managed_partitions(node_id);
            if (actual == expected) {
                return;
            }
            ss::sleep(100ms).get();
        }
        vlog(arch_fixture_log.error, "wait_all_partitions timed out");
        BOOST_FAIL("Wait until all partitions are managed failed");
    }

    /// Wait until archiver_service is started managing all all partitions
    /// on every node.
    /// Returns number of managed replicas.
    void wait_all_partitions_managed(
      const fragmented_vector<model::ntp>& all_ntp,
      ss::lowres_clock::duration timeout = 30s) {
        std::set<model::ntp> expected(all_ntp.begin(), all_ntp.end());
        auto deadline = ss::lowres_clock::now() + timeout;
        while (ss::lowres_clock::now() < deadline) {
            auto actual = set_of_managed_partitions();
            if (actual == expected) {
                return;
            }
            ss::sleep(100ms).get();
        }
        vlog(arch_fixture_log.error, "wait_all_partitions timed out");
        BOOST_FAIL("Wait until all partitions are managed failed");
    }

    /// Wait until archiver_service is started uploading all all partitions
    /// on every node.
    /// Returns number of managed replicas.
    void wait_all_partition_leaders(
      const fragmented_vector<model::ntp>& expected,
      model::node_id target,
      ss::lowres_clock::duration timeout = 30s) {
        std::set<model::ntp> s_expected(expected.begin(), expected.end());
        auto deadline = ss::lowres_clock::now() + timeout;
        while (ss::lowres_clock::now() < deadline) {
            auto actual = leader_partitions(target);
            if (actual == s_expected) {
                return;
            }
            ss::sleep(100ms).get();
        }
        vlog(arch_fixture_log.error, "wait_all_partition_leaders timed out");
        BOOST_FAIL("Wait until all partition leaders are managed failed");
    }

    /// Wait until archiver_service is started uploading all all partitions
    /// on every node.
    /// Returns number of managed replicas.
    void wait_all_partition_leaders(
      const fragmented_vector<model::ntp>& expected,
      ss::lowres_clock::duration timeout = 30s) {
        std::set<model::ntp> s_expected(expected.begin(), expected.end());
        auto deadline = ss::lowres_clock::now() + timeout;
        while (ss::lowres_clock::now() < deadline) {
            auto actual = leader_partitions();
            if (actual == s_expected) {
                return;
            }
            ss::sleep(100ms).get();
        }
        vlog(arch_fixture_log.error, "wait_all_partition_leaders timed out");
        BOOST_FAIL("Wait until all partition leaders are managed failed");
    }

    /// Return full list of NTP's managed by all archiver service instances
    ///
    /// \return set of NTP's managed by the cluster
    std::set<model::ntp> set_of_managed_partitions() {
        std::set<model::ntp> initial;
        auto reduce = [](std::set<model::ntp> lhs, std::set<model::ntp> rhs) {
            lhs.insert(rhs.begin(), rhs.end());
            std::stringstream str;
            for (const auto& n : lhs) {
                fmt::print(str, " {}", n);
            }
            vlog(arch_fixture_log.debug, "Merged NTP's {}", str.str());
            return lhs;
        };
        auto map = [this](model::node_id node) {
            auto app = get(node);
            auto& svc = app->archiver_manager.local();
            auto lst = svc.managed_partitions();
            std::stringstream str;
            for (const auto& n : lst) {
                fmt::print(str, " {}", n);
            }
            vlog(arch_fixture_log.debug, "Collected NTP's {}", str.str());
            std::set<model::ntp> res;
            res.insert(lst.begin(), lst.end());
            return res;
        };
        return map_reduce(map, initial, reduce);
    }

    /// Return full list of NTP's managed by the archiver service on the node
    ///
    /// \param target is an id of the node which has to be checked
    /// \return set of NTP's managed by the target node
    std::set<model::ntp> set_of_managed_partitions(model::node_id target) {
        std::set<model::ntp> initial;
        auto reduce = [](std::set<model::ntp> lhs, std::set<model::ntp> rhs) {
            lhs.insert(rhs.begin(), rhs.end());
            std::stringstream str;
            for (const auto& n : lhs) {
                fmt::print(str, " {}", n);
            }
            vlog(arch_fixture_log.debug, "Merged NTP's {}", str.str());
            return lhs;
        };
        auto map = [this, target](model::node_id node) {
            std::set<model::ntp> res;
            if (node != target) {
                return res;
            }
            auto app = get(node);
            auto& svc = app->archiver_manager.local();
            auto lst = svc.managed_partitions();
            std::stringstream str;
            for (const auto& n : lst) {
                fmt::print(str, " {}", n);
            }
            vlog(arch_fixture_log.debug, "Collected NTP's {}", str.str());
            res.insert(lst.begin(), lst.end());
            return res;
        };
        return map_reduce(map, initial, reduce);
    }

    /// Return list of leader NTP's managed by one node
    ///
    /// \return set of NTP's with leaders managed by the node
    std::set<model::ntp> leader_partitions(model::node_id target) {
        std::set<model::ntp> initial;
        auto reduce = [](std::set<model::ntp> lhs, std::set<model::ntp> rhs) {
            lhs.insert(rhs.begin(), rhs.end());
            return lhs;
        };
        auto map = [this, target](model::node_id node) {
            std::set<model::ntp> res;
            if (target != node) {
                return res;
            }
            auto app = get(node);
            auto& svc = app->archiver_manager.local();
            auto lst = svc.leader_partitions();
            res.insert(lst.begin(), lst.end());
            return res;
        };
        return map_reduce(map, initial, reduce);
    }

    /// Return full list of leader NTP's managed by all archiver service
    /// instances
    ///
    /// \return set of NTP's with leaders managed by the cluster
    std::set<model::ntp> leader_partitions() {
        std::set<model::ntp> initial;
        auto reduce = [](std::set<model::ntp> lhs, std::set<model::ntp> rhs) {
            lhs.insert(rhs.begin(), rhs.end());
            return lhs;
        };
        auto map = [this](model::node_id node) {
            auto app = get(node);
            auto& svc = app->archiver_manager.local();
            auto lst = svc.leader_partitions();
            std::set<model::ntp> res;
            res.insert(lst.begin(), lst.end());
            return res;
        };
        return map_reduce(map, initial, reduce);
    }

    /// Similar to seastar's map_reduce0 but runs on all tested redpanda
    /// instances.
    ///
    /// \param map callable with the signature `Value (model::node_id)`
    ///            used as the second input to \c reduce
    /// \param initial initial value used as the first input to \c reduce.
    /// \param reduce binary function used to left-fold the return values of
    ///               the \c map into \c initial
    template<typename Mapper, typename Value, typename Reduce>
    Value map_reduce(Mapper map, Value initial, Reduce reduce) {
        Value acc = initial;
        for (auto& [node_id, value] : apps) {
            for (size_t cpu_id = 0; cpu_id < ss::smp::count; cpu_id++) {
                auto res = ss::smp::submit_to(cpu_id, [node_id, &map] {
                               return map(node_id);
                           }).get();
                acc = reduce(acc, res);
            }
        }
        return acc;
    }

    /// Fill partition with data and then read it back and return
    ///
    batches_t replicate_and_read(
      application* node,
      const model::ntp& ntp,
      int count,
      raft::consistency_level acks = raft::consistency_level::quorum_ack) {
        vlog(
          arch_fixture_log.debug,
          "Producing {} to {} with acks={}",
          count,
          ntp,
          acks);

        // Make test data
        auto batches
          = model::test::make_random_batches(model::offset(0), count).get();

        auto partition = node->partition_manager.local().get(ntp);

        result<raft::replicate_result> res
          = partition->raft()
              ->replicate(
                model::make_memory_record_batch_reader(std::move(batches)),
                raft::replicate_options(acks))
              .get();

        BOOST_REQUIRE(res.has_value());
        auto last_offset = res.value().last_offset;

        // Read data back starting from the last offset
        auto reader
          = partition
              ->make_reader(storage::log_reader_config(
                model::offset{0}, last_offset, ss::default_priority_class()))
              .get();

        return model::consume_reader_to_memory(
                 std::move(reader), model::no_timeout)
          .get();
    }

    void shuffle_leadership_smp(model::ntp ntp) {
        auto app = apps.begin()->second;
        auto& leaders = app->controller->get_partition_leaders().local();
        auto current_leader = leaders.get_leader(ntp);
        if (!current_leader) {
            vlog(arch_fixture_log.warn, "Partition {} is leaderless", ntp);
            return;
        }
        auto leader_node = get(*current_leader);
        leader_node->partition_manager
          .invoke_on_all([ntp](cluster::partition_manager& pm) {
              auto part = pm.get(ntp);
              if (!part) {
                  return ss::now();
              }
              return part
                ->transfer_leadership(
                  raft::transfer_leadership_request{.group = part->group()})
                .discard_result();
          })
          .get();
    }

    std::map<model::node_id, application*> apps;
};
