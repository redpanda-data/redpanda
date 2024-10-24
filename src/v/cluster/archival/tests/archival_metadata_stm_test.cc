// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cloud_storage/partition_manifest.h"
#include "cloud_storage/remote.h"
#include "cloud_storage/types.h"
#include "cluster/archival/archival_metadata_stm.h"
#include "cluster/errc.h"
#include "features/feature_table.h"
#include "http/tests/http_imposter.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/record.h"
#include "model/timestamp.h"
#include "raft/fundamental.h"
#include "raft/persisted_stm.h"
#include "raft/state_machine_manager.h"
#include "raft/tests/raft_group_fixture.h"
#include "raft/tests/simple_raft_fixture.h"
#include "storage/tests/utils/disk_log_builder.h"
#include "test_utils/async.h"

#include <seastar/core/io_priority_class.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/noncopyable_function.hh>

#include <boost/test/tools/old/interface.hpp>

#include <chrono>

using namespace std::chrono_literals;

namespace {
ss::logger logger{"archival_metadata_stm_test"};
} // namespace

static ss::abort_source never_abort;

namespace cluster::details {
class command_batch_builder_accessor {};
} // namespace cluster::details

struct archival_metadata_stm_base_fixture
  : simple_raft_fixture
  , http_imposter_fixture
  , cluster::details::command_batch_builder_accessor {
    using simple_raft_fixture::start_raft;
    using simple_raft_fixture::wait_for_becoming_leader;
    using simple_raft_fixture::wait_for_confirmed_leader;

    archival_metadata_stm_base_fixture(
      const archival_metadata_stm_base_fixture&)
      = delete;
    archival_metadata_stm_base_fixture&
    operator=(const archival_metadata_stm_base_fixture&)
      = delete;
    archival_metadata_stm_base_fixture(archival_metadata_stm_base_fixture&&)
      = delete;
    archival_metadata_stm_base_fixture&
    operator=(archival_metadata_stm_base_fixture&&)
      = delete;

    static cloud_storage_clients::s3_configuration
    get_s3_configuration(uint16_t port) {
        net::unresolved_address server_addr(ss::sstring(httpd_host_name), port);
        cloud_storage_clients::s3_configuration conf;
        conf.uri = cloud_storage_clients::access_point_uri(
          ss::sstring(httpd_host_name));
        conf.access_key = cloud_roles::public_key_str("acess-key");
        conf.secret_key = cloud_roles::private_key_str("secret-key");
        conf.region = cloud_roles::aws_region_name("us-east-1");
        conf.url_style = cloud_storage_clients::s3_url_style::virtual_host;
        conf.server_addr = server_addr;
        conf._probe = ss::make_shared<cloud_storage_clients::client_probe>(
          net::metrics_disabled::yes,
          net::public_metrics_disabled::yes,
          cloud_roles::aws_region_name{"us-east-1"},
          cloud_storage_clients::endpoint_url{httpd_host_name});
        return conf;
    }

    archival_metadata_stm_base_fixture()
      : http_imposter_fixture(4446) {
        // Blank feature table to satisfy constructor interface
        feature_table.start().get();
        // Cloud storage config
        cloud_cfg.start().get();
        cloud_cfg
          .invoke_on_all(
            [port = httpd_port_number()](cloud_storage::configuration& cfg) {
                cfg.bucket_name = cloud_storage_clients::bucket_name(
                  "panda-bucket");
                cfg.connection_limit = cloud_storage::connection_limit(10);
                cfg.client_config = get_s3_configuration(port);
            })
          .get();
        // Connection pool
        cloud_conn_pool
          .start(
            cloud_cfg.local().connection_limit(), ss::sharded_parameter([this] {
                return cloud_cfg.local().client_config;
            }))
          .get();
        // Cloud storage remote api
        cloud_io
          .start(
            std::ref(cloud_conn_pool),
            ss::sharded_parameter(
              [this] { return cloud_cfg.local().client_config; }),
            ss::sharded_parameter(
              [this] { return cloud_cfg.local().cloud_credentials_source; }))
          .get();
        cloud_io.invoke_on_all([](cloud_io::remote& io) { return io.start(); })
          .get();
        cloud_api.start(std::ref(cloud_io), std::ref(cloud_cfg)).get();
        cloud_api
          .invoke_on_all([](cloud_storage::remote& api) { return api.start(); })
          .get();
    }

    ~archival_metadata_stm_base_fixture() override {
        stop_all();
        cloud_conn_pool.local().shutdown_connections();
        cloud_io.stop().get();
        cloud_api.stop().get();
        cloud_conn_pool.stop().get();
        cloud_cfg.stop().get();
        feature_table.stop().get();
    }

    ss::sharded<features::feature_table> feature_table;
    ss::sharded<cloud_storage::configuration> cloud_cfg;
    ss::sharded<cloud_storage_clients::client_pool> cloud_conn_pool;
    ss::sharded<cloud_io::remote> cloud_io;
    ss::sharded<cloud_storage::remote> cloud_api;
};

struct archival_metadata_stm_fixture : archival_metadata_stm_base_fixture {
    archival_metadata_stm_fixture() {
        // Archival metadata STM
        create_raft();
        raft::state_machine_manager_builder builder;
        archival_stm = builder.create_stm<cluster::archival_metadata_stm>(
          _raft.get(),
          cloud_api.local(),
          feature_table.local(),
          logger,
          std::nullopt,
          std::nullopt);

        _raft->start(std::move(builder)).get();
        _started = true;
    }

    ss::shared_ptr<cluster::archival_metadata_stm> archival_stm;
};

using cloud_storage::partition_manifest;
using segment_meta = cloud_storage::partition_manifest::segment_meta;
using cloud_storage::segment_name;

FIXTURE_TEST(test_archival_stm_happy_path, archival_metadata_stm_fixture) {
    wait_for_confirmed_leader();
    std::vector<cloud_storage::segment_meta> m;
    m.push_back(segment_meta{
      .base_offset = model::offset(0),
      .committed_offset = model::offset(99),
      .archiver_term = model::term_id(1),
      .segment_term = model::term_id(1)});

    // State machine is initially dirty: this is a cue to upload a manifest
    // when a partition is created, even if we haven't uploaded any segments
    // yet.
    BOOST_REQUIRE(
      archival_stm->get_dirty()
      == cluster::archival_metadata_stm::state_dirty::dirty);

    // Replicate add_segment_cmd command that adds segment with offset 0
    archival_stm
      ->add_segments(
        m,
        std::nullopt,
        model::producer_id{},
        ss::lowres_clock::now() + 10s,
        never_abort,
        cluster::segment_validated::yes)
      .get();
    BOOST_REQUIRE(archival_stm->manifest().size() == 1);
    BOOST_REQUIRE(
      archival_stm->manifest().begin()->base_offset == model::offset(0));
    BOOST_REQUIRE(
      archival_stm->manifest().begin()->committed_offset == model::offset(99));

    // Adding segments should have marked the stm dirty
    BOOST_REQUIRE(
      archival_stm->get_dirty()
      == cluster::archival_metadata_stm::state_dirty::dirty);

    // Mark the manifest clean (emulate an uploader completing an upload of the
    // manifest to object storage)
    archival_stm
      ->mark_clean(
        ss::lowres_clock::now() + 10s,
        archival_stm->get_insync_offset(),
        never_abort)
      .get();
    BOOST_REQUIRE(
      archival_stm->get_dirty()
      == cluster::archival_metadata_stm::state_dirty::clean);
}

FIXTURE_TEST(
  test_archival_stm_update_lco_when_compacted_segment_added,
  archival_metadata_stm_fixture) {
    wait_for_confirmed_leader();
    std::vector<segment_meta> m;
    m.push_back(segment_meta{
      .is_compacted = true,
      .base_offset = model::offset(0),
      .committed_offset = model::offset(99),
      .archiver_term = model::term_id(1),
      .segment_term = model::term_id(1),
    });
    archival_stm
      ->add_segments(
        m,
        std::nullopt,
        model::producer_id{},
        ss::lowres_clock::now() + 10s,
        never_abort,
        cluster::segment_validated::yes)
      .get();
    BOOST_REQUIRE_EQUAL(archival_stm->manifest().size(), 1);
    BOOST_REQUIRE_EQUAL(
      archival_stm->manifest().get_last_uploaded_compacted_offset(),
      model::offset{99});
    BOOST_REQUIRE_EQUAL(
      archival_stm->manifest().begin()->committed_offset, model::offset(99));
}

FIXTURE_TEST(test_archival_stm_segment_replace, archival_metadata_stm_fixture) {
    wait_for_confirmed_leader();
    std::vector<cloud_storage::segment_meta> m1;
    m1.push_back(segment_meta{
      .base_offset = model::offset(0),
      .committed_offset = model::offset(999),
      .archiver_term = model::term_id(1),
      .segment_term = model::term_id(1)});
    m1.push_back(segment_meta{
      .base_offset = model::offset(1000),
      .committed_offset = model::offset(1999),
      .archiver_term = model::term_id(1),
      .segment_term = model::term_id(1)});
    // Replicate add_segment_cmd command that adds segment with offset 0
    archival_stm
      ->add_segments(
        m1,
        std::nullopt,
        model::producer_id{},
        ss::lowres_clock::now() + 10s,
        never_abort,
        cluster::segment_validated::yes)
      .get();
    archival_stm->sync(10s).get();
    BOOST_REQUIRE(archival_stm->manifest().size() == 2);
    BOOST_REQUIRE(archival_stm->get_start_offset() == model::offset(0));
    // Replace first segment
    std::vector<cloud_storage::segment_meta> m2;
    m2.push_back(segment_meta{
      .is_compacted = true,
      .size_bytes = 1024,
      .base_offset = model::offset(0),
      .committed_offset = model::offset(999),
      .archiver_term = model::term_id(1),
      .segment_term = model::term_id(1),
      .sname_format = cloud_storage::segment_name_format::v2});
    archival_stm
      ->add_segments(
        m2,
        std::nullopt,
        model::producer_id{},
        ss::lowres_clock::now() + 10s,
        never_abort,
        cluster::segment_validated::yes)
      .get();
    archival_stm->sync(10s).get();
    BOOST_REQUIRE(archival_stm->manifest().size() == 2);
    BOOST_REQUIRE(archival_stm->manifest().replaced_segments().size() == 1);
    BOOST_REQUIRE(archival_stm->get_start_offset() == model::offset(0));
}

void check_snapshot_size(
  const cluster::archival_metadata_stm& archival_stm,
  const storage::ntp_config& ntp_cfg) {
    std::filesystem::path snapshot_file_path = std::filesystem::path(
                                                 ntp_cfg.work_directory())
                                               / "archival_metadata.snapshot";
    bool snapshot_exists = ss::file_exists(snapshot_file_path.string()).get();

    BOOST_REQUIRE(snapshot_exists);

    BOOST_REQUIRE(
      archival_stm.get_local_snapshot_size()
      == ss::file_size(snapshot_file_path.string()).get());
}

FIXTURE_TEST(test_snapshot_loading, archival_metadata_stm_base_fixture) {
    create_raft();
    auto& ntp_cfg = _raft->log_config();
    partition_manifest m(ntp_cfg.ntp(), ntp_cfg.get_initial_revision());
    m.add(
      segment_name("0-1-v1.log"),
      segment_meta{
        .base_offset = model::offset(0),
        .committed_offset = model::offset(99),
        .archiver_term = model::term_id(1),
        .segment_term = model::term_id(1),
      });
    m.add(
      segment_name("100-1-v1.log"),
      segment_meta{
        .base_offset = model::offset(100),
        .committed_offset = model::offset(199),
        .archiver_term = model::term_id(1),
        .segment_term = model::term_id(1),
      });
    m.add(
      segment_name("200-1-v1.log"),
      segment_meta{
        .base_offset = model::offset(200),
        .committed_offset = model::offset(299),
        .archiver_term = model::term_id(1),
        .segment_term = model::term_id(1),
      });
    m.add(
      segment_name("100-1-v1.log"),
      segment_meta{
        .is_compacted = true,
        .base_offset = model::offset(100),
        .committed_offset = model::offset(299),
        .archiver_term = model::term_id(1),
        .segment_term = model::term_id(1),
        .sname_format = cloud_storage::segment_name_format::v2,
      });
    m.advance_insync_offset(model::offset{42});

    BOOST_REQUIRE(m.advance_highest_producer_id(model::producer_id{1000}));
    BOOST_REQUIRE_EQUAL(m.highest_producer_id(), model::producer_id{1000});
    BOOST_REQUIRE(m.advance_start_offset(model::offset{100}));
    BOOST_REQUIRE_EQUAL(m.get_start_offset().value(), model::offset(100));
    BOOST_REQUIRE_EQUAL(m.get_insync_offset(), model::offset(42));
    BOOST_REQUIRE_EQUAL(
      m.get_last_uploaded_compacted_offset(), model::offset{299});

    cluster::archival_metadata_stm::make_snapshot(ntp_cfg, m, model::offset{42})
      .get();

    raft::state_machine_manager_builder builder;
    auto archival_stm = builder.create_stm<cluster::archival_metadata_stm>(
      _raft.get(),
      cloud_api.local(),
      feature_table.local(),
      logger,
      std::nullopt,
      std::nullopt);
    _raft->start(std::move(builder)).get();
    _started = true;
    wait_for_confirmed_leader();
    {
        std::stringstream s1, s2;
        m.serialize_json(s1);
        archival_stm->manifest().serialize_json(s2);
        vlog(logger.info, "original manifest: {}", s1.str());
        vlog(logger.info, "restored manifest: {}", s2.str());
    }

    BOOST_REQUIRE_EQUAL(archival_stm->get_start_offset(), model::offset{100});
    BOOST_REQUIRE(archival_stm->manifest() == m);
    check_snapshot_size(*archival_stm, ntp_cfg);

    // A snapshot constructed with make_snapshot is always clean
    BOOST_REQUIRE(
      archival_stm->get_dirty()
      == cluster::archival_metadata_stm::state_dirty::clean);
}

FIXTURE_TEST(test_sname_derivation, archival_metadata_stm_base_fixture) {
    create_raft();
    auto& ntp_cfg = _raft->log_config();
    partition_manifest m(ntp_cfg.ntp(), ntp_cfg.get_initial_revision());

    // original segments
    m.add(
      segment_name("0-1-v1.log"),
      segment_meta{
        .base_offset = model::offset(0),
        .committed_offset = model::offset(99),
        .archiver_term = model::term_id(1),
        .segment_term = model::term_id(1),
        .sname_format = cloud_storage::segment_name_format::v1,
      });

    m.add(
      segment_name("100-1-v1.log"),
      segment_meta{
        .base_offset = model::offset(100),
        .committed_offset = model::offset(199),
        .archiver_term = model::term_id(1),
        .segment_term = model::term_id(1),
        .sname_format = cloud_storage::segment_name_format::v2,
      });

    m.add(
      segment_name("200-1-v1.log"),
      segment_meta{
        .base_offset = model::offset(200),
        .committed_offset = model::offset(299),
        .archiver_term = model::term_id(1),
        .segment_term = model::term_id(1),
        .sname_format = cloud_storage::segment_name_format::v3,
      });

    // replaced segments
    m.add(
      segment_name("0-1-v1.log"),
      segment_meta{
        .base_offset = model::offset(0),
        .committed_offset = model::offset(99),
        .archiver_term = model::term_id(1),
        .segment_term = model::term_id(2),
        .sname_format = cloud_storage::segment_name_format::v1,
      });

    m.add(
      segment_name("100-1-v1.log"),
      segment_meta{
        .base_offset = model::offset(100),
        .committed_offset = model::offset(199),
        .archiver_term = model::term_id(1),
        .segment_term = model::term_id(2),
        .sname_format = cloud_storage::segment_name_format::v2,
      });

    m.add(
      segment_name("200-1-v1.log"),
      segment_meta{
        .base_offset = model::offset(200),
        .committed_offset = model::offset(299),
        .archiver_term = model::term_id(1),
        .segment_term = model::term_id(2),
        .sname_format = cloud_storage::segment_name_format::v3,
      });

    m.advance_insync_offset(model::offset{42});
    cluster::archival_metadata_stm::make_snapshot(ntp_cfg, m, model::offset{42})
      .get();

    raft::state_machine_manager_builder builder;
    auto archival_stm = builder.create_stm<cluster::archival_metadata_stm>(
      _raft.get(),
      cloud_api.local(),
      feature_table.local(),
      logger,
      std::nullopt,
      std::nullopt);

    _raft->start(std::move(builder)).get();
    _started = true;
    wait_for_confirmed_leader();

    auto replaced = archival_stm->manifest().replaced_segments();
    BOOST_REQUIRE_EQUAL(
      replaced[0].sname_format, cloud_storage::segment_name_format::v1);
    BOOST_REQUIRE_EQUAL(
      replaced[1].sname_format, cloud_storage::segment_name_format::v2);
    BOOST_REQUIRE_EQUAL(
      replaced[2].sname_format, cloud_storage::segment_name_format::v3);
}

FIXTURE_TEST(
  test_archival_stm_segment_truncate, archival_metadata_stm_fixture) {
    using lw_segment_meta = cloud_storage::partition_manifest::lw_segment_meta;

    wait_for_confirmed_leader();
    auto& ntp_cfg = _raft->log_config();
    std::vector<cloud_storage::segment_meta> m;
    m.push_back(segment_meta{
      .base_offset = model::offset(0),
      .committed_offset = model::offset(99),
      .archiver_term = model::term_id(1),
      .segment_term = model::term_id(1)});
    m.push_back(segment_meta{
      .base_offset = model::offset(100),
      .committed_offset = model::offset(199),
      .archiver_term = model::term_id(1),
      .segment_term = model::term_id(1)});
    m.push_back(segment_meta{
      .base_offset = model::offset(200),
      .committed_offset = model::offset(299),
      .archiver_term = model::term_id(1),
      .segment_term = model::term_id(1)});
    m.push_back(segment_meta{
      .base_offset = model::offset(300),
      .committed_offset = model::offset(399),
      .archiver_term = model::term_id(1),
      .segment_term = model::term_id(1)});
    partition_manifest pm(ntp_cfg.ntp(), ntp_cfg.get_initial_revision());
    for (const auto& s : m) {
        auto name = cloud_storage::generate_local_segment_name(
          s.base_offset, model::term_id{1});
        pm.add(name, s);
    }
    pm.advance_insync_offset(model::offset{4});
    pm.advance_applied_offset(model::offset{4});
    archival_stm
      ->add_segments(
        m,
        std::nullopt,
        model::producer_id{},
        ss::lowres_clock::now() + 10s,
        never_abort,
        cluster::segment_validated::yes)
      .get();
    BOOST_REQUIRE(archival_stm->manifest().size() == 4);
    BOOST_REQUIRE(archival_stm->get_start_offset() == model::offset(0));
    BOOST_REQUIRE(archival_stm->manifest() == pm);

    // Truncate the STM, first segment should be added to the backlog
    archival_stm
      ->truncate(model::offset(101), ss::lowres_clock::now() + 10s, never_abort)
      .get();

    BOOST_REQUIRE_EQUAL(archival_stm->get_start_offset(), model::offset(100));
    auto backlog = archival_stm->get_segments_to_cleanup();
    BOOST_REQUIRE_EQUAL(backlog.size(), 1);
    auto name = cloud_storage::generate_local_segment_name(
      backlog[0].base_offset, backlog[0].segment_term);
    BOOST_REQUIRE(pm.get(name).has_value());
    BOOST_REQUIRE(backlog[0] == lw_segment_meta::convert(*pm.get(name)));

    // Truncate the STM, next segment should be added to the backlog
    archival_stm
      ->truncate(model::offset(200), ss::lowres_clock::now() + 10s, never_abort)
      .get();

    BOOST_REQUIRE_EQUAL(archival_stm->get_start_offset(), model::offset(200));
    backlog = archival_stm->get_segments_to_cleanup();
    BOOST_REQUIRE_EQUAL(backlog.size(), 2);
    for (const auto& it : backlog) {
        auto name = cloud_storage::generate_local_segment_name(
          it.base_offset, it.segment_term);
        BOOST_REQUIRE(pm.get(name).has_value());
        BOOST_REQUIRE(it == lw_segment_meta::convert(*pm.get(name)));
    }
}

namespace old {

using namespace cluster;

struct segment
  : public serde::
      envelope<segment, serde::version<0>, serde::compat_version<0>> {
    // ntp_revision is needed to reconstruct full remote path of
    // the segment. Deprecated because ntp_revision is now part of
    // segment_meta.
    model::initial_revision_id ntp_revision_deprecated;
    cloud_storage::segment_name name;
    cloud_storage::partition_manifest::segment_meta meta;

    auto serde_fields() {
        return std::tie(ntp_revision_deprecated, name, meta);
    }
};

struct snapshot
  : public serde::
      envelope<snapshot, serde::version<0>, serde::compat_version<0>> {
    /// List of segments
    std::vector<segment> segments;

    auto serde_fields() { return std::tie(segments); }
};

} // namespace old

std::vector<old::segment>
old_segments_from_manifest(const cloud_storage::partition_manifest& m) {
    std::vector<old::segment> segments;
    segments.reserve(m.size() + m.size());

    for (auto meta : m) {
        if (meta.ntp_revision == model::initial_revision_id{}) {
            meta.ntp_revision = m.get_revision_id();
        }
        auto name = cloud_storage::generate_local_segment_name(
          meta.base_offset, meta.segment_term);
        segments.push_back(old::segment{
          .ntp_revision_deprecated = meta.ntp_revision,
          .name = std::move(name),
          .meta = meta});
    }

    std::sort(
      segments.begin(), segments.end(), [](const auto& s1, const auto& s2) {
          return s1.meta.base_offset < s2.meta.base_offset;
      });

    return segments;
}

namespace cluster::details {
class archival_metadata_stm_accessor {
public:
    static ss::future<> persist_snapshot(
      storage::simple_snapshot_manager& mgr, raft::stm_snapshot&& snapshot) {
        return raft::file_backed_stm_snapshot::persist_local_snapshot(
          mgr, std::move(snapshot));
    }
};
} // namespace cluster::details

ss::future<> make_old_snapshot(
  const storage::ntp_config& ntp_cfg,
  const cloud_storage::partition_manifest& m,
  model::offset insync_offset) {
    // Create archival_stm_snapshot
    auto segments = old_segments_from_manifest(m);
    iobuf snap_data = serde::to_iobuf(
      old::snapshot{.segments = std::move(segments)});

    auto snapshot = raft::stm_snapshot::create(
      0, insync_offset, std::move(snap_data));

    storage::simple_snapshot_manager tmp_snapshot_mgr(
      std::filesystem::path(ntp_cfg.work_directory()),
      "archival_metadata.snapshot",
      ss::default_priority_class());

    co_await cluster::details::archival_metadata_stm_accessor::persist_snapshot(
      tmp_snapshot_mgr, std::move(snapshot));
}

FIXTURE_TEST(
  test_archival_metadata_stm_snapshot_version_compatibility,
  archival_metadata_stm_base_fixture) {
    create_raft();
    auto& ntp_cfg = _raft->log_config();
    partition_manifest m(ntp_cfg.ntp(), ntp_cfg.get_initial_revision());
    m.add(
      segment_name("0-1-v1.log"),
      segment_meta{
        .base_offset = model::offset(0),
        .committed_offset = model::offset(99),
        .archiver_term = model::term_id(1),
        .segment_term = model::term_id(1),
      });
    m.add(
      segment_name("100-1-v1.log"),
      segment_meta{
        .base_offset = model::offset(100),
        .committed_offset = model::offset(199),
        .archiver_term = model::term_id(1),
        .segment_term = model::term_id(1),
      });
    m.add(
      segment_name("200-1-v1.log"),
      segment_meta{
        .base_offset = model::offset(200),
        .committed_offset = model::offset(299),
        .archiver_term = model::term_id(1),
        .segment_term = model::term_id(1),
      });
    m.advance_insync_offset(model::offset(3));

    make_old_snapshot(ntp_cfg, m, model::offset{3}).get();

    raft::state_machine_manager_builder builder;
    auto archival_stm = builder.create_stm<cluster::archival_metadata_stm>(
      _raft.get(),
      cloud_api.local(),
      feature_table.local(),
      logger,
      std::nullopt,
      std::nullopt);

    _raft->start(std::move(builder)).get();
    _started = true;
    wait_for_confirmed_leader();

    BOOST_REQUIRE(archival_stm->manifest() == m);
    check_snapshot_size(*archival_stm, ntp_cfg);
}

FIXTURE_TEST(test_archival_stm_batching, archival_metadata_stm_fixture) {
    wait_for_confirmed_leader();
    std::vector<cloud_storage::segment_meta> m;
    m.push_back(segment_meta{
      .size_bytes = 200,
      .base_offset = model::offset(0),
      .committed_offset = model::offset(999),
      .archiver_term = model::term_id(1),
      .segment_term = model::term_id(1)});
    m.push_back(segment_meta{
      .base_offset = model::offset(1000),
      .committed_offset = model::offset(1999),
      .archiver_term = model::term_id(1),
      .segment_term = model::term_id(1)});
    m.push_back(segment_meta{
      .size_bytes = 100,
      .base_offset = model::offset(0),
      .committed_offset = model::offset(999),
      .archiver_term = model::term_id(2),
      .segment_term = model::term_id(1),
      .sname_format = cloud_storage::segment_name_format::v2});
    // Replicate add_segment_cmd command that adds segment with offset 0
    auto batcher = archival_stm->batch_start(
      ss::lowres_clock::now() + 10s, never_abort);
    batcher.add_segments(m, cluster::segment_validated::yes);
    batcher.cleanup_metadata();
    batcher.replicate().get();
    BOOST_REQUIRE(archival_stm->manifest().size() == 2);
    BOOST_REQUIRE(archival_stm->get_start_offset() == model::offset(0));
    BOOST_REQUIRE(archival_stm->manifest().replaced_segments().size() == 0);
    BOOST_REQUIRE(
      archival_stm->manifest().begin()->archiver_term == model::term_id(2));
}

FIXTURE_TEST(test_archival_stm_spillover, archival_metadata_stm_fixture) {
    wait_for_confirmed_leader();
    std::vector<cloud_storage::segment_meta> m;
    m.push_back(segment_meta{
      .size_bytes = 100,
      .base_offset = model::offset(0),
      .committed_offset = model::offset(999),
      .archiver_term = model::term_id(1),
      .segment_term = model::term_id(1)});
    m.push_back(segment_meta{
      .size_bytes = 200,
      .base_offset = model::offset(1000),
      .committed_offset = model::offset(1999),
      .archiver_term = model::term_id(1),
      .segment_term = model::term_id(1)});
    m.push_back(segment_meta{
      .size_bytes = 300,
      .base_offset = model::offset(2000),
      .committed_offset = model::offset(2999),
      .archiver_term = model::term_id(2),
      .segment_term = model::term_id(2)});

    // Replicate add_segment_cmd command that adds segment with offset 0
    auto batcher = archival_stm->batch_start(
      ss::lowres_clock::now() + 10s, never_abort);
    batcher.add_segments(m, cluster::segment_validated::yes);
    batcher.replicate().get();
    BOOST_REQUIRE_EQUAL(archival_stm->manifest().size(), 3);
    BOOST_REQUIRE_EQUAL(archival_stm->get_start_offset(), model::offset(0));
    BOOST_REQUIRE_EQUAL(
      archival_stm->manifest().get_archive_start_offset(), model::offset());

    archival_stm
      ->truncate_archive_init(
        model::offset{0},
        model::offset_delta(0),
        ss::lowres_clock::now() + 10s,
        never_abort)
      .get();
    BOOST_REQUIRE_EQUAL(
      archival_stm->manifest().get_archive_start_offset(), model::offset(0));
    BOOST_REQUIRE_EQUAL(
      archival_stm->manifest().get_archive_clean_offset(), model::offset());

    archival_stm
      ->cleanup_archive(
        model::offset{0}, 0, ss::lowres_clock::now() + 10s, never_abort)
      .get();
    BOOST_REQUIRE_EQUAL(
      archival_stm->manifest().get_archive_start_offset(), model::offset{});
    BOOST_REQUIRE_EQUAL(
      archival_stm->manifest().get_archive_clean_offset(), model::offset{});

    // unaligned spillover command shouldn't remove segment
    archival_stm
      ->spillover(
        cloud_storage::segment_meta{
          .base_offset = model::offset{0},
          .committed_offset = model::offset{1}},
        ss::lowres_clock::now() + 10s,
        never_abort)
      .get();
    // the start offset remains unchanged
    BOOST_REQUIRE_EQUAL(archival_stm->get_start_offset(), model::offset(0));

    // aligned spillover command should remove segment
    auto batcher2 = archival_stm->batch_start(
      ss::lowres_clock::now() + 10s, never_abort);
    batcher2.spillover(cloud_storage::segment_meta{
      .base_offset = model::offset(0), .committed_offset = model::offset(999)});
    batcher2.truncate_archive_init(model::offset(200), model::offset_delta(0));
    batcher2.cleanup_archive(model::offset(100), 0);
    batcher2.replicate().get();
    BOOST_REQUIRE_EQUAL(archival_stm->get_start_offset(), model::offset(1000));
    BOOST_REQUIRE_EQUAL(
      archival_stm->manifest().get_archive_start_offset(), model::offset(200));
    BOOST_REQUIRE_EQUAL(
      archival_stm->manifest().get_archive_clean_offset(), model::offset(100));

    // try to move archive_clean_offset backward
    archival_stm
      ->cleanup_archive(
        model::offset{0}, 0, ss::lowres_clock::now() + 10s, never_abort)
      .get();
    BOOST_REQUIRE_EQUAL(
      archival_stm->manifest().get_archive_clean_offset(), model::offset(100));
}

FIXTURE_TEST(
  test_archival_stm_truncate_kafka_offset, archival_metadata_stm_fixture) {
    wait_for_confirmed_leader();
    std::vector<cloud_storage::segment_meta> m;
    m.push_back(segment_meta{
      .size_bytes = 1000,
      .base_offset = model::offset(1000),
      .committed_offset = model::offset(1999),
      .delta_offset = model::offset_delta(0),
      .archiver_term = model::term_id(1),
      .segment_term = model::term_id(1),
      .delta_offset_end = model::offset_delta(0),
    });
    m.push_back(segment_meta{
      .size_bytes = 2000,
      .base_offset = model::offset(2000),
      .committed_offset = model::offset(2999),
      .delta_offset = model::offset_delta(0),
      .archiver_term = model::term_id(1),
      .segment_term = model::term_id(1),
      .delta_offset_end = model::offset_delta(0),
    });
    m.push_back(segment_meta{
      .size_bytes = 3000,
      .base_offset = model::offset(3000),
      .committed_offset = model::offset(3999),
      .delta_offset = model::offset_delta(0),
      .archiver_term = model::term_id(2),
      .segment_term = model::term_id(2),
      .delta_offset_end = model::offset_delta(0),
    });

    // Replicate add_segment_cmd command that adds segment with offset 0
    auto batcher1 = archival_stm->batch_start(
      ss::lowres_clock::now() + 10s, never_abort);
    batcher1.add_segments(m, cluster::segment_validated::yes);
    batcher1.replicate().get();
    BOOST_REQUIRE_EQUAL(archival_stm->manifest().size(), 3);
    BOOST_REQUIRE_EQUAL(archival_stm->get_start_offset(), model::offset(1000));
    BOOST_REQUIRE_EQUAL(
      archival_stm->manifest().get_archive_start_offset(), model::offset());

    auto batcher2 = archival_stm->batch_start(
      ss::lowres_clock::now() + 10s, never_abort);
    batcher2.truncate_archive_init(model::offset(0), model::offset_delta(0));
    batcher2.cleanup_archive(model::offset(0), 0);
    batcher2.replicate().get();
    BOOST_REQUIRE_EQUAL(
      archival_stm->manifest().get_archive_start_offset(), model::offset(0));

    // Truncate by kafka offset inside the archive
    archival_stm
      ->truncate(kafka::offset(200), ss::lowres_clock::now() + 10s, never_abort)
      .get();
    // The start kafka offset doesn't change, only the override changes.
    BOOST_REQUIRE_EQUAL(
      archival_stm->manifest().get_start_kafka_offset_override(),
      kafka::offset(200));
    BOOST_REQUIRE_EQUAL(
      archival_stm->get_start_kafka_offset(), kafka::offset(1000));
    BOOST_REQUIRE_EQUAL(archival_stm->get_start_offset(), model::offset(1000));

    archival_stm
      ->truncate(
        kafka::offset(1200), ss::lowres_clock::now() + 10s, never_abort)
      .get();
    BOOST_REQUIRE_EQUAL(
      archival_stm->get_start_kafka_offset(), kafka::offset(1000));
    BOOST_REQUIRE_EQUAL(
      archival_stm->manifest().get_start_kafka_offset_override(),
      kafka::offset(1200));
    BOOST_REQUIRE_EQUAL(archival_stm->get_start_offset(), model::offset(1000));

    // Advancing the start offset past the override resets the override.
    archival_stm
      ->truncate(
        model::offset(2000), ss::lowres_clock::now() + 10s, never_abort)
      .get();
    BOOST_REQUIRE_EQUAL(
      archival_stm->manifest().get_start_kafka_offset_override(),
      kafka::offset{});
    BOOST_REQUIRE_EQUAL(archival_stm->get_start_offset(), model::offset(2000));
}

FIXTURE_TEST(test_reset_metadata, archival_metadata_stm_fixture) {
    wait_for_confirmed_leader();
    std::vector<cloud_storage::segment_meta> m;
    m.push_back(segment_meta{
      .base_offset = model::offset(0),
      .committed_offset = model::offset(99),
      .archiver_term = model::term_id(1),
      .segment_term = model::term_id(1)});
    m.push_back(segment_meta{
      .base_offset = model::offset(100),
      .committed_offset = model::offset(199),
      .archiver_term = model::term_id(1),
      .segment_term = model::term_id(1)});

    // Replicate add_segment_cmd command that adds segment with offset 0
    archival_stm
      ->add_segments(
        m,
        std::nullopt,
        model::producer_id{},
        ss::lowres_clock::now() + 10s,
        never_abort,
        cluster::segment_validated::yes)
      .get();
    BOOST_REQUIRE(archival_stm->manifest().size() == 2);

    // Reset the manifest and update the start offset, term id, etc.
    auto batcher = archival_stm->batch_start(
      ss::lowres_clock::now() + 10s, never_abort);
    m.clear();
    m.push_back(segment_meta{
      .base_offset = model::offset(100),
      .committed_offset = model::offset(199),
      .archiver_term = model::term_id(2),
      .segment_term = model::term_id(2)});
    batcher.reset_metadata();
    batcher.add_segments(std::move(m), cluster::segment_validated::yes);
    batcher.replicate().get();
    BOOST_REQUIRE(archival_stm->manifest().size() == 1);
    BOOST_REQUIRE(archival_stm->get_start_offset() == model::offset(100));
    BOOST_REQUIRE(archival_stm->manifest().replaced_segments().size() == 0);
    BOOST_REQUIRE(
      archival_stm->manifest().begin()->archiver_term == model::term_id(2));
}

FIXTURE_TEST(test_highest_producer_id, archival_metadata_stm_fixture) {
    wait_for_confirmed_leader();
    auto add_segment =
      [&](int64_t base, int64_t committed, model::producer_id pid) {
          std::vector<cloud_storage::segment_meta> m;
          m.push_back(segment_meta{
            .base_offset = model::offset(base),
            .committed_offset = model::offset(committed),
            .archiver_term = model::term_id(1),
            .segment_term = model::term_id(1)});
          archival_stm
            ->add_segments(
              std::move(m),
              std::nullopt,
              pid,
              ss::lowres_clock::now() + 10s,
              never_abort,
              cluster::segment_validated::yes)
            .get();
      };
    add_segment(0, 9, model::producer_id{});
    BOOST_REQUIRE_EQUAL(
      archival_stm->manifest().highest_producer_id(), model::producer_id{});

    // Bump the producer id.
    add_segment(10, 19, model::producer_id{100});
    BOOST_REQUIRE_EQUAL(
      archival_stm->manifest().highest_producer_id(), model::producer_id{100});

    // We shouldn't be able to move the producer id backwards.
    add_segment(20, 29, model::producer_id{90});
    BOOST_REQUIRE_EQUAL(
      archival_stm->manifest().highest_producer_id(), model::producer_id{100});
    add_segment(30, 39, model::producer_id{});
    BOOST_REQUIRE_EQUAL(
      archival_stm->manifest().highest_producer_id(), model::producer_id{100});
}
