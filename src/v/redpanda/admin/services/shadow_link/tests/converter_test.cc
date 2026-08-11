/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "cluster_link/model/types.h"
#include "crypto/crypto.h"
#include "redpanda/admin/services/shadow_link/converter.h"
#include "utils/base64.h"

#include <gtest/gtest.h>

#include <utility>

using namespace std::chrono_literals;

namespace {
template<typename T>
chunked_vector<T> copy_chunked_vector(const chunked_vector<T>& src) {
    chunked_vector<T> dst;
    dst.reserve(src.size());
    std::ranges::copy(src, std::back_inserter(dst));
    return dst;
}
} // namespace

TEST(converter_test, create_to_metadata_no_authn) {
    const auto name = "test-link";
    proto::admin::shadow_link shadow_link;
    proto::admin::create_shadow_link_request req;
    proto::admin::shadow_link_configurations shadow_link_configurations;
    proto::admin::shadow_link_client_options shadow_link_client_options;

    shadow_link_client_options.set_bootstrap_servers({"localhost:9092"});
    shadow_link_configurations.set_client_options(
      std::move(shadow_link_client_options));

    shadow_link.set_configurations(std::move(shadow_link_configurations));
    shadow_link.set_name(ss::sstring{name});
    req.set_shadow_link(std::move(shadow_link));

    auto md = admin::convert_create_to_metadata(std::move(req));

    EXPECT_EQ(md.name, cluster_link::model::name_t{name});
    EXPECT_EQ(md.connection.bootstrap_servers.size(), 1);
    EXPECT_EQ(
      md.connection.bootstrap_servers[0],
      net::unresolved_address("localhost", 9092));
    EXPECT_EQ(
      md.configuration.topic_metadata_mirroring_cfg.get_start_offset_ts(),
      cluster_link::model::earliest_offset_ts);
    EXPECT_FALSE(md.configuration.topic_metadata_mirroring_cfg.starting_offset
                   .has_value());
    EXPECT_TRUE(md.configuration.topic_metadata_mirroring_cfg.is_enabled);
    EXPECT_TRUE(md.configuration.consumer_groups_mirroring_cfg.is_enabled);
    EXPECT_TRUE(md.configuration.security_settings_sync_cfg.is_enabled);
}

TEST(converter_test, create_to_metadata_tasks_disabled) {
    const auto name = "test-link";
    proto::admin::shadow_link shadow_link;
    proto::admin::create_shadow_link_request req;
    proto::admin::shadow_link_configurations shadow_link_configurations;
    proto::admin::shadow_link_client_options shadow_link_client_options;

    shadow_link_client_options.set_bootstrap_servers({"localhost:9092"});
    shadow_link_configurations.set_client_options(
      std::move(shadow_link_client_options));

    proto::admin::topic_metadata_sync_options topic_metadata_sync_options;
    topic_metadata_sync_options.set_paused(true);
    shadow_link_configurations.set_topic_metadata_sync_options(
      std::move(topic_metadata_sync_options));

    proto::admin::consumer_offset_sync_options consumer_offset_sync_options;
    consumer_offset_sync_options.set_paused(true);
    shadow_link_configurations.set_consumer_offset_sync_options(
      std::move(consumer_offset_sync_options));

    proto::admin::security_settings_sync_options security_settings_sync_options;
    security_settings_sync_options.set_paused(true);
    shadow_link_configurations.set_security_sync_options(
      std::move(security_settings_sync_options));

    shadow_link.set_configurations(std::move(shadow_link_configurations));
    shadow_link.set_name(ss::sstring{name});
    req.set_shadow_link(std::move(shadow_link));

    auto md = admin::convert_create_to_metadata(std::move(req));

    EXPECT_FALSE(md.configuration.topic_metadata_mirroring_cfg.is_enabled);
    EXPECT_FALSE(md.configuration.consumer_groups_mirroring_cfg.is_enabled);
    EXPECT_FALSE(md.configuration.security_settings_sync_cfg.is_enabled);
}

TEST(converter_test, create_no_bootstrap) {
    proto::admin::create_shadow_link_request req;
    req.set_shadow_link(proto::admin::shadow_link{});
    EXPECT_THROW(
      admin::convert_create_to_metadata(std::move(req)),
      serde::pb::rpc::invalid_argument_exception);
}

TEST(converter_test, create_invalid_bootstrap) {
    proto::admin::shadow_link shadow_link;
    proto::admin::create_shadow_link_request req;
    proto::admin::shadow_link_configurations shadow_link_configurations;
    proto::admin::shadow_link_client_options shadow_link_client_options;

    shadow_link_client_options.set_bootstrap_servers({"localhost"});
    shadow_link_configurations.set_client_options(
      std::move(shadow_link_client_options));

    shadow_link.set_configurations(std::move(shadow_link_configurations));
    req.set_shadow_link(std::move(shadow_link));
    EXPECT_THROW(
      admin::convert_create_to_metadata(std::move(req)),
      serde::pb::rpc::invalid_argument_exception);
}

TEST(converter_test, create_with_authn_config_scram_256) {
    const auto name = "test-link";
    const auto username = "test-user";
    const auto password = "test-password";
    const auto mechanism = "SCRAM-SHA-256";
    proto::admin::shadow_link shadow_link;
    proto::admin::create_shadow_link_request req;
    proto::admin::shadow_link_configurations shadow_link_configurations;
    proto::admin::shadow_link_client_options shadow_link_client_options;
    proto::admin::authentication_configuration authn_config;
    proto::admin::scram_config scram_config;

    scram_config.set_username(ss::sstring{username});
    scram_config.set_password(ss::sstring{password});
    scram_config.set_scram_mechanism(
      proto::admin::scram_mechanism::scram_sha_256);
    authn_config.set_scram_configuration(std::move(scram_config));
    shadow_link_client_options.set_authentication_configuration(
      std::move(authn_config));

    shadow_link_client_options.set_bootstrap_servers({"localhost:9092"});
    shadow_link_configurations.set_client_options(
      std::move(shadow_link_client_options));

    shadow_link.set_configurations(std::move(shadow_link_configurations));
    shadow_link.set_name(ss::sstring{name});
    req.set_shadow_link(std::move(shadow_link));

    auto now = model::to_time_point(model::timestamp::now());
    auto md = admin::convert_create_to_metadata(std::move(req));
    ASSERT_TRUE(md.connection.authn_config.has_value());
    ASSERT_TRUE(
      std::holds_alternative<cluster_link::model::scram_credentials>(
        md.connection.authn_config.value()));
    const auto& md_authn_config
      = std::get<cluster_link::model::scram_credentials>(
        md.connection.authn_config.value());

    EXPECT_EQ(md_authn_config.username, username);
    EXPECT_EQ(md_authn_config.password, password);
    EXPECT_EQ(md_authn_config.mechanism, mechanism);
    auto pwd_updated = model::to_time_point(
      md_authn_config.password_last_updated);
    // Expect the password updated time to be within 10s
    EXPECT_GE(pwd_updated, now - 5s);
    EXPECT_LE(pwd_updated, now + 5s);
}

TEST(converter_test, create_with_authn_config_scram_512) {
    const auto name = "test-link";
    const auto username = "test-user";
    const auto password = "test-password";
    const auto mechanism = "SCRAM-SHA-512";
    proto::admin::shadow_link shadow_link;
    proto::admin::create_shadow_link_request req;
    proto::admin::shadow_link_configurations shadow_link_configurations;
    proto::admin::shadow_link_client_options shadow_link_client_options;
    proto::admin::authentication_configuration authn_config;
    proto::admin::scram_config scram_config;

    scram_config.set_username(ss::sstring{username});
    scram_config.set_password(ss::sstring{password});
    scram_config.set_scram_mechanism(
      proto::admin::scram_mechanism::scram_sha_512);
    authn_config.set_scram_configuration(std::move(scram_config));
    shadow_link_client_options.set_authentication_configuration(
      std::move(authn_config));

    shadow_link_client_options.set_bootstrap_servers({"localhost:9092"});
    shadow_link_configurations.set_client_options(
      std::move(shadow_link_client_options));

    shadow_link.set_configurations(std::move(shadow_link_configurations));
    shadow_link.set_name(ss::sstring{name});
    req.set_shadow_link(std::move(shadow_link));

    auto md = admin::convert_create_to_metadata(std::move(req));
    ASSERT_TRUE(md.connection.authn_config.has_value());
    ASSERT_TRUE(
      std::holds_alternative<cluster_link::model::scram_credentials>(
        md.connection.authn_config.value()));
    const auto& md_authn_config
      = std::get<cluster_link::model::scram_credentials>(
        md.connection.authn_config.value());

    EXPECT_EQ(md_authn_config.username, username);
    EXPECT_EQ(md_authn_config.password, password);
    EXPECT_EQ(md_authn_config.mechanism, mechanism);
}

TEST(converter_test, create_with_authn_config_scram_unspecified) {
    const auto name = "test-link";
    const auto username = "test-user";
    const auto password = "test-password";

    proto::admin::shadow_link shadow_link;
    proto::admin::create_shadow_link_request req;
    proto::admin::shadow_link_configurations shadow_link_configurations;
    proto::admin::shadow_link_client_options shadow_link_client_options;
    proto::admin::authentication_configuration authn_config;
    proto::admin::scram_config scram_config;

    scram_config.set_username(ss::sstring{username});
    scram_config.set_password(ss::sstring{password});
    scram_config.set_scram_mechanism(
      proto::admin::scram_mechanism::unspecified);
    authn_config.set_scram_configuration(std::move(scram_config));
    shadow_link_client_options.set_authentication_configuration(
      std::move(authn_config));

    shadow_link_client_options.set_bootstrap_servers({"localhost:9092"});
    shadow_link_configurations.set_client_options(
      std::move(shadow_link_client_options));

    shadow_link.set_configurations(std::move(shadow_link_configurations));
    shadow_link.set_name(ss::sstring{name});
    req.set_shadow_link(std::move(shadow_link));

    EXPECT_THROW(
      admin::convert_create_to_metadata(std::move(req)),
      serde::pb::rpc::invalid_argument_exception);
}

TEST(converter_test, create_with_plain) {
    const auto name = "test-link";
    const auto username = "test-user";
    const auto password = "test-password";

    proto::admin::shadow_link shadow_link;
    proto::admin::create_shadow_link_request req;
    proto::admin::shadow_link_configurations shadow_link_configurations;
    proto::admin::shadow_link_client_options shadow_link_client_options;
    proto::admin::authentication_configuration authn_config;
    proto::admin::plain_config plain_config;

    plain_config.set_username(ss::sstring{username});
    plain_config.set_password(ss::sstring{password});
    authn_config.set_plain_configuration(std::move(plain_config));
    shadow_link_client_options.set_authentication_configuration(
      std::move(authn_config));

    shadow_link_client_options.set_bootstrap_servers({"localhost:9092"});
    shadow_link_configurations.set_client_options(
      std::move(shadow_link_client_options));

    shadow_link.set_configurations(std::move(shadow_link_configurations));
    shadow_link.set_name(ss::sstring{name});
    req.set_shadow_link(std::move(shadow_link));

    auto now = model::to_time_point(model::timestamp::now());
    auto md = admin::convert_create_to_metadata(std::move(req));

    ASSERT_TRUE(md.connection.authn_config.has_value());
    ASSERT_TRUE(
      std::holds_alternative<cluster_link::model::scram_credentials>(
        md.connection.authn_config.value()));
    const auto& md_authn_config
      = std::get<cluster_link::model::scram_credentials>(
        md.connection.authn_config.value());

    EXPECT_EQ(md_authn_config.username, username);
    EXPECT_EQ(md_authn_config.password, password);
    EXPECT_EQ(md_authn_config.mechanism, "PLAIN");
    auto pwd_updated = model::to_time_point(
      md_authn_config.password_last_updated);
    // Expect the password updated time to be within 10s
    EXPECT_GE(pwd_updated, now - 5s);
    EXPECT_LE(pwd_updated, now + 5s);
}

TEST(converter_test, create_with_tls_flag_only) {
    const auto name = "test-link";
    proto::admin::shadow_link shadow_link;
    proto::admin::create_shadow_link_request req;
    proto::admin::shadow_link_configurations shadow_link_configurations;
    proto::admin::shadow_link_client_options shadow_link_client_options;
    proto::common::tls_settings tls_settings;

    tls_settings.set_enabled(true);
    shadow_link_client_options.set_tls_settings(std::move(tls_settings));

    shadow_link_client_options.set_bootstrap_servers({"localhost:9092"});
    shadow_link_configurations.set_client_options(
      std::move(shadow_link_client_options));

    shadow_link.set_configurations(std::move(shadow_link_configurations));
    shadow_link.set_name(ss::sstring{name});
    req.set_shadow_link(std::move(shadow_link));

    auto md = ss::make_lw_shared<cluster_link::model::metadata>(
      admin::convert_create_to_metadata(std::move(req)));

    EXPECT_TRUE(md->connection.tls_enabled);
    EXPECT_FALSE(md->connection.ca.has_value());
    EXPECT_FALSE(md->connection.key.has_value());
    EXPECT_FALSE(md->connection.cert.has_value());
    EXPECT_TRUE(md->connection.tls_provide_sni);

    auto sl = admin::metadata_to_shadow_link(std::move(md), {});

    ASSERT_TRUE(
      sl.get_configurations().get_client_options().has_tls_settings());

    EXPECT_TRUE(sl.get_configurations()
                  .get_client_options()
                  .get_tls_settings()
                  .get_enabled());

    EXPECT_FALSE(sl.get_configurations()
                   .get_client_options()
                   .get_tls_settings()
                   .has_tls_file_settings());
    EXPECT_FALSE(sl.get_configurations()
                   .get_client_options()
                   .get_tls_settings()
                   .has_tls_pem_settings());
}

TEST(converter_test, create_with_tls_files) {
    const auto name = "test-link";
    const auto ca_file = "ca_file.pem";
    const auto key_file = "key_file.pem";
    const auto cert_file = "cert_file.pem";
    proto::admin::shadow_link shadow_link;
    proto::admin::create_shadow_link_request req;
    proto::admin::shadow_link_configurations shadow_link_configurations;
    proto::admin::shadow_link_client_options shadow_link_client_options;
    proto::common::tls_settings tls_settings;
    proto::common::tls_file_settings tls_file_settings;

    tls_settings.set_enabled(true);
    tls_file_settings.set_ca_path(ss::sstring{ca_file});
    tls_file_settings.set_key_path(ss::sstring{key_file});
    tls_file_settings.set_cert_path(ss::sstring{cert_file});
    tls_settings.set_tls_file_settings(std::move(tls_file_settings));
    tls_settings.set_do_not_set_sni_hostname(true);
    shadow_link_client_options.set_tls_settings(std::move(tls_settings));

    shadow_link_client_options.set_bootstrap_servers({"localhost:9092"});
    shadow_link_configurations.set_client_options(
      std::move(shadow_link_client_options));

    shadow_link.set_configurations(std::move(shadow_link_configurations));
    shadow_link.set_name(ss::sstring{name});
    req.set_shadow_link(std::move(shadow_link));

    auto md = admin::convert_create_to_metadata(std::move(req));
    EXPECT_TRUE(md.connection.tls_enabled);
    EXPECT_FALSE(md.connection.tls_provide_sni);
    ASSERT_TRUE(md.connection.ca.has_value());
    ASSERT_TRUE(
      std::holds_alternative<cluster_link::model::tls_file_path>(
        md.connection.ca.value()));
    EXPECT_EQ(
      std::get<cluster_link::model::tls_file_path>(md.connection.ca.value()),
      ca_file);
    ASSERT_TRUE(md.connection.key.has_value());
    ASSERT_TRUE(
      std::holds_alternative<cluster_link::model::tls_file_path>(
        md.connection.key.value()));
    EXPECT_EQ(
      std::get<cluster_link::model::tls_file_path>(md.connection.key.value()),
      key_file);
    ASSERT_TRUE(md.connection.cert.has_value());
    ASSERT_TRUE(
      std::holds_alternative<cluster_link::model::tls_file_path>(
        md.connection.cert.value()));
    EXPECT_EQ(
      std::get<cluster_link::model::tls_file_path>(md.connection.cert.value()),
      cert_file);
}

TEST(converter_test, create_with_tls_files_invalid) {
    const auto name = "test-link";
    const auto key_file = "key_file.pem";
    proto::admin::shadow_link shadow_link;
    proto::admin::create_shadow_link_request req;
    proto::admin::shadow_link_configurations shadow_link_configurations;
    proto::admin::shadow_link_client_options shadow_link_client_options;
    proto::common::tls_settings tls_settings;
    proto::common::tls_file_settings tls_file_settings;

    tls_file_settings.set_key_path(ss::sstring{key_file});
    tls_settings.set_tls_file_settings(std::move(tls_file_settings));
    shadow_link_client_options.set_tls_settings(std::move(tls_settings));

    shadow_link_client_options.set_bootstrap_servers({"localhost:9092"});
    shadow_link_configurations.set_client_options(
      std::move(shadow_link_client_options));

    shadow_link.set_configurations(std::move(shadow_link_configurations));
    shadow_link.set_name(ss::sstring{name});
    req.set_shadow_link(std::move(shadow_link));

    EXPECT_THROW(
      admin::convert_create_to_metadata(std::move(req)),
      serde::pb::rpc::invalid_argument_exception);
}

TEST(converter_test, create_with_tls_value) {
    const auto name = "test-link";
    const auto ca = "ca";
    const auto key = "key";
    const auto cert = "cert";
    proto::admin::shadow_link shadow_link;
    proto::admin::create_shadow_link_request req;
    proto::admin::shadow_link_configurations shadow_link_configurations;
    proto::admin::shadow_link_client_options shadow_link_client_options;
    proto::common::tls_settings tls_settings;
    proto::common::tlspem_settings tls_pem_settings;
    tls_pem_settings.set_ca(iobuf::from(ca));
    tls_pem_settings.set_key(iobuf::from(key));
    tls_pem_settings.set_cert(iobuf::from(cert));
    tls_settings.set_tls_pem_settings(std::move(tls_pem_settings));

    shadow_link_client_options.set_tls_settings(std::move(tls_settings));
    shadow_link_client_options.set_bootstrap_servers({"localhost:9092"});
    shadow_link_configurations.set_client_options(
      std::move(shadow_link_client_options));

    shadow_link.set_configurations(std::move(shadow_link_configurations));
    shadow_link.set_name(ss::sstring{name});
    req.set_shadow_link(std::move(shadow_link));

    auto md = admin::convert_create_to_metadata(std::move(req));
    ASSERT_TRUE(md.connection.ca.has_value());
    ASSERT_TRUE(
      std::holds_alternative<cluster_link::model::tls_value>(
        md.connection.ca.value()));
    EXPECT_EQ(
      std::get<cluster_link::model::tls_value>(md.connection.ca.value()), ca);
    ASSERT_TRUE(md.connection.key.has_value());
    ASSERT_TRUE(
      std::holds_alternative<cluster_link::model::tls_value>(
        md.connection.key.value()));
    EXPECT_EQ(
      std::get<cluster_link::model::tls_value>(md.connection.key.value()), key);
    ASSERT_TRUE(md.connection.cert.has_value());
    ASSERT_TRUE(
      std::holds_alternative<cluster_link::model::tls_value>(
        md.connection.cert.value()));
    EXPECT_EQ(
      std::get<cluster_link::model::tls_value>(md.connection.cert.value()),
      cert);
}

TEST(converter_test, create_with_tls_value_invalid) {
    const auto name = "test-link";
    const auto key = "key";
    proto::admin::shadow_link shadow_link;
    proto::admin::create_shadow_link_request req;
    proto::admin::shadow_link_configurations shadow_link_configurations;
    proto::admin::shadow_link_client_options shadow_link_client_options;
    proto::common::tls_settings tls_settings;
    proto::common::tlspem_settings tls_pem_settings;
    tls_pem_settings.set_key(iobuf::from(key));

    tls_settings.set_tls_pem_settings(std::move(tls_pem_settings));

    shadow_link_client_options.set_tls_settings(std::move(tls_settings));
    shadow_link_client_options.set_bootstrap_servers({"localhost:9092"});
    shadow_link_configurations.set_client_options(
      std::move(shadow_link_client_options));

    shadow_link.set_configurations(std::move(shadow_link_configurations));
    shadow_link.set_name(ss::sstring{name});
    req.set_shadow_link(std::move(shadow_link));

    EXPECT_THROW(
      admin::convert_create_to_metadata(std::move(req)),
      serde::pb::rpc::invalid_argument_exception);
}

TEST(converter_test, create_with_client_options) {
    const auto metadata_max_age_ms = 1;
    const auto connection_timeout_ms = 1;
    const auto retry_backoff_ms = 1;
    const auto fetch_wait_max_ms = 1;
    const auto fetch_min_bytes = 2;
    const auto fetch_max_bytes = 3;
    const auto name = "test-link";

    proto::admin::shadow_link shadow_link;
    proto::admin::create_shadow_link_request req;
    proto::admin::shadow_link_configurations shadow_link_configurations;
    proto::admin::shadow_link_client_options shadow_link_client_options;
    shadow_link_client_options.set_metadata_max_age_ms(metadata_max_age_ms);
    shadow_link_client_options.set_connection_timeout_ms(connection_timeout_ms);
    shadow_link_client_options.set_retry_backoff_ms(retry_backoff_ms);
    shadow_link_client_options.set_fetch_wait_max_ms(fetch_wait_max_ms);
    shadow_link_client_options.set_fetch_min_bytes(fetch_min_bytes);
    shadow_link_client_options.set_fetch_max_bytes(fetch_max_bytes);

    shadow_link_client_options.set_bootstrap_servers({"localhost:9092"});
    shadow_link_configurations.set_client_options(
      std::move(shadow_link_client_options));

    shadow_link.set_configurations(std::move(shadow_link_configurations));
    shadow_link.set_name(ss::sstring{name});
    req.set_shadow_link(std::move(shadow_link));

    auto md = admin::convert_create_to_metadata(std::move(req));

    EXPECT_EQ(md.connection.get_metadata_max_age_ms(), metadata_max_age_ms);
    EXPECT_EQ(md.connection.get_connection_timeout_ms(), connection_timeout_ms);
    EXPECT_EQ(md.connection.get_retry_backoff_ms(), retry_backoff_ms);
    EXPECT_EQ(md.connection.get_fetch_wait_max_ms(), fetch_wait_max_ms);
    EXPECT_EQ(md.connection.get_fetch_min_bytes(), fetch_min_bytes);
    EXPECT_EQ(md.connection.get_fetch_max_bytes(), fetch_max_bytes);
}

proto::admin::name_filter create_name_filter(
  proto::admin::pattern_type pattern,
  proto::admin::filter_type filter,
  ss::sstring name) {
    proto::admin::name_filter nf;
    nf.set_pattern_type(pattern);
    nf.set_filter_type(filter);
    nf.set_name(std::move(name));
    return nf;
}

TEST(converter_test, create_with_metadata_sync_options) {
    const auto name = "test-link";
    proto::admin::shadow_link shadow_link;
    proto::admin::create_shadow_link_request req;
    proto::admin::shadow_link_configurations shadow_link_configurations;
    proto::admin::shadow_link_client_options shadow_link_client_options;
    proto::admin::topic_metadata_sync_options topic_metadata_sync_options;
    proto::admin::schema_registry_sync_options schema_registry_sync_options;

    topic_metadata_sync_options.set_interval(absl::Seconds(1));
    chunked_vector<proto::admin::name_filter> filters;
    filters.emplace_back(create_name_filter(
      proto::admin::pattern_type::literal,
      proto::admin::filter_type::include,
      "test-literal-include"));
    filters.emplace_back(create_name_filter(
      proto::admin::pattern_type::prefix,
      proto::admin::filter_type::exclude,
      "test-prefix-exclude"));
    topic_metadata_sync_options.set_auto_create_shadow_topic_filters(
      std::move(filters));
    topic_metadata_sync_options.set_synced_shadow_topic_properties({"prop"});
    topic_metadata_sync_options.set_exclude_default(true);

    schema_registry_sync_options.set_shadow_schema_registry_topic({});

    shadow_link_client_options.set_bootstrap_servers({"localhost:9092"});
    shadow_link_configurations.set_client_options(
      std::move(shadow_link_client_options));
    shadow_link_configurations.set_topic_metadata_sync_options(
      std::move(topic_metadata_sync_options));
    shadow_link_configurations.set_schema_registry_sync_options(
      std::move(schema_registry_sync_options));

    shadow_link.set_configurations(std::move(shadow_link_configurations));
    shadow_link.set_name(ss::sstring{name});
    req.set_shadow_link(std::move(shadow_link));

    auto md = admin::convert_create_to_metadata(std::move(req));

    EXPECT_EQ(md.configuration.topic_metadata_mirroring_cfg.task_interval, 1s);
    ASSERT_EQ(
      md.configuration.topic_metadata_mirroring_cfg.topic_properties_to_mirror
        .size(),
      1);
    EXPECT_EQ(
      *md.configuration.topic_metadata_mirroring_cfg.topic_properties_to_mirror
         .begin(),
      "prop");
    ASSERT_EQ(
      md.configuration.topic_metadata_mirroring_cfg.topic_name_filters.size(),
      2);
    EXPECT_TRUE(md.configuration.topic_metadata_mirroring_cfg.exclude_default);
    ASSERT_TRUE(md.configuration.schema_registry_sync_cfg
                  .sync_schema_registry_topic_mode.has_value());
    EXPECT_TRUE(
      std::holds_alternative<cluster_link::model::schema_registry_sync_config::
                               shadow_entire_schema_registry>(
        *md.configuration.schema_registry_sync_cfg
           .sync_schema_registry_topic_mode));

    chunked_vector<cluster_link::model::resource_name_filter_pattern> expected{
      cluster_link::model::resource_name_filter_pattern{
        .pattern_type = cluster_link::model::filter_pattern_type::literal,
        .filter = cluster_link::model::filter_type::include,
        .pattern = "test-literal-include"},
      cluster_link::model::resource_name_filter_pattern{
        .pattern_type = cluster_link::model::filter_pattern_type::prefix,
        .filter = cluster_link::model::filter_type::exclude,
        .pattern = "test-prefix-exclude",
      }};

    std::ranges::sort(
      md.configuration.topic_metadata_mirroring_cfg.topic_name_filters,
      [](const auto& a, const auto& b) { return a.pattern < b.pattern; });

    std::ranges::sort(expected, [](const auto& a, const auto& b) {
        return a.pattern < b.pattern;
    });

    EXPECT_EQ(
      expected,
      md.configuration.topic_metadata_mirroring_cfg.topic_name_filters);
}

TEST(converter_test, metadata_to_shadow_link) {
    auto uuid = uuid_t::create();
    auto md = ss::make_lw_shared<cluster_link::model::metadata>();
    md->name = cluster_link::model::name_t{"test-link"};
    md->uuid = cluster_link::model::uuid_t(uuid);
    md->connection.bootstrap_servers = {
      net::unresolved_address("localhost", 9092)};

    auto sl = admin::metadata_to_shadow_link(std::move(md), {});
    EXPECT_EQ(sl.get_name(), "test-link");
    EXPECT_EQ(sl.get_uid(), fmt::format("{}", uuid));

    const auto& client_options = sl.get_configurations().get_client_options();
    ASSERT_FALSE(client_options.get_bootstrap_servers().empty());
    EXPECT_EQ(client_options.get_bootstrap_servers()[0], "localhost:9092");
    EXPECT_EQ(client_options.get_metadata_max_age_ms(), 0);
    EXPECT_EQ(
      client_options.get_effective_metadata_max_age_ms(),
      cluster_link::model::connection_config::metadata_max_age_ms_default);
    EXPECT_EQ(client_options.get_connection_timeout_ms(), 0);
    EXPECT_EQ(
      client_options.get_effective_connection_timeout_ms(),
      cluster_link::model::connection_config::connection_timeout_ms_default);
    EXPECT_EQ(client_options.get_retry_backoff_ms(), 0);
    EXPECT_EQ(
      client_options.get_effective_retry_backoff_ms(),
      cluster_link::model::connection_config::retry_backoff_ms_default);
    EXPECT_EQ(client_options.get_fetch_wait_max_ms(), 0);
    EXPECT_EQ(
      client_options.get_effective_fetch_wait_max_ms(),
      cluster_link::model::connection_config::fetch_wait_max_ms_default);
    EXPECT_EQ(client_options.get_fetch_min_bytes(), 0);
    EXPECT_EQ(
      client_options.get_effective_fetch_min_bytes(),
      cluster_link::model::connection_config::fetch_min_bytes_default);
    EXPECT_EQ(client_options.get_fetch_max_bytes(), 0);
    EXPECT_EQ(
      client_options.get_effective_fetch_max_bytes(),
      cluster_link::model::connection_config::fetch_max_bytes_default);
    EXPECT_EQ(client_options.get_fetch_partition_max_bytes(), 0);
    EXPECT_EQ(
      client_options.get_effective_fetch_partition_max_bytes(),
      cluster_link::model::connection_config::
        default_fetch_partition_max_bytes);

    const auto& topic_metadata_sync_options
      = sl.get_configurations().get_topic_metadata_sync_options();
    EXPECT_EQ(topic_metadata_sync_options.get_interval(), absl::Seconds(0));
    EXPECT_EQ(
      topic_metadata_sync_options.get_effective_interval(),
      absl::FromChrono(
        cluster_link::model::topic_metadata_mirroring_config::
          task_interval_default));
    EXPECT_FALSE(topic_metadata_sync_options.get_paused());

    const auto& security_settings
      = sl.get_configurations().get_security_sync_options();

    EXPECT_EQ(security_settings.get_interval(), absl::Seconds(0));
    EXPECT_EQ(
      security_settings.get_effective_interval(),
      absl::FromChrono(
        cluster_link::model::security_settings_sync_config::
          task_interval_default));
    EXPECT_FALSE(security_settings.get_paused());

    const auto& cg_settings
      = sl.get_configurations().get_consumer_offset_sync_options();
    EXPECT_EQ(cg_settings.get_interval(), absl::Seconds(0));
    EXPECT_EQ(
      cg_settings.get_effective_interval(),
      absl::FromChrono(
        cluster_link::model::consumer_groups_mirroring_config::
          default_task_interval));
    EXPECT_FALSE(cg_settings.get_paused());
}

TEST(converter_test, metadata_to_shadow_link_tasks_disabled) {
    auto uuid = uuid_t::create();
    auto md = ss::make_lw_shared<cluster_link::model::metadata>();
    md->name = cluster_link::model::name_t{"test-link"};
    md->uuid = cluster_link::model::uuid_t(uuid);
    md->connection.bootstrap_servers = {
      net::unresolved_address("localhost", 9092)};
    md->configuration.topic_metadata_mirroring_cfg.is_enabled
      = cluster_link::model::enabled_t::no;
    md->configuration.consumer_groups_mirroring_cfg.is_enabled
      = cluster_link::model::enabled_t::no;
    md->configuration.security_settings_sync_cfg.is_enabled
      = cluster_link::model::enabled_t::no;

    auto sl = admin::metadata_to_shadow_link(std::move(md), {});

    const auto& topic_metadata_sync_options
      = sl.get_configurations().get_topic_metadata_sync_options();
    EXPECT_TRUE(topic_metadata_sync_options.get_paused());

    const auto& security_settings
      = sl.get_configurations().get_security_sync_options();
    EXPECT_TRUE(security_settings.get_paused());

    const auto& cg_settings
      = sl.get_configurations().get_consumer_offset_sync_options();
    EXPECT_TRUE(cg_settings.get_paused());
}

TEST(converter_test, metadata_to_shadow_link_authn_scram_256) {
    auto md = ss::make_lw_shared<cluster_link::model::metadata>();
    md->connection.authn_config = cluster_link::model::scram_credentials{
      .username = "test-user",
      .password = "test-password",
      .mechanism = "SCRAM-SHA-256"};

    auto sl = admin::metadata_to_shadow_link(std::move(md), {});

    const auto& client_options = sl.get_configurations().get_client_options();
    ASSERT_TRUE(client_options.has_authentication_configuration());
    ASSERT_TRUE(client_options.get_authentication_configuration()
                  .has_scram_configuration());
    const auto& scram_config = client_options.get_authentication_configuration()
                                 .get_scram_configuration();
    EXPECT_EQ(scram_config.get_username(), "test-user");
    EXPECT_TRUE(scram_config.get_password_set());
    EXPECT_TRUE(scram_config.get_password().empty());
    EXPECT_EQ(
      scram_config.get_scram_mechanism(),
      proto::admin::scram_mechanism::scram_sha_256);
}

TEST(converter_test, metadata_to_shadow_link_authn_scram_512) {
    auto md = ss::make_lw_shared<cluster_link::model::metadata>();
    md->connection.authn_config = cluster_link::model::scram_credentials{
      .username = "test-user",
      .password = "test-password",
      .mechanism = "SCRAM-SHA-512"};

    auto sl = admin::metadata_to_shadow_link(std::move(md), {});

    const auto& client_options = sl.get_configurations().get_client_options();
    ASSERT_TRUE(client_options.has_authentication_configuration());
    ASSERT_TRUE(client_options.get_authentication_configuration()
                  .has_scram_configuration());
    const auto& scram_config = client_options.get_authentication_configuration()
                                 .get_scram_configuration();
    EXPECT_EQ(scram_config.get_username(), "test-user");
    EXPECT_TRUE(scram_config.get_password_set());
    EXPECT_TRUE(scram_config.get_password().empty());
    EXPECT_EQ(
      scram_config.get_scram_mechanism(),
      proto::admin::scram_mechanism::scram_sha_512);
}

TEST(converter_test, metadata_to_shadow_link_authn_invalid_scram) {
    auto md = ss::make_lw_shared<cluster_link::model::metadata>();
    md->connection.authn_config = cluster_link::model::scram_credentials{
      .username = "test-user",
      .password = "test-password",
      .mechanism = "SCRAM-SHA-NOPE"};
    EXPECT_THROW(
      admin::metadata_to_shadow_link(std::move(md), {}), std::invalid_argument);
}

TEST(converter_test, metadata_to_shadow_link_authn_plain) {
    auto md = ss::make_lw_shared<cluster_link::model::metadata>();
    md->connection.authn_config = cluster_link::model::scram_credentials{
      .username = "test-user",
      .password = "test-password",
      .mechanism = "PLAIN"};

    auto sl = admin::metadata_to_shadow_link(std::move(md), {});

    const auto& client_options = sl.get_configurations().get_client_options();
    ASSERT_TRUE(client_options.has_authentication_configuration());
    ASSERT_TRUE(client_options.get_authentication_configuration()
                  .has_plain_configuration());
    const auto& plain_config = client_options.get_authentication_configuration()
                                 .get_plain_configuration();
    EXPECT_EQ(plain_config.get_username(), "test-user");
    EXPECT_TRUE(plain_config.get_password_set());
    EXPECT_TRUE(plain_config.get_password().empty());
}

TEST(converter_test, metadata_to_shadow_link_tls_file) {
    const auto ca_file = "ca_file.pem";
    const auto key_file = "key_file.pem";
    const auto cert_file = "cert_file.pem";
    auto md = ss::make_lw_shared<cluster_link::model::metadata>();
    md->connection.ca = cluster_link::model::tls_file_path(ca_file);
    md->connection.key = cluster_link::model::tls_file_path(key_file);
    md->connection.cert = cluster_link::model::tls_file_path(cert_file);

    auto sl = admin::metadata_to_shadow_link(std::move(md), {});

    const auto& client_options = sl.get_configurations().get_client_options();
    ASSERT_TRUE(client_options.has_tls_settings());
    const auto& tls_settings = client_options.get_tls_settings();
    ASSERT_TRUE(tls_settings.has_tls_file_settings());
    const auto& tls_file_settings = tls_settings.get_tls_file_settings();
    EXPECT_EQ(tls_file_settings.get_ca_path(), ca_file);
    EXPECT_EQ(tls_file_settings.get_key_path(), key_file);
    EXPECT_EQ(tls_file_settings.get_cert_path(), cert_file);
    EXPECT_FALSE(tls_settings.get_do_not_set_sni_hostname());
}

TEST(converter_test, metadata_to_shadow_link_tls_file_on_ca) {
    const auto ca_file = "ca_file.pem";
    auto md = ss::make_lw_shared<cluster_link::model::metadata>();
    md->connection.ca = cluster_link::model::tls_file_path(ca_file);
    md->connection.tls_provide_sni
      = cluster_link::model::connection_config::tls_provide_sni_t::no;

    auto sl = admin::metadata_to_shadow_link(std::move(md), {});

    const auto& client_options = sl.get_configurations().get_client_options();
    ASSERT_TRUE(client_options.has_tls_settings());
    const auto& tls_settings = client_options.get_tls_settings();
    ASSERT_TRUE(tls_settings.has_tls_file_settings());
    const auto& tls_file_settings = tls_settings.get_tls_file_settings();
    EXPECT_EQ(tls_file_settings.get_ca_path(), ca_file);
    EXPECT_TRUE(tls_file_settings.get_key_path().empty());
    EXPECT_TRUE(tls_file_settings.get_cert_path().empty());
    EXPECT_TRUE(tls_settings.get_do_not_set_sni_hostname());
}

TEST(converter_test, metadata_to_shadow_link_tls_value) {
    const auto ca = "ca";
    const auto key = "key";
    const auto cert = "cert";

    auto md = ss::make_lw_shared<cluster_link::model::metadata>();
    md->connection.ca = cluster_link::model::tls_value(ca);
    md->connection.key = cluster_link::model::tls_value(key);
    md->connection.cert = cluster_link::model::tls_value(cert);

    auto sl = admin::metadata_to_shadow_link(std::move(md), {});

    const auto& client_options = sl.get_configurations().get_client_options();
    ASSERT_TRUE(client_options.has_tls_settings());
    const auto& tls_settings = client_options.get_tls_settings();
    ASSERT_TRUE(tls_settings.has_tls_pem_settings());
    const auto& tls_value_settings = tls_settings.get_tls_pem_settings();
    EXPECT_EQ(tls_value_settings.get_ca(), ca);
    EXPECT_EQ(tls_value_settings.get_key(), "");
    EXPECT_EQ(
      tls_value_settings.get_key_fingerprint(),
      bytes_to_base64(crypto::digest(crypto::digest_type::SHA256, key)));
    EXPECT_EQ(tls_value_settings.get_cert(), cert);
}

TEST(converter_test, metadata_to_shadow_link_tls_value_only_ca) {
    const auto ca = "ca";

    auto md = ss::make_lw_shared<cluster_link::model::metadata>();
    md->connection.ca = cluster_link::model::tls_value(ca);

    auto sl = admin::metadata_to_shadow_link(std::move(md), {});

    const auto& client_options = sl.get_configurations().get_client_options();
    ASSERT_TRUE(client_options.has_tls_settings());
    const auto& tls_settings = client_options.get_tls_settings();
    ASSERT_TRUE(tls_settings.has_tls_pem_settings());
    const auto& tls_value_settings = tls_settings.get_tls_pem_settings();
    EXPECT_EQ(tls_value_settings.get_ca(), ca);
    EXPECT_TRUE(tls_value_settings.get_key().empty());
    EXPECT_TRUE(tls_value_settings.get_cert().empty());
}

TEST(converter_test, metadata_to_shadow_link_mismatch_tls) {
    {
        auto md = ss::make_lw_shared<cluster_link::model::metadata>();
        md->connection.ca = cluster_link::model::tls_file_path("ca");
        md->connection.key = cluster_link::model::tls_value("key");
        md->connection.cert = cluster_link::model::tls_value("cert");

        EXPECT_THROW(
          admin::metadata_to_shadow_link(std::move(md), {}),
          std::invalid_argument);
    }
    {
        auto md = ss::make_lw_shared<cluster_link::model::metadata>();
        md->connection.ca = cluster_link::model::tls_value("ca");
        md->connection.key = cluster_link::model::tls_file_path("key");
        md->connection.cert = cluster_link::model::tls_file_path("cert");

        EXPECT_THROW(
          admin::metadata_to_shadow_link(std::move(md), {}),
          std::invalid_argument);
    }
    {
        auto md = ss::make_lw_shared<cluster_link::model::metadata>();
        md->connection.ca = cluster_link::model::tls_file_path("ca");
        md->connection.key = cluster_link::model::tls_file_path("key");
        md->connection.cert = cluster_link::model::tls_value("cert");

        EXPECT_THROW(
          admin::metadata_to_shadow_link(std::move(md), {}),
          std::invalid_argument);
    }
}

TEST(converter_test, metadata_to_shadow_link_topic_mirroring_cfg) {
    const auto interval = 15s;
    auto md = ss::make_lw_shared<cluster_link::model::metadata>();
    md->configuration.topic_metadata_mirroring_cfg.task_interval = interval;
    md->configuration.topic_metadata_mirroring_cfg.topic_properties_to_mirror
      = {"prop1", "prop2"};
    md->configuration.topic_metadata_mirroring_cfg.topic_name_filters = {
      {.pattern_type = cluster_link::model::filter_pattern_type::literal,
       .filter = cluster_link::model::filter_type::include,
       .pattern = "test-literal-include"},
      {
        .pattern_type = cluster_link::model::filter_pattern_type::prefix,
        .filter = cluster_link::model::filter_type::exclude,
        .pattern = "test-prefix-exclude",
      }};
    md->configuration.topic_metadata_mirroring_cfg.exclude_default = true;

    md->configuration.schema_registry_sync_cfg.sync_schema_registry_topic_mode
      = cluster_link::model::schema_registry_sync_config::
        shadow_entire_schema_registry{};

    auto sl = admin::metadata_to_shadow_link(std::move(md), {});
    const auto& topic_metadata_sync_options
      = sl.get_configurations().get_topic_metadata_sync_options();

    EXPECT_EQ(topic_metadata_sync_options.get_interval(), absl::Seconds(15));
    chunked_vector<ss::sstring> expected_properties{"prop1", "prop2"};
    std::ranges::sort(expected_properties);
    auto synced_shadow_topic_properties = copy_chunked_vector(
      topic_metadata_sync_options.get_synced_shadow_topic_properties());
    std::ranges::sort(synced_shadow_topic_properties);
    EXPECT_EQ(synced_shadow_topic_properties, expected_properties);

    chunked_vector<proto::admin::name_filter> expected_filters;
    expected_filters.reserve(2);
    expected_filters.emplace_back(create_name_filter(
      proto::admin::pattern_type::literal,
      proto::admin::filter_type::include,
      "test-literal-include"));
    expected_filters.emplace_back(create_name_filter(
      proto::admin::pattern_type::prefix,
      proto::admin::filter_type::exclude,
      "test-prefix-exclude"));

    EXPECT_EQ(
      topic_metadata_sync_options.get_auto_create_shadow_topic_filters(),
      expected_filters);

    EXPECT_TRUE(topic_metadata_sync_options.get_exclude_default());

    const auto& schema_registry_sync_options
      = sl.get_configurations().get_schema_registry_sync_options();
    EXPECT_TRUE(
      schema_registry_sync_options.has_shadow_schema_registry_topic());
}

proto::admin::shadow_topic
create_shadow_topic(ss::sstring name, proto::admin::shadow_topic_state state) {
    proto::admin::shadow_topic st;
    proto::admin::shadow_topic_status sts;

    st.set_name(std::move(name));
    sts.set_state(state);
    st.set_status(std::move(sts));

    return st;
}

TEST(converter_test, metadata_to_shadow_link_topic_status) {
    auto md = ss::make_lw_shared<cluster_link::model::metadata>();
    cluster_link::model::link_state::mirror_topics_t mirror_topic_states;
    mirror_topic_states[model::topic{"active"}]
      = cluster_link::model::mirror_topic_metadata{
        .status = cluster_link::model::mirror_topic_status::active};
    mirror_topic_states[model::topic{"failed"}]
      = cluster_link::model::mirror_topic_metadata{
        .status = cluster_link::model::mirror_topic_status::failed};
    mirror_topic_states[model::topic{"paused"}]
      = cluster_link::model::mirror_topic_metadata{
        .status = cluster_link::model::mirror_topic_status::paused};
    mirror_topic_states[model::topic{"promoted"}]
      = cluster_link::model::mirror_topic_metadata{
        .status = cluster_link::model::mirror_topic_status::promoted};

    md->state.set_mirror_topics(std::move(mirror_topic_states));

    auto sl = admin::metadata_to_shadow_link(std::move(md), {});

    auto& mirror_topics = sl.get_status().get_shadow_topics();

    chunked_vector<proto::admin::shadow_topic> expected;
    expected.reserve(4);
    expected.emplace_back(
      create_shadow_topic("active", proto::admin::shadow_topic_state::active));
    expected.emplace_back(
      create_shadow_topic("failed", proto::admin::shadow_topic_state::faulted));
    expected.emplace_back(
      create_shadow_topic("paused", proto::admin::shadow_topic_state::paused));
    expected.emplace_back(create_shadow_topic(
      "promoted", proto::admin::shadow_topic_state::promoted));

    std::ranges::sort(mirror_topics, [](const auto& a, const auto& b) {
        return a.get_name() < b.get_name();
    });

    std::ranges::sort(expected, [](const auto& a, const auto& b) {
        return a.get_name() < b.get_name();
    });

    EXPECT_EQ(mirror_topics, expected);
}

TEST(converter_test, update_shadow_link_add_field) {
    cluster_link::model::metadata current_md;
    current_md.name = cluster_link::model::name_t{"test-link"};
    current_md.uuid = cluster_link::model::uuid_t{uuid_t::create()};
    current_md.connection.bootstrap_servers = {
      net::unresolved_address("localhost", 9092)};
    admin::set_client_id(current_md);

    proto::admin::update_shadow_link_request req;
    req.get_shadow_link()
      .get_configurations()
      .get_topic_metadata_sync_options()
      .set_interval(absl::Seconds(300));
    serde::pb::field_mask mask;
    mask.paths.emplace_back(
      serde::pb::field_mask::path{
        "configurations", "topic_metadata_sync_options", "interval"});
    req.set_update_mask(std::move(mask));

    auto update_cmd = admin::create_update_cluster_link_config_cmd(
      std::move(req),
      ss::make_lw_shared<cluster_link::model::metadata>({
        .name = current_md.name,
        .uuid = current_md.uuid,
        .connection = current_md.connection,
        .configuration = current_md.configuration.copy(),
      }));

    EXPECT_EQ(update_cmd.connection, current_md.connection);
    EXPECT_NE(update_cmd.link_config, current_md.configuration);
    EXPECT_EQ(
      update_cmd.link_config.topic_metadata_mirroring_cfg.get_task_interval(),
      300s);
}

TEST(converter_test, update_scram_creds) {
    cluster_link::model::metadata current_md;
    current_md.name = cluster_link::model::name_t{"test-link"};
    current_md.uuid = cluster_link::model::uuid_t{uuid_t::create()};
    current_md.connection.bootstrap_servers = {
      net::unresolved_address("localhost", 9092)};
    current_md.connection.authn_config = cluster_link::model::scram_credentials{
      .username = "old-user",
      .password = "old-password",
      .mechanism = "SCRAM-SHA-256",
      .password_last_updated = model::to_timestamp(
        std::chrono::system_clock::now() - 1h)};
    admin::set_client_id(current_md);

    proto::admin::scram_config scram_config;
    scram_config.set_username("new-user");
    scram_config.set_password("new-password");
    scram_config.set_scram_mechanism(
      proto::admin::scram_mechanism::scram_sha_512);

    proto::admin::authentication_configuration authn_config;
    authn_config.set_scram_configuration(std::move(scram_config));

    proto::admin::update_shadow_link_request req;
    req.get_shadow_link()
      .get_configurations()
      .get_client_options()
      .set_authentication_configuration(std::move(authn_config));

    serde::pb::field_mask mask;
    mask.paths.emplace_back(
      serde::pb::field_mask::path{
        "configurations", "client_options", "authentication_configuration"});
    req.set_update_mask(std::move(mask));

    auto now = model::to_time_point(model::timestamp::now());
    auto update_cmd = admin::create_update_cluster_link_config_cmd(
      std::move(req),
      ss::make_lw_shared<cluster_link::model::metadata>({
        .name = current_md.name,
        .uuid = current_md.uuid,
        .connection = current_md.connection,
        .configuration = current_md.configuration.copy(),
      }));

    const auto& new_scram_config
      = std::get<cluster_link::model::scram_credentials>(
        update_cmd.connection.authn_config.value());

    EXPECT_EQ(new_scram_config.username, "new-user");
    EXPECT_EQ(new_scram_config.password, "new-password");
    EXPECT_EQ(new_scram_config.mechanism, "SCRAM-SHA-512");
    auto pwd_updated = model::to_time_point(
      new_scram_config.password_last_updated);
    // Expect the password updated time to be within 10s
    EXPECT_GE(pwd_updated, now - 5s);
    EXPECT_LE(pwd_updated, now + 5s);
}

TEST(converter_test, remove_scram_creds) {
    cluster_link::model::metadata current_md;
    current_md.name = cluster_link::model::name_t{"test-link"};
    current_md.uuid = cluster_link::model::uuid_t{uuid_t::create()};
    current_md.connection.bootstrap_servers = {
      net::unresolved_address("localhost", 9092)};
    current_md.connection.authn_config = cluster_link::model::scram_credentials{
      .username = "old-user",
      .password = "old-password",
      .mechanism = "SCRAM-SHA-256",
      .password_last_updated = model::to_timestamp(
        std::chrono::system_clock::now() - 1h)};
    admin::set_client_id(current_md);

    proto::admin::update_shadow_link_request req;
    serde::pb::field_mask mask;
    mask.paths.emplace_back(
      serde::pb::field_mask::path{
        "configurations", "client_options", "authentication_configuration"});
    req.set_update_mask(std::move(mask));

    auto update_cmd = admin::create_update_cluster_link_config_cmd(
      std::move(req),
      ss::make_lw_shared<cluster_link::model::metadata>({
        .name = current_md.name,
        .uuid = current_md.uuid,
        .connection = current_md.connection,
        .configuration = current_md.configuration.copy(),
      }));
    EXPECT_EQ(update_cmd.connection.authn_config, std::nullopt);
}

TEST(converter_test, do_not_update_scram_creds) {
    cluster_link::model::metadata current_md;
    current_md.name = cluster_link::model::name_t{"test-link"};
    current_md.uuid = cluster_link::model::uuid_t{uuid_t::create()};
    current_md.connection.bootstrap_servers = {
      net::unresolved_address("localhost", 9092)};
    current_md.connection.authn_config = cluster_link::model::scram_credentials{
      .username = "old-user",
      .password = "old-password",
      .mechanism = "SCRAM-SHA-256"};
    admin::set_client_id(current_md);

    proto::admin::update_shadow_link_request req;
    req.get_shadow_link()
      .get_configurations()
      .get_topic_metadata_sync_options()
      .set_interval(absl::Seconds(300));
    serde::pb::field_mask mask;
    mask.paths.emplace_back(
      serde::pb::field_mask::path{
        "configurations", "topic_metadata_sync_options", "interval"});
    req.set_update_mask(std::move(mask));

    auto update_cmd = admin::create_update_cluster_link_config_cmd(
      std::move(req),
      ss::make_lw_shared<cluster_link::model::metadata>({
        .name = current_md.name,
        .uuid = current_md.uuid,
        .connection = current_md.connection,
        .configuration = current_md.configuration.copy(),
      }));

    EXPECT_EQ(
      update_cmd.link_config.topic_metadata_mirroring_cfg.get_task_interval(),
      300s);

    const auto& new_scram_config
      = std::get<cluster_link::model::scram_credentials>(
        update_cmd.connection.authn_config.value());

    EXPECT_EQ(new_scram_config.username, "old-user");
    EXPECT_EQ(new_scram_config.password, "old-password");
    EXPECT_EQ(new_scram_config.mechanism, "SCRAM-SHA-256");
}

TEST(converter_test, invalid_scram_update) {
    cluster_link::model::metadata current_md;
    current_md.name = cluster_link::model::name_t{"test-link"};
    current_md.uuid = cluster_link::model::uuid_t{uuid_t::create()};
    current_md.connection.bootstrap_servers = {
      net::unresolved_address("localhost", 9092)};
    current_md.connection.authn_config = cluster_link::model::scram_credentials{
      .username = "old-user",
      .password = "old-password",
      .mechanism = "SCRAM-SHA-256"};
    admin::set_client_id(current_md);

    proto::admin::scram_config scram_config;
    // Do not set username, this should result in an error during update
    scram_config.set_password("new-password");
    scram_config.set_scram_mechanism(
      proto::admin::scram_mechanism::scram_sha_512);

    proto::admin::authentication_configuration authn_config;
    authn_config.set_scram_configuration(std::move(scram_config));

    proto::admin::update_shadow_link_request req;
    req.get_shadow_link()
      .get_configurations()
      .get_client_options()
      .set_authentication_configuration(std::move(authn_config));

    serde::pb::field_mask mask;
    mask.paths.emplace_back(
      serde::pb::field_mask::path{
        "configurations", "client_options", "authentication_configuration"});
    req.set_update_mask(std::move(mask));

    EXPECT_THROW(
      admin::create_update_cluster_link_config_cmd(
        std::move(req),
        ss::make_lw_shared<cluster_link::model::metadata>({
          .name = current_md.name,
          .uuid = current_md.uuid,
          .connection = current_md.connection,
          .configuration = current_md.configuration.copy(),
        })),
      serde::pb::rpc::invalid_argument_exception);
}

TEST(converter_test, test_update_tls_value) {
    cluster_link::model::metadata current_md;
    current_md.name = cluster_link::model::name_t{"test-link"};
    current_md.uuid = cluster_link::model::uuid_t{uuid_t::create()};
    current_md.connection.bootstrap_servers = {
      net::unresolved_address("localhost", 9092)};
    current_md.connection.ca = cluster_link::model::tls_value("old-ca");
    current_md.connection.key = cluster_link::model::tls_value("old-key");
    current_md.connection.cert = cluster_link::model::tls_value("old-cert");

    admin::set_client_id(current_md);

    proto::common::tlspem_settings tls_pem_settings;
    tls_pem_settings.set_ca(iobuf::from("new-ca"));
    tls_pem_settings.set_key(iobuf::from("new-key"));
    tls_pem_settings.set_cert(iobuf::from("new-cert"));

    proto::common::tls_settings tls_settings;
    tls_settings.set_tls_pem_settings(std::move(tls_pem_settings));

    proto::admin::update_shadow_link_request req;
    req.get_shadow_link()
      .get_configurations()
      .get_client_options()
      .set_tls_settings(std::move(tls_settings));

    serde::pb::field_mask mask;
    mask.paths.emplace_back(
      serde::pb::field_mask::path{
        "configurations", "client_options", "tls_settings"});
    req.set_update_mask(std::move(mask));

    auto update_cmd = admin::create_update_cluster_link_config_cmd(
      std::move(req),
      ss::make_lw_shared<cluster_link::model::metadata>({
        .name = current_md.name,
        .uuid = current_md.uuid,
        .connection = current_md.connection,
        .configuration = current_md.configuration.copy(),
      }));

    EXPECT_EQ(
      std::get<cluster_link::model::tls_value>(*update_cmd.connection.ca),
      "new-ca");
    EXPECT_EQ(
      std::get<cluster_link::model::tls_value>(*update_cmd.connection.key),
      "new-key");
    EXPECT_EQ(
      std::get<cluster_link::model::tls_value>(*update_cmd.connection.cert),
      "new-cert");
}

TEST(converter_test, metadata_to_shadow_link_security) {
    auto md = ss::make_lw_shared<cluster_link::model::metadata>();
    md->configuration.security_settings_sync_cfg.task_interval = 10s;
    md->configuration.security_settings_sync_cfg.acl_filters.emplace_back(
      cluster_link::model::acl_filter{
        .resource_filter = cluster_link::model::
          acl_resource_filter{.resource_type = cluster_link::model::acl_resource::any, .pattern_type = cluster_link::model::acl_pattern::any, .name = "*"},
        .access_filter = cluster_link::model::
          acl_access_filter{.principal = "User:*", .operation = cluster_link::model::acl_operation::any, .permission_type = cluster_link::model::acl_permission_type::any, .host = "*"},
      });

    auto sl = admin::metadata_to_shadow_link(std::move(md), {});

    const auto& security_settings
      = sl.get_configurations().get_security_sync_options();

    EXPECT_EQ(security_settings.get_interval(), absl::Seconds(10));
    ASSERT_EQ(security_settings.get_acl_filters().size(), 1);

    const auto& filter = security_settings.get_acl_filters()[0];
    EXPECT_EQ(filter.get_access_filter().get_principal(), "User:*");
    EXPECT_EQ(
      filter.get_access_filter().get_operation(),
      proto::common::acl_operation::any);
    EXPECT_EQ(
      filter.get_access_filter().get_permission_type(),
      proto::common::acl_permission_type::any);
    EXPECT_EQ(filter.get_access_filter().get_host(), "*");

    EXPECT_EQ(
      filter.get_resource_filter().get_resource_type(),
      proto::common::acl_resource::any);
    EXPECT_EQ(
      filter.get_resource_filter().get_pattern_type(),
      proto::common::acl_pattern::any);
    EXPECT_EQ(filter.get_resource_filter().get_name(), "*");
}

TEST(converter_test, shadow_link_to_metadata_security) {
    proto::admin::shadow_link shadow_link;
    proto::admin::create_shadow_link_request req;
    proto::admin::shadow_link_configurations shadow_link_configurations;
    proto::admin::security_settings_sync_options security_settings;
    proto::admin::shadow_link_client_options shadow_link_client_options;

    shadow_link_client_options.set_bootstrap_servers({"localhost:9092"});
    shadow_link_configurations.set_client_options(
      std::move(shadow_link_client_options));

    security_settings.set_interval(absl::Seconds(10));
    chunked_vector<proto::admin::acl_filter> filters;
    proto::admin::acl_filter filter;
    proto::admin::acl_resource_filter resource_filter;
    resource_filter.set_resource_type(proto::common::acl_resource::any);
    resource_filter.set_pattern_type(proto::common::acl_pattern::any);
    resource_filter.set_name("*");
    filter.set_resource_filter(std::move(resource_filter));

    proto::admin::acl_access_filter access_filter;
    access_filter.set_host("*");
    access_filter.set_principal("User:*");
    access_filter.set_operation(proto::common::acl_operation::any);
    access_filter.set_permission_type(proto::common::acl_permission_type::any);
    filter.set_access_filter(std::move(access_filter));

    filters.emplace_back(std::move(filter));
    security_settings.set_acl_filters(std::move(filters));
    shadow_link_configurations.set_security_sync_options(
      std::move(security_settings));

    shadow_link.set_configurations(std::move(shadow_link_configurations));
    shadow_link.set_name("test-link");

    req.set_shadow_link(std::move(shadow_link));

    auto md = admin::convert_create_to_metadata(std::move(req));

    const auto& model_security = md.configuration.security_settings_sync_cfg;
    EXPECT_EQ(model_security.get_task_interval(), 10s);
    ASSERT_EQ(model_security.acl_filters.size(), 1);
    const auto& acl_filter = model_security.acl_filters[0];

    EXPECT_EQ(acl_filter.access_filter.principal, "User:*");
    EXPECT_EQ(acl_filter.access_filter.host, "*");
    EXPECT_EQ(
      acl_filter.access_filter.operation,
      cluster_link::model::acl_operation::any);
    EXPECT_EQ(
      acl_filter.access_filter.permission_type,
      cluster_link::model::acl_permission_type::any);

    EXPECT_EQ(
      acl_filter.resource_filter.resource_type,
      cluster_link::model::acl_resource::any);
    EXPECT_EQ(
      acl_filter.resource_filter.pattern_type,
      cluster_link::model::acl_pattern::any);
    EXPECT_EQ(acl_filter.resource_filter.name, "*");
}

namespace {
proto::admin::create_shadow_link_request create_base_link() {
    const auto name = "test-link";
    proto::admin::shadow_link shadow_link;
    proto::admin::create_shadow_link_request req;
    proto::admin::shadow_link_configurations shadow_link_configurations;
    proto::admin::shadow_link_client_options shadow_link_client_options;

    shadow_link_client_options.set_bootstrap_servers({"localhost:9092"});
    shadow_link_configurations.set_client_options(
      std::move(shadow_link_client_options));

    shadow_link.set_configurations(std::move(shadow_link_configurations));
    shadow_link.set_name(ss::sstring{name});
    req.set_shadow_link(std::move(shadow_link));

    return req;
}
} // namespace

TEST(converter_test, test_convert_timestamp) {
    {
        auto req = create_base_link();

        req.get_shadow_link()
          .get_configurations()
          .get_topic_metadata_sync_options()
          .set_start_at_earliest({});

        auto md = admin::convert_create_to_metadata(std::move(req));
        EXPECT_EQ(
          md.configuration.topic_metadata_mirroring_cfg.get_start_offset_ts(),
          cluster_link::model::earliest_offset_ts);
    }

    {
        auto req = create_base_link();

        req.get_shadow_link()
          .get_configurations()
          .get_topic_metadata_sync_options()
          .set_start_at_latest({});

        auto md = admin::convert_create_to_metadata(std::move(req));
        EXPECT_EQ(
          md.configuration.topic_metadata_mirroring_cfg.get_start_offset_ts(),
          cluster_link::model::latest_offset_ts);
    }

    {
        auto req = create_base_link();

        auto now = absl::Now();

        req.get_shadow_link()
          .get_configurations()
          .get_topic_metadata_sync_options()
          .set_start_at_timestamp(absl::Time{now});

        auto md = admin::convert_create_to_metadata(std::move(req));
        EXPECT_EQ(
          md.configuration.topic_metadata_mirroring_cfg.get_start_offset_ts(),
          model::timestamp{absl::ToUnixMillis(now)});
    }
}

namespace {
ss::lw_shared_ptr<cluster_link::model::metadata> create_base_metadata() {
    auto uuid = uuid_t::create();
    auto md = ss::make_lw_shared<cluster_link::model::metadata>();
    md->name = cluster_link::model::name_t{"test-link"};
    md->uuid = cluster_link::model::uuid_t(uuid);
    md->connection.bootstrap_servers = {
      net::unresolved_address("localhost", 9092)};

    return md;
}
} // namespace

TEST(converter_test, timestamp_to_string) {
    {
        auto md = create_base_metadata();
        auto sl = admin::metadata_to_shadow_link(std::move(md), {});

        sl.get_configurations()
          .get_topic_metadata_sync_options()
          .visit_start_offset(
            [](std::monostate) {},
            [](auto&&) { ADD_FAILURE() << "Expected unset starting offset"; });
    }
    {
        auto md = create_base_metadata();
        md->configuration.topic_metadata_mirroring_cfg.starting_offset
          = cluster_link::model::earliest_offset_ts;
        auto sl = admin::metadata_to_shadow_link(std::move(md), {});

        EXPECT_TRUE(sl.get_configurations()
                      .get_topic_metadata_sync_options()
                      .has_start_at_earliest())
          << "Expected earliest starting offset";
    }
    {
        auto md = create_base_metadata();
        md->configuration.topic_metadata_mirroring_cfg.starting_offset
          = cluster_link::model::latest_offset_ts;
        auto sl = admin::metadata_to_shadow_link(std::move(md), {});

        EXPECT_TRUE(sl.get_configurations()
                      .get_topic_metadata_sync_options()
                      .has_start_at_latest())
          << "Expected latest starting offset";
    }
    {
        auto md = create_base_metadata();
        md->configuration.topic_metadata_mirroring_cfg.starting_offset
          = model::timestamp{1759193250080};
        auto sl = admin::metadata_to_shadow_link(std::move(md), {});

        ASSERT_TRUE(sl.get_configurations()
                      .get_topic_metadata_sync_options()
                      .has_start_at_timestamp())
          << "Expected timestamp starting offset";

        auto ts = sl.get_configurations()
                    .get_topic_metadata_sync_options()
                    .get_start_at_timestamp();
        EXPECT_EQ(absl::FromUnixMillis(1759193250080), ts);
    }
}
