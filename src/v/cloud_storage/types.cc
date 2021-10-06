#include "cloud_storage/types.h"

#include "cloud_storage/logger.h"
#include "config/configuration.h"

namespace cloud_storage {

std::ostream& operator<<(std::ostream& o, const download_result& r) {
    switch (r) {
    case download_result::success:
        o << "{success}";
        break;
    case download_result::notfound:
        o << "{key_not_found}";
        break;
    case download_result::timedout:
        o << "{timed_out}";
        break;
    case download_result::failed:
        o << "{failed}";
        break;
    };
    return o;
}

std::ostream& operator<<(std::ostream& o, const upload_result& r) {
    switch (r) {
    case upload_result::success:
        o << "{success}";
        break;
    case upload_result::timedout:
        o << "{timed_out}";
        break;
    case upload_result::failed:
        o << "{failed}";
        break;
    };
    return o;
}

std::ostream& operator<<(std::ostream& o, const configuration& cfg) {
    fmt::print(
      o,
      "{{connection_limit: {}, client_config: {}, metrics_disabled: {}, "
      "bucket_name: {}",
      cfg.connection_limit,
      cfg.client_config,
      cfg.metrics_disabled,
      cfg.bucket_name);
    return o;
}

static ss::sstring get_value_or_throw(
  const config::property<std::optional<ss::sstring>>& prop, const char* name) {
    auto opt = prop.value();
    if (!opt) {
        vlog(
          cst_log.error,
          "Configuration property {} is required to enable archival storage",
          name);
        throw std::runtime_error(
          fmt::format("configuration property {} is not set", name));
    }
    return *opt;
}

ss::future<configuration> configuration::get_config() {
    vlog(cst_log.debug, "Generating archival configuration");
    auto secret_key = s3::private_key_str(get_value_or_throw(
      config::shard_local_cfg().cloud_storage_secret_key,
      "cloud_storage_secret_key"));
    auto access_key = s3::public_key_str(get_value_or_throw(
      config::shard_local_cfg().cloud_storage_access_key,
      "cloud_storage_access_key"));
    auto region = s3::aws_region_name(get_value_or_throw(
      config::shard_local_cfg().cloud_storage_region, "cloud_storage_region"));
    auto disable_metrics = rpc::metrics_disabled(
      config::shard_local_cfg().disable_metrics());

    // Set default overrides
    s3::default_overrides overrides;
    overrides.max_idle_time
      = config::shard_local_cfg()
          .cloud_storage_max_connection_idle_time_ms.value();
    if (auto optep
        = config::shard_local_cfg().cloud_storage_api_endpoint.value();
        optep.has_value()) {
        overrides.endpoint = s3::endpoint_url(*optep);
    }
    overrides.disable_tls = config::shard_local_cfg().cloud_storage_disable_tls;
    if (auto cert = config::shard_local_cfg().cloud_storage_trust_file.value();
        cert.has_value()) {
        overrides.trust_file = s3::ca_trust_file(std::filesystem::path(*cert));
    }
    overrides.port = config::shard_local_cfg().cloud_storage_api_endpoint_port;

    auto s3_conf = co_await s3::configuration::make_configuration(
      access_key, secret_key, region, overrides, disable_metrics);

    configuration cfg{
      .client_config = std::move(s3_conf),
      .connection_limit = s3_connection_limit(
        config::shard_local_cfg().cloud_storage_max_connections.value()),
      .metrics_disabled = remote_metrics_disabled(
        static_cast<bool>(disable_metrics)),
      .bucket_name = s3::bucket_name(get_value_or_throw(
        config::shard_local_cfg().cloud_storage_bucket,
        "cloud_storage_bucket")),
    };
    vlog(cst_log.debug, "Cloud storage configuration generated: {}", cfg);
    co_return cfg;
}

} // namespace cloud_storage
