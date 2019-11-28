#include "config/tls_config.h"
#include "rpc/logger.h"
#include "rpc/netbuf.h"
#include "rpc/parse_utils.h"
#include "rpc/server.h"
#include "rpc/types.h"
#include "seastarx.h"

#include <seastar/core/sharded.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/thread.hh>
#include <seastar/testing/thread_test_case.hh>

// manually generated via:
// rpcgen.py --service_file test_definitions.json --output_file rpcgen.h
#include "rpc/test/rpcgen.h"

using namespace std::chrono_literals; // NOLINT

static logger lgr{"cycling test"};

struct movistar final : cycling::team_movistar_service {
    movistar(scheduling_group& sc, smp_service_group& ssg)
      : cycling::team_movistar_service(sc, ssg) {}
    future<cycling::mount_tamalpais>
    ibis_hakka(cycling::san_francisco&&, rpc::streaming_context&) final {
        lgr.info("Finished calling ibis_hakka");
        return make_ready_future<cycling::mount_tamalpais>(
          cycling::mount_tamalpais{66});
    }
    future<cycling::nairo_quintana>
    canyon(cycling::ultimate_cf_slx&&, rpc::streaming_context&) final {
        lgr.info("Finished calling canyon");
        return make_ready_future<cycling::nairo_quintana>(
          cycling::nairo_quintana{32});
    }
};

SEASTAR_THREAD_TEST_CASE(rpcgen_integration) {
    std::cout.setf(std::ios::unitbuf);
    auto ssg = create_smp_service_group({5000}).get0();
    auto sc = create_scheduling_group("rpc scheduling group", 200).get0();
    rpc::server_configuration cfg;
    cfg.max_service_memory_per_core = memory::stats().total_memory() / 10;
    cfg.addrs.push_back(socket_address(ipv4_addr("127.0.0.1", 11118)));
    rpc::server s(cfg);
    s.register_service<movistar>(sc, ssg);
    lgr.info("Registered service, about to start");
    s.start();
    lgr.info("started");
    rpc::client_configuration client_cfg;
    client_cfg.server_addr = socket_address(ipv4_addr("127.0.0.1", 11118));
    auto cli = std::make_unique<movistar::client>(client_cfg);
    lgr.info("client connecting");
    cli->connect().get();
    lgr.info("client calling method");
    auto ret = cli->ibis_hakka(cycling::san_francisco{66}).get0();
    lgr.info("client stopping");
    cli->stop().get();
    lgr.info("service stopping");
    s.stop().get();
}

SEASTAR_THREAD_TEST_CASE(rpcgen_tls_integration) {
    std::cout.setf(std::ios::unitbuf);
    auto ssg = create_smp_service_group({5000}).get0();
    auto sc = create_scheduling_group("rpc tls scheduling group", 200).get0();
    auto creds_builder = config::tls_config(
                           true,
                           config::key_cert{"redpanda.key", "redpanda.crt"},
                           "root_certificate_authority.chain_cert",
                           false)
                           .get_credentials_builder()
                           .get0();

    rpc::server_configuration cfg;
    cfg.max_service_memory_per_core = memory::stats().total_memory() / 10;
    cfg.addrs.push_back(socket_address(ipv4_addr("127.0.0.1", 11118)));
    cfg.credentials = creds_builder;
    rpc::server s(cfg);
    s.register_service<movistar>(sc, ssg);
    lgr.info("Registered service, about to start");
    s.start();
    lgr.info("started");
    rpc::client_configuration client_cfg;
    client_cfg.server_addr = socket_address(ipv4_addr("127.0.0.1", 11118));
    client_cfg.credentials = creds_builder;
    auto cli = std::make_unique<movistar::client>(client_cfg);
    lgr.info("client connecting");
    cli->connect().get();
    lgr.info("client calling method");
    auto ret = cli->ibis_hakka(cycling::san_francisco{66}).get0();
    lgr.info("client stopping");
    cli->stop().get();
    lgr.info("service stopping");
    s.stop().get();
}
