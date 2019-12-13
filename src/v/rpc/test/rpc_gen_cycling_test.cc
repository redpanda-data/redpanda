
#include "rpc/test/rpc_integration_fixture.h"
#include "test_utils/fixture.h"

#include <boost/test/tools/old/interface.hpp>

using namespace std::chrono_literals; // NOLINT

FIXTURE_TEST(rpcgen_integration, rpc_integration_fixture) {
    configure_server();
    register_movistar();
    start_server();
    info("started");
    auto cli = rpc::client<cycling::team_movistar_client_protocol>(
      client_config());
    info("client connecting");
    cli.connect().get();
    info("client calling method");
    auto ret = cli.ibis_hakka(cycling::san_francisco{66}).get0();
    info("client stopping");
    cli.stop().get();
    info("service stopping");
    
    BOOST_REQUIRE_EQUAL(ret.data.x, 66);
}

FIXTURE_TEST(rpcgen_tls_integration, rpc_integration_fixture) {
    auto creds_builder = config::tls_config(
                           true,
                           config::key_cert{"redpanda.key", "redpanda.crt"},
                           "root_certificate_authority.chain_cert",
                           false)
                           .get_credentials_builder()
                           .get0();
    configure_server(creds_builder);
    register_movistar();
    start_server();
    info("started");
    auto cli = rpc::client<cycling::team_movistar_client_protocol>(
      client_config(creds_builder));
    info("client connecting");
    cli.connect().get();
    info("client calling method");
    auto ret = cli.ibis_hakka(cycling::san_francisco{66}).get0();
    info("client stopping");
    cli.stop().get();
    info("service stopping");

    BOOST_REQUIRE_EQUAL(ret.data.x, 66);
}
