# nix/tests/constants.nix
#
# Shared configuration for all Redpanda Nix tests.
# Centralizes ports, timeouts, config templates, and image size limits
# so changes propagate to every test automatically.
{
  ports = {
    kafka = 9092;
    admin = 9644;
    rpc = 33145;
    pandaproxy = 8082;
    schemaRegistry = 8081;
  };

  timeouts = {
    startup = 60;
    shutdown = 30;
    produce = 10;
    schemaRegistry = 15;
    lifecycle = 120;
  };

  # Generate a developer-mode redpanda.yaml for testing.
  # dataDir is a placeholder string replaced at runtime via sed.
  mkRedpandaYaml =
    {
      dataDir ? "DATA_DIR_PLACEHOLDER",
      nodeId ? 0,
      kafkaPort ? 9092,
      adminPort ? 9644,
      rpcPort ? 33145,
    }:
    ''
      redpanda:
        data_directory: ${dataDir}
        developer_mode: true
        node_id: ${toString nodeId}
        rpc_server:
          address: 127.0.0.1
          port: ${toString rpcPort}
        kafka_api:
          - address: 127.0.0.1
            port: ${toString kafkaPort}
        admin:
          - address: 127.0.0.1
            port: ${toString adminPort}
        seed_servers: []
      pandaproxy:
        pandaproxy_api:
          - address: 127.0.0.1
            port: 8082
      schema_registry:
        schema_registry_api:
          - address: 127.0.0.1
            port: 8081
    '';

  imageSizeLimits = {
    redpanda = 600;
    redpandaDebug = 700;
    rpk = 200;
  };

  testMessages = {
    small = "hello-from-nix-test";
    medium = "medium-payload-with-some-additional-data-for-testing-purposes-1234567890";
  };
}
