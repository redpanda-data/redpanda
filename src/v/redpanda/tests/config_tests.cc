#include "redpanda/redpanda_cfg.h"

#include <gtest/gtest.h>

#include <iostream>
#include <utility>

TEST(redpanda_config, load_minimal) {
    YAML::Node config = YAML::Load("---\n"
                                   "redpanda:\n"
                                   "  directory: \"~/.redpanda\"\n"
                                   "  port: 33145\n"
                                   "  ip: \"127.0.0.1\"\n"
                                   "  id: 1\n"
                                   "  seed_servers:\n"
                                   "    - addr: \"127.0.0.1:33145\"\n"
                                   "      id: 1\n");
    auto cfg = config["redpanda"].as<redpanda_cfg>();
    ASSERT_EQ(cfg.id, 1);
    ASSERT_EQ(cfg.min_version, 0);
    ASSERT_EQ(cfg.max_version, 0);
    ASSERT_EQ(cfg.seed_server_meta_topic_partitions, 7);
    ASSERT_EQ(cfg.seed_servers.size(), 1);
    ASSERT_EQ(cfg.retention_size_bytes, -1);
    ASSERT_EQ(cfg.retention_period_hrs, 168);
    ASSERT_EQ(cfg.developer_mode, false);
    ASSERT_EQ(cfg.flush_period_ms, 1000);
}

TEST(redpanda_config, load_full_definition) {
    YAML::Node config = YAML::Load("---\n"
                                   "redpanda:\n"
                                   "  directory: \"~/.redpanda\"\n"
                                   "  flush_period_ms: 6000\n"
                                   "  retention_size_bytes: 128000000\n"
                                   "  retention_period_hrs: 6\n"
                                   "  flush_period_ms: 6000\n"
                                   "  developer_mode: true\n"
                                   "  min_version: 1\n"
                                   "  max_version: 2\n"
                                   "  seed_server_meta_topic_partitions: 17\n"
                                   "  port: 33145\n"
                                   "  ip: \"127.0.0.1\"\n"
                                   "  id: 1\n"
                                   "  seed_servers:\n"
                                   "    - addr: \"127.0.0.1:33145\"\n"
                                   "      id: 1\n"
                                   "    - addr: \"127.0.0.5:33145\"\n"
                                   "      id: 5\n"
                                   "    - addr: \"127.0.0.4:33145\"\n"
                                   "      id: 4\n"
                                   "    - addr: \"127.0.0.3:33145\"\n"
                                   "      id: 3\n"
                                   "    - addr: \"127.0.0.2:33145\"\n"
                                   "      id: 2\n"
                                   "");
    auto cfg = config["redpanda"].as<redpanda_cfg>();
    ASSERT_EQ(cfg.id, 1);
    ASSERT_EQ(cfg.min_version, 1);
    ASSERT_EQ(cfg.max_version, 2);
    ASSERT_EQ(cfg.seed_server_meta_topic_partitions, 17);
    ASSERT_EQ(cfg.seed_servers.size(), 5);
    ASSERT_EQ(cfg.retention_size_bytes, 128000000);
    ASSERT_EQ(cfg.retention_period_hrs, 6);
    ASSERT_EQ(cfg.developer_mode, true);
    ASSERT_EQ(cfg.flush_period_ms, 6000);
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
