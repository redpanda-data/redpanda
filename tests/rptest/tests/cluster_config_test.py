# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import time
import requests
import json
import re
import yaml
import tempfile

from rptest.services.admin import Admin
from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.rpk import RpkTool, RpkException
from rptest.clients.rpk_remote import RpkRemoteTool
from rptest.clients.kcl import KCL
from rptest.services.cluster import cluster
from ducktape.mark import parametrize
from ducktape.utils.util import wait_until

BOOTSTRAP_CONFIG = {
    # A non-default value for checking bootstrap import works
    'enable_idempotence': True,
}


class ClusterConfigTest(RedpandaTest):
    def __init__(self, *args, **kwargs):
        rp_conf = BOOTSTRAP_CONFIG.copy()

        # Enable our feature flag
        rp_conf['enable_central_config'] = True

        super(ClusterConfigTest, self).__init__(*args,
                                                extra_rp_conf=rp_conf,
                                                **kwargs)

        self.admin = Admin(self.redpanda)
        self.rpk = RpkTool(self.redpanda)

    @cluster(num_nodes=3)
    def test_get_config(self):
        """
        Verify that the config GET endpoint serves valid json with some options in it.
        """
        admin = Admin(self.redpanda)
        config = admin.get_cluster_config()

        # Pick an arbitrary config property to verify that the result
        # contained some properties
        assert 'enable_transactions' in config

        node_config = admin.get_node_config()

        # Some arbitrary property to check syntax of result
        assert 'kafka_api' in node_config

    @cluster(num_nodes=3)
    def test_bootstrap(self):
        """
        Verify that config settings present in redpanda.cfg are imported on
        first startup.
        :return:
        """
        admin = Admin(self.redpanda)
        config = admin.get_cluster_config()
        for k, v in BOOTSTRAP_CONFIG.items():
            assert config[k] == v

        set_again = {'enable_idempotence': False}
        assert BOOTSTRAP_CONFIG['enable_idempotence'] != set_again[
            'enable_idempotence']

        self.redpanda.restart_nodes(self.redpanda.nodes, set_again)

        # Our attempt to set the value differently in the config file after first startup
        # should have failed: the original config value should still be set.
        config = admin.get_cluster_config()
        for k, v in BOOTSTRAP_CONFIG.items():
            assert config[k] == v

    def _wait_for_version_sync(self, version):
        wait_until(
            lambda: set([
                n['config_version']
                for n in self.admin.get_cluster_config_status()
            ]) == {version},
            timeout_sec=10,
            backoff_sec=0.5,
            err_msg=f"Config status versions did not converge on {version}")

    def _check_restart_clears(self):
        """
        After changing a setting with needs_restart=true, check that
        nodes clear the flag after being restarted.
        """
        status = self.admin.get_cluster_config_status()
        for n in status:
            assert n['restart'] is True

        first_node = self.redpanda.nodes[0]
        other_nodes = self.redpanda.nodes[1:]
        self.redpanda.restart_nodes(first_node)
        wait_until(lambda: self.admin.get_cluster_config_status()[0]['restart']
                   == False,
                   timeout_sec=10,
                   backoff_sec=0.5,
                   err_msg=f"Restart flag did not clear after restart")

        self.redpanda.restart_nodes(other_nodes)
        wait_until(lambda: set(
            [n['restart']
             for n in self.admin.get_cluster_config_status()]) == {False},
                   timeout_sec=10,
                   backoff_sec=0.5,
                   err_msg=f"Not all nodes cleared restart flag")

    @cluster(num_nodes=3)
    def test_restart(self):
        """
        Verify that a setting requiring restart is indicated as such in status,
        and that status is cleared after we restart the node.
        """
        # An arbitrary restart-requiring setting with a non-default value
        new_setting = ('kafka_qdc_idle_depth', 77)

        patch_result = self.admin.patch_cluster_config(
            upsert=dict([new_setting]))
        new_version = patch_result['config_version']
        self._wait_for_version_sync(new_version)

        assert self.admin.get_cluster_config()[
            new_setting[0]] == new_setting[1]
        # Update of cluster status is not synchronous
        self._check_restart_clears()

        # Test that a reset to default triggers the restart flag the same way as
        # an upsert does
        patch_result = self.admin.patch_cluster_config(remove=[new_setting[0]])
        new_version = patch_result['config_version']
        self._wait_for_version_sync(new_version)
        assert self.admin.get_cluster_config()[
            new_setting[0]] != new_setting[1]
        self._check_restart_clears()

    @cluster(num_nodes=3)
    def test_multistring_restart(self):
        """
        Reproduce an issue where the key we edit is saved correctly,
        but other cached keys are getting extra-quoted.
        """

        # Initially set both values together
        patch_result = self.admin.patch_cluster_config(
            upsert={
                "cloud_storage_access_key": "user",
                "cloud_storage_secret_key": "pass"
            })
        self._wait_for_version_sync(patch_result['config_version'])
        self._check_value_everywhere("cloud_storage_access_key", "user")
        self._check_value_everywhere("cloud_storage_secret_key", "pass")

        # Check initially set values survive a restart
        self.redpanda.restart_nodes(self.redpanda.nodes)
        self._check_value_everywhere("cloud_storage_access_key", "user")
        self._check_value_everywhere("cloud_storage_secret_key", "pass")

        # Set just one of the values
        patch_result = self.admin.patch_cluster_config(
            upsert={"cloud_storage_access_key": "user2"})
        self._wait_for_version_sync(patch_result['config_version'])
        self._check_value_everywhere("cloud_storage_access_key", "user2")
        self._check_value_everywhere("cloud_storage_secret_key", "pass")

        # Check that the recently set value persists, AND the originally
        # set value of another property is not corrupted.
        self.redpanda.restart_nodes(self.redpanda.nodes)
        self._check_value_everywhere("cloud_storage_access_key", "user2")
        self._check_value_everywhere("cloud_storage_secret_key", "pass")

    def _check_value_everywhere(self, key, expect_value):
        for node in self.redpanda.nodes:
            actual_value = self.admin.get_cluster_config(node)[key]
            if actual_value != expect_value:
                self.logger.error(
                    f"Wrong value on node {node.account.hostname}: {key}={actual_value} (!={expect_value})"
                )
            assert self.admin.get_cluster_config(node)[key] == expect_value

    def _check_propagated_and_persistent(self, key, expect_value):
        """
        Verify that a configuration value has successfully propagated to all
        nodes, and that it persists after a restart.
        """
        self._check_value_everywhere(key, expect_value)
        self.redpanda.restart_nodes(self.redpanda.nodes)
        self._check_value_everywhere(key, expect_value)

    @cluster(num_nodes=3)
    def test_simple_live_change(self):
        # An arbitrary non-restart-requiring setting
        norestart_new_setting = ('log_message_timestamp_type', "LogAppendTime")
        assert self.admin.get_cluster_config()[
            norestart_new_setting[0]] == "CreateTime"  # Initially default
        patch_result = self.admin.patch_cluster_config(
            upsert=dict([norestart_new_setting]))
        new_version = patch_result['config_version']
        self._wait_for_version_sync(new_version)

        assert self.admin.get_cluster_config()[
            norestart_new_setting[0]] == norestart_new_setting[1]

        # Status should not indicate restart needed
        status = self.admin.get_cluster_config_status()
        for n in status:
            assert n['restart'] is False

        # Setting should be propagated and survive a restart
        self._check_propagated_and_persistent(norestart_new_setting[0],
                                              norestart_new_setting[1])

    @cluster(num_nodes=3)
    @parametrize(key='log_message_timestamp_type', value="rhubarb")
    @parametrize(key='log_message_timestamp_type', value="31415")
    @parametrize(key='log_message_timestamp_type', value="false")
    @parametrize(key='kafka_qdc_enable', value="rhubarb")
    @parametrize(key='kafka_qdc_enable', value="31415")
    @parametrize(key='metadata_dissemination_retries', value="rhubarb")
    @parametrize(key='metadata_dissemination_retries', value="false")
    @parametrize(key='it_does_not_exist', value="123")
    def test_invalid_settings(self, key, value):
        """
        Test that without force=true, attempts to set invalid property
        values are rejected with a 400 status.
        """
        try:
            patch_result = self.admin.patch_cluster_config(upsert={key: value})
        except requests.exceptions.HTTPError as e:
            if e.response.status_code != 400:
                raise

            errors = e.response.json()
            assert set(errors.keys()) == {key}
        else:
            raise RuntimeError(
                f"Expected 400 but got {patch_result} for {key}={value})")

    @cluster(num_nodes=3)
    def test_invalid_settings_forced(self):
        """
        Test that if a value makes it past the frontend API validation, it is caught
        at the point of apply on each node, and fed back in the config_status.
        """
        default_value = "CreateTime"
        invalid_setting = ('log_message_timestamp_type', "rhubarb")
        assert self.admin.get_cluster_config()[
            invalid_setting[0]] == default_value
        patch_result = self.admin.patch_cluster_config(upsert=dict(
            [invalid_setting]),
                                                       force=True)
        new_version = patch_result['config_version']
        self._wait_for_version_sync(new_version)

        assert self.admin.get_cluster_config()[
            invalid_setting[0]] == default_value

        # Status should not indicate restart needed
        status = self.admin.get_cluster_config_status()
        for n in status:
            assert n['restart'] is False
            assert n['invalid'] == [invalid_setting[0]]

        # List of invalid properties in node status should not clear on restart.
        self.redpanda.restart_nodes(self.redpanda.nodes)

        # We have to sleep here because in the success case there is no status update
        # being sent: it's a no-op after node startup when they realize their config
        # status is the same as the one already reported.
        time.sleep(10)

        status = self.admin.get_cluster_config_status()
        for n in status:
            assert n['restart'] is False
            assert n['invalid'] == [invalid_setting[0]]

        # Reset the properties, check that it disappears from the list of invalid settings
        patch_result = self.admin.patch_cluster_config(
            remove=[invalid_setting[0]], force=True)
        self._wait_for_version_sync(patch_result['config_version'])
        assert self.admin.get_cluster_config()[
            invalid_setting[0]] == default_value

        status = self.admin.get_cluster_config_status()
        for n in status:
            assert n['restart'] is False
            assert n['invalid'] == []

        # TODO as well as specific invalid examples, do a pass across the whole
        # schema to check that
        pass

    @cluster(num_nodes=3)
    def test_bad_requests(self):
        """
        Verify that syntactically malformed configuration requests result
        in proper 400 responses (rather than 500s or crashes)
        """

        for content_type, body in [
            ('text/html', ""),  # Wrong type, empty
            ('text/html', "garbage"),  # Wrong type, nonempty
            ('application/json', ""),  # Empty
            ('application/json', "garbage"),  # Not JSON
            ('application/json', "{\"a\": 123}"),  # Wrong top level attributes
            ('application/json', "{\"upsert\": []}"),  # Wrong type of 'upsert'
        ]:
            try:
                self.logger.info(f"Checking {content_type}, {body}")
                self.admin._request("PUT",
                                    "cluster_config",
                                    node=self.redpanda.nodes[0],
                                    headers={'content-type': content_type},
                                    data=body)
            except requests.exceptions.HTTPError as e:
                assert e.response.status_code == 400
            else:
                # Should not succeed!
                assert False

    @cluster(num_nodes=3)
    def test_valid_settings(self):
        """
        Bulk exercise of all config settings & the schema endpoint:
        - for all properties in the schema, set them with a valid non-default value
        - check the new values are reflected in config GET
        - restart all nodes (prompt a reload from cache file)
        - check the new values are reflected in config GET

        This is not just checking the central config infrastructure: it's also
        validating that all the property types are outputting the same format
        as their input (e.g. they have proper rjson_serialize implementations)
        """
        schema_properties = self.admin.get_cluster_config_schema(
        )['properties']
        updates = {}
        properties_require_restart = False

        # Don't change these settings, they prevent the test from subsequently
        # using the cluster
        exclude_settings = {'enable_sasl', 'enable_admin_api'}

        initial_config = self.admin.get_cluster_config()

        for name, p in schema_properties.items():
            if name in exclude_settings:
                continue

            properties_require_restart |= p['needs_restart']

            initial_value = initial_config[name]
            if 'example' in p:
                valid_value = p['example']
            elif p['type'] == 'integer':
                if initial_value:
                    valid_value = initial_value * 2
                else:
                    valid_value = 100
            elif p['type'] == 'number':
                if initial_value:
                    valid_value = float(initial_value * 2)
                else:
                    valid_value = 1000.0
            elif p['type'] == 'string':
                valid_value = "rhubarb"
            elif p['type'] == 'boolean':
                valid_value = not initial_config[name]
            elif p['type'] == "array" and p['items']['type'] == 'string':
                valid_value = ["custard", "cream"]
            else:
                raise NotImplementedError(p['type'])

            if name == 'enable_coproc':
                # Don't try enabling coproc, it has external dependencies
                continue

            updates[name] = valid_value

        patch_result = self.admin.patch_cluster_config(upsert=updates,
                                                       remove=[])
        self._wait_for_version_sync(patch_result['config_version'])

        def check_status(expect_restart):
            # Use one node's status, they should be symmetric
            status = self.admin.get_cluster_config_status()[0]

            self.logger.info(f"Status: {json.dumps(status, indent=2)}")

            assert status['invalid'] == []
            assert status['restart'] is expect_restart

        def check_values():
            read_back = self.admin.get_cluster_config()
            mismatch = []
            for k, expect in updates.items():
                # String-ized comparison, because the example values are strings,
                # whereas by the time we read them back they're properly typed.
                actual = read_back.get(k, None)
                if isinstance(actual, bool):
                    # Lowercase because yaml and python capitalize bools differently.
                    actual = str(actual).lower()
                else:
                    actual = str(actual)
                if actual != str(expect):
                    self.logger.error(
                        f"Config set failed ({k}) {actual}!={expect}")
                    mismatch.append((k, actual, expect))

            assert len(mismatch) == 0

        check_status(properties_require_restart)
        check_values()
        self.redpanda.restart_nodes(self.redpanda.nodes)

        # We have to sleep here because in the success case there is no status update
        # being sent: it's a no-op after node startup when they realize their config
        # status is the same as the one already reported.
        time.sleep(10)

        # Check after restart that confuration persisted and status shows valid
        check_status(False)
        check_values()

    def _export(self, all):
        with tempfile.NamedTemporaryFile('r') as file:
            self.rpk.cluster_config_export(file.name, all)
            return file.read()

    def _import(self, text, all, allow_noop=False):
        with tempfile.NamedTemporaryFile('w') as file:
            file.write(text)
            file.flush()
            import_stdout = self.rpk.cluster_config_import(file.name, all)

        last_line = import_stdout.strip().split("\n")[-1]
        m = re.match(r"^.+new config version (\d+).*$", last_line)

        self.logger.debug(f"_import status: {last_line}")

        if m is None and allow_noop:
            return None

        assert m is not None, f"Config version not found: {last_line}"
        version = int(m.group(1))
        return version

    def _export_import_modify_one(self, before: str, after: str, all=False):
        return self._export_import_modify([(before, after)], all)

    def _export_import_modify(self, changes: list[tuple[str, str]], all=False):
        text = self._export(all)

        # Validate that RPK gives us valid yaml
        _ = yaml.load(text)

        self.logger.debug(f"Exported config before modification: {text}")

        for before, after in changes:
            self.logger.debug(f"Replacing \"{before}\" with \"{after}\"")

            # Intentionally not passing this through a YAML deserialize/serialize
            # step during edit, to more realistically emulate someone hand editing
            text = text.replace(before, after)

        self.logger.debug(f"Exported config after modification: {text}")

        # Edit a setting, import the resulting document
        version = self._import(text, all)

        return version, text

    @cluster(num_nodes=3)
    def test_rpk_export_import(self):
        """
        Test `rpk cluster config [export|import]` and implicitly
        also `edit` (which is just an export/import cycle with
        a text editor run in the middle)
        """
        # An arbitrary tunable for checking --all
        tunable_property = 'kafka_qdc_depth_alpha'

        # RPK should give us a valid yaml document
        version_a, text = self._export_import_modify_one(
            "kafka_qdc_enable: false", "kafka_qdc_enable: true")
        assert version_a is not None
        self._wait_for_version_sync(version_a)

        # Default should not have included tunables
        assert tunable_property not in text

        # The setting we edited should be updated
        self._check_value_everywhere("kafka_qdc_enable", True)

        # Clear a setting, it should revert to its default
        version_b, text = self._export_import_modify_one(
            "kafka_qdc_enable: true", "")
        assert version_b is not None

        assert version_b > version_a
        self._wait_for_version_sync(version_b)
        self._check_value_everywhere("kafka_qdc_enable", False)

        # Check that an --all export includes tunables
        text_all = self._export(all=True)
        assert tunable_property in text_all

        # Check that editing a tunable with --all works
        version_c, text = self._export_import_modify_one(
            "kafka_qdc_depth_alpha: 0.8",
            "kafka_qdc_depth_alpha: 1.5",
            all=True)
        assert version_c is not None

        assert version_c > version_b
        self._wait_for_version_sync(version_c)
        self._check_value_everywhere("kafka_qdc_depth_alpha", 1.5)

        # Check that clearing a tunable with --all works
        version_d, text = self._export_import_modify_one(
            "kafka_qdc_depth_alpha: 1.5", "", all=True)
        assert version_d is not None

        assert version_d > version_c
        self._wait_for_version_sync(version_d)
        self._check_value_everywhere("kafka_qdc_depth_alpha", 0.8)

        # Check that an import/export with no edits does nothing.
        text = self._export(all=True)
        noop_version = self._import(text, allow_noop=True, all=True)
        assert noop_version is None

    @cluster(num_nodes=3)
    def test_rpk_import_validation(self):
        """
        Verify that RPK handles 400 responses on import nicely
        """

        # RPK should return an error with explanatory text
        try:
            _, out = self._export_import_modify(
                [("kafka_qdc_enable: false", "kafka_qdc_enable: rhubarb"),
                 ("topic_fds_per_partition: 10",
                  "topic_fds_per_partition: 9999"),
                 ("default_num_windows: 10", "default_num_windows: 32768")],
                all=True)
        except RpkException as e:
            assert 'kafka_qdc_enable: expected type boolean' in e.stderr
            assert 'topic_fds_per_partition: too large' in e.stderr
            assert 'default_num_windows: out of range' in e.stderr
        else:
            raise RuntimeError(
                f"RPK command should have failed, but ran with output: {out}")

    @cluster(num_nodes=3)
    def test_rpk_edit_string(self):
        """
        Test import/export of string fields, make sure they don't end
        up with extraneous quotes
        """
        version_a, _ = self._export_import_modify_one(
            "cloud_storage_access_key:\n",
            "cloud_storage_access_key: foobar\n")
        self._wait_for_version_sync(version_a)
        self._check_value_everywhere("cloud_storage_access_key", "foobar")

        version_b, _ = self._export_import_modify_one(
            "cloud_storage_access_key: foobar\n",
            "cloud_storage_access_key: \"foobaz\"")
        self._wait_for_version_sync(version_b)
        self._check_value_everywhere("cloud_storage_access_key", "foobaz")

    @cluster(num_nodes=3)
    def test_rpk_status(self):
        """
        This command is a thin wrapper over the status API
        that is covered more comprehensively in other tests: this
        case is just a superficial test that the command succeeds and
        returns info for each node.
        """
        status_text = self.rpk.cluster_config_status()

        # Split into lines, skip first one (header)
        lines = status_text.strip().split("\n")[1:]

        # Example:

        # NODE  CONFIG_VERSION  NEEDS_RESTART  INVALID  UNKNOWN
        # 0     17              false          []       []

        assert len(lines) == len(self.redpanda.nodes)

        for i, l in enumerate(lines):
            m = re.match(
                r"^(\d+)\s+(\d+)\s+(true|false)\s+\[(.*)\]\s+\[(.*)\]$", l)
            assert m is not None
            node_id, *_ = m.groups()

            node = self.redpanda.nodes[i]
            assert int(node_id) == self.redpanda.idx(node)

    @cluster(num_nodes=3)
    def test_rpk_reset(self):
        """
        Verify that RPK's `reset` command for disaster recovery works as
        expected: redpanda should start up and behave as if the property
        is its default value.
        """
        # Set some non-default config value
        pr = self.admin.patch_cluster_config(upsert={
            'kafka_qdc_enable': True,
            'append_chunk_size': 65536
        })
        self._wait_for_version_sync(pr['config_version'])
        self._check_value_everywhere("kafka_qdc_enable", True)

        # Reset the property on all nodes
        for node in self.redpanda.nodes:
            rpk_remote = RpkRemoteTool(self.redpanda, node)
            self.redpanda.stop_node(node)
            rpk_remote.cluster_config_reset("kafka_qdc_enable")
            self.redpanda.start_node(node)

        # Check that the reset property has reverted to its default
        self._check_value_everywhere("kafka_qdc_enable", False)

        # Check that the bystander config property was not reset
        self._check_value_everywhere("append_chunk_size", 65536)

    @cluster(num_nodes=3)
    def test_secret_redaction(self):
        def search_log(pattern):
            for node in self.redpanda.nodes:
                for line in node.account.ssh_capture(
                        f"grep \"{pattern}\" {self.redpanda.STDOUT_STDERR_CAPTURE} || true"
                ):
                    # We got a match
                    self.logger.debug(
                        f"Found {pattern} on node {node.name}: {line}")
                    return True

            # Fall through, no matches
            return False

        def set_and_search(key, value, expect_log):
            patch_result = self.admin.patch_cluster_config(upsert={key: value})
            self._wait_for_version_sync(patch_result['config_version'])

            # Check value was/was not printed to log while applying
            assert search_log(value) is expect_log

            # Check we do/don't print on next startup
            self.redpanda.restart_nodes(self.redpanda.nodes)
            assert search_log(value) is expect_log

        secret_key = "cloud_storage_secret_key"
        secret_value = "ThePandaFliesTonight"
        set_and_search(secret_key, secret_value, False)

        # To avoid false negatives in the test of a secret, go through the same procedure
        # but on a non-secret property, thereby validating that our log scanning procedure
        # would have detected the secret if it had been printed
        unsecret_key = "cloud_storage_api_endpoint"
        unsecret_value = "http://nowhere"
        set_and_search(unsecret_key, unsecret_value, True)

    @cluster(num_nodes=3)
    def test_incremental_alter_configs(self):
        """
        Central config can also be accessed via Kafka API -- exercise that
        using `kcl`.

        :param incremental: whether to use incremental kafka config API or
                            legacy config API.
        """
        kcl = KCL(self.redpanda)

        # Redpanda only support incremental config changes: the legacy
        # AlterConfig API is a bad user experience
        incremental = True

        # Set a property by its redpanda name
        out = kcl.alter_broker_config(
            {"log_message_timestamp_type": "CreateTime"}, incremental)
        # kcl does not set an error exist status when config set fails, so must
        # read its output text to validate that calls are successful
        assert 'OK' in out

        out = kcl.alter_broker_config(
            {"log_message_timestamp_type": "LogAppendTime"}, incremental)
        assert 'OK' in out
        if incremental:
            kcl.delete_broker_config(["log_message_timestamp_type"],
                                     incremental)
            assert 'OK' in out

        # Set a property by its Kafka-interop names and values
        kafka_props = {
            "log.message.timestamp.type": ["CreateTime", "LogAppendTime"],
            "log.cleanup.policy": ["compact", "delete"],
            "log.compression.type": ["gzip", "snappy", "lz4", "zstd"],
        }
        for property, value_list in kafka_props.items():
            for value in value_list:
                out = kcl.alter_broker_config({property: value}, incremental)
                assert 'OK' in out

        # Set a nonexistent property
        out = kcl.alter_broker_config({"does_not_exist": "avalue"},
                                      incremental)
        assert 'INVALID_CONFIG' in out

        # Set a malformed property
        out = kcl.alter_broker_config(
            {"log_message_timestamp_type": "BadValue"}, incremental)
        assert 'INVALID_CONFIG' in out

        # Set a property on a named broker: should fail because this
        # interface is only for cluster-wide properties
        out = kcl.alter_broker_config(
            {"log_message_timestamp_type": "CreateTime"},
            incremental,
            broker="1")
        assert 'INVALID_CONFIG' in out
        assert "Setting broker properties on named brokers is unsupported" in out

    @cluster(num_nodes=3)
    def test_alter_configs(self):
        """
        We only support incremental config changes.  Check that AlterConfigs requests
        are correctly handled with an 'unsupported' response.
        """

        kcl = KCL(self.redpanda)
        out = kcl.alter_broker_config(
            {"log_message_timestamp_type": "CreateTime"}, incremental=False)
        self.logger.info("AlterConfigs output: {out}")
        assert 'INVALID_CONFIG' in out
        assert "changing broker properties isn't supported via this API" in out
