# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
import json
import pprint
import re
import tempfile
import time
from typing import Any, NamedTuple

import requests
import yaml
from ducktape.mark import parametrize, matrix
from ducktape.utils.util import wait_until

from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.clients.rpk import RpkTool, RpkException
from rptest.clients.rpk_remote import RpkRemoteTool
from rptest.clients.types import TopicSpec
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.redpanda import CloudStorageType, SISettings, RESTART_LOG_ALLOW_LIST, IAM_ROLES_API_CALL_ALLOW_LIST, get_cloud_storage_type
from rptest.services.metrics_check import MetricCheck
from rptest.tests.redpanda_test import RedpandaTest
from rptest.util import expect_http_error, expect_exception, produce_until_segments
from rptest.utils.si_utils import BucketView

BOOTSTRAP_CONFIG = {
    # A non-default value for checking bootstrap import works
    'enable_idempotence': False,
}

SECRET_CONFIG_NAMES = frozenset(
    ["cloud_storage_secret_key", "cloud_storage_azure_shared_key"])


class ClusterConfigUpgradeTest(RedpandaTest):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, extra_rp_conf={}, **kwargs)

    def setUp(self):
        # Skip starting redpanda, so that test can explicitly start
        # it with some override_cfg_params
        pass

    @cluster(num_nodes=1)
    def test_upgrade_redpanda_yaml(self):
        """
        Verify that on first start, values are imported from redpanda.yaml
        to facilitate upgrades, but on subsequent startups we just emit
        a log message to say we're ignoring them.
        """

        node = self.redpanda.nodes[0]
        admin = Admin(self.redpanda)

        # Since we skip RedpandaService.start, must clean node explicitly
        self.redpanda.clean_node(node)

        # Start node outside of the usual RedpandaService.start, so that we
        # skip writing out bootstrap.yaml files (the presence of which disables
        # the upgrade import of values from redpanda.yaml)
        self.redpanda.start_node(
            node, override_cfg_params={'delete_retention_ms': '9876'})

        # On first startup, redpanda should notice the value in
        # redpanda.yaml and import it into central config store
        assert admin.get_cluster_config()['delete_retention_ms'] == 9876

        # On second startup, central config is already initialized,
        # so the modified value in redpanda.yaml should be ignored.
        self.redpanda.restart_nodes(
            [node], override_cfg_params={'delete_retention_ms': '1234'})
        assert admin.get_cluster_config()['delete_retention_ms'] == 9876
        assert self.redpanda.search_log_any(
            "Ignoring value for 'delete_retention_ms'")


class ClusterConfigTest(RedpandaTest):
    def __init__(self, *args, **kwargs):
        rp_conf = BOOTSTRAP_CONFIG.copy()

        # Force verbose logging for the secret redaction test
        kwargs['log_level'] = 'trace'

        # An explicit cluster_id prevents metrics_report from auto-setting
        # it, and thereby prevents it doing a background write of a new
        # config version that would disrupt our tests.
        rp_conf['cluster_id'] = "placeholder"

        super(ClusterConfigTest, self).__init__(*args,
                                                extra_rp_conf=rp_conf,
                                                **kwargs)

        self.admin = Admin(self.redpanda)
        self.rpk = RpkTool(self.redpanda)

    @cluster(num_nodes=3)
    @parametrize(legacy=False)
    @parametrize(legacy=True)
    def test_get_config(self, legacy):
        """
        Verify that the config GET endpoint serves valid json with some options in it.

        :param legacy: whether to use the legacy /config endpoint
        """
        admin = Admin(self.redpanda)
        if legacy:
            config = admin._request("GET", "config").json()
        else:
            config = admin.get_cluster_config()

        # Pick an arbitrary config property to verify that the result
        # contained some properties
        assert 'enable_transactions' in config

        node_config = admin.get_node_config()

        # Some arbitrary property to check syntax of result
        assert 'kafka_api' in node_config
        self._quiesce_status()

        # Validate expected status for a cluster that we have made no changes to
        # since first start
        status = admin.get_cluster_config_status()
        for s in status:
            assert s['restart'] is False
            assert not s['invalid']
            assert not s['unknown']
        assert len(set([s['config_version'] for s in status])) == 1

    @cluster(num_nodes=1)
    def test_get_config_nodefaults(self):
        admin = Admin(self.redpanda)
        initial_short_config = admin.get_cluster_config(include_defaults=False)
        long_config = admin.get_cluster_config(include_defaults=True)

        assert len(long_config) > len(initial_short_config)

        assert 'kafka_qdc_enable' not in initial_short_config

        # After setting something to non-default is should appear
        patch_result = self.admin.patch_cluster_config(
            upsert={'kafka_qdc_enable': True})
        self._wait_for_version_sync(patch_result['config_version'])
        short_config = admin.get_cluster_config(include_defaults=False)
        assert 'kafka_qdc_enable' in short_config
        assert len(short_config) == len(initial_short_config) + 1

        # After resetting to default it should disappear
        patch_result = self.admin.patch_cluster_config(
            remove=['kafka_qdc_enable'])
        self._wait_for_version_sync(patch_result['config_version'])
        short_config = admin.get_cluster_config(include_defaults=False)
        assert 'kafka_qdc_enable' not in short_config
        assert len(short_config) == len(initial_short_config)

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

        set_again = {'enable_idempotence': True}
        assert BOOTSTRAP_CONFIG['enable_idempotence'] != set_again[
            'enable_idempotence']
        self.redpanda.set_extra_rp_conf(set_again)
        self.redpanda.write_bootstrap_cluster_config()

        self.redpanda.restart_nodes(self.redpanda.nodes, set_again)

        # Our attempt to set the value differently in the config file after first startup
        # should have failed: the original config value should still be set.
        config = admin.get_cluster_config()
        for k, v in BOOTSTRAP_CONFIG.items():
            assert config[k] == v

    def _wait_for_version_sync(self, version):
        """
        Waits for the controller to see up to date status at `version` from
        all nodes.  Does _not_ guarantee that this status result has also
        propagated to all other node: use _wait_for_version_status_sync
        if you need to query status from an arbitrary node and get consistent
        result.
        """
        wait_until(
            lambda: set([
                n['config_version'] for n in self.admin.
                get_cluster_config_status(node=self.redpanda.controller())
            ]) == {version},
            timeout_sec=10,
            backoff_sec=0.5,
            err_msg=f"Config status versions did not converge on {version}")

    def _wait_for_version_status_sync(self, version):
        """
        Stricter than _wait_for_version_sync: this requires not only that
        the config version has propagated to all nodes, but also that the
        consequent config status (of all the peers) has propagated to all nodes.
        """
        def is_complete(node):
            node_status = self.admin.get_cluster_config_status(node=node)
            return set(n['config_version'] for n in node_status) == {
                version
            } and len(node_status) == len(self.redpanda.nodes)

        for node in self.redpanda.nodes:
            wait_until(lambda: is_complete(node),
                       timeout_sec=10,
                       backoff_sec=0.5,
                       err_msg=f"Config status did not converge on {version}")

    def _quiesce_status(self):
        """
        Query the cluster version from the controller leader, then wait til all
        nodes' report that all other nodes have seen that version (i.e. config
        is up to date globally _and_ config status is up to date globally).
        """
        leader = self.redpanda.controller()

        # Read authoritative version from controller
        version = max(
            n['config_version']
            for n in self.admin.get_cluster_config_status(node=leader))

        # Wait for all nodes to report all other nodes status' up to date
        self._wait_for_version_status_sync(version)

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
        self._wait_for_version_status_sync(new_version)

        assert self.admin.get_cluster_config()[
            new_setting[0]] == new_setting[1]
        # Update of cluster status is not synchronous
        self._check_restart_clears()

        # Test that a reset to default triggers the restart flag the same way as
        # an upsert does
        patch_result = self.admin.patch_cluster_config(remove=[new_setting[0]])
        new_version = patch_result['config_version']
        self._wait_for_version_status_sync(new_version)
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
        self._check_value_everywhere("cloud_storage_secret_key", "[secret]")

        # Check initially set values survive a restart
        self.redpanda.restart_nodes(self.redpanda.nodes)
        self._check_value_everywhere("cloud_storage_access_key", "user")
        self._check_value_everywhere("cloud_storage_secret_key", "[secret]")

        # Set just one of the values
        patch_result = self.admin.patch_cluster_config(
            upsert={"cloud_storage_access_key": "user2"})
        self._wait_for_version_sync(patch_result['config_version'])
        self._check_value_everywhere("cloud_storage_access_key", "user2")
        self._check_value_everywhere("cloud_storage_secret_key", "[secret]")

        # Check that the recently set value persists, AND the originally
        # set value of another property is not corrupted.
        self.redpanda.restart_nodes(self.redpanda.nodes)
        self._check_value_everywhere("cloud_storage_access_key", "user2")
        self._check_value_everywhere("cloud_storage_secret_key", "[secret]")

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
        self._wait_for_version_status_sync(new_version)
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

    @cluster(num_nodes=1)
    def test_dry_run(self):
        """
        Verify that when the dry_run flag is used, validation is done but
        changes are not made.
        """

        # An invalid PUT
        try:
            self.admin.patch_cluster_config(
                upsert={"log_message_timestamp_type": "rhubarb"}, dry_run=True)
        except requests.exceptions.HTTPError as e:
            if e.response.status_code != 400:
                raise
            assert set(
                e.response.json().keys()) == {"log_message_timestamp_type"}
        else:
            raise RuntimeError(f"Expected 400 but got success")

        # A valid PUT
        self.admin.patch_cluster_config(
            upsert={"log_message_timestamp_type": "LogAppendTime"},
            dry_run=True)

        # Check the value didn't get set (i.e. remains default)
        self._check_value_everywhere("log_message_timestamp_type",
                                     "CreateTime")

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
        self._wait_for_version_status_sync(new_version)

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

        self._wait_for_version_status_sync(patch_result['config_version'])
        status = self.admin.get_cluster_config_status()
        for n in status:
            assert n['restart'] is False
            assert n['invalid'] == []

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
        exclude_settings = {
            'enable_sasl', 'kafka_enable_authorization',
            'kafka_mtls_principal_mapping_rules'
        }

        # Don't enable coproc: it generates log errors if its companion service isn't running
        exclude_settings.add('enable_coproc')

        initial_config = self.admin.get_cluster_config()

        for name, p in schema_properties.items():
            if name in exclude_settings:
                continue

            properties_require_restart |= p['needs_restart']

            initial_value = initial_config[name]
            if 'example' in p:
                valid_value = p['example']
                if p['type'] == "array":
                    valid_value = yaml.full_load(valid_value)
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
                if name.endswith("_url"):
                    valid_value = "http://example.com"
                else:
                    valid_value = "rhubarb"
            elif p['type'] == 'boolean':
                valid_value = not initial_config[name]
            elif p['type'] == "array" and p['items']['type'] == 'string':
                valid_value = ["custard", "cream"]
            else:
                raise NotImplementedError(p['type'])

            if name == 'sasl_mechanisms':
                # The default value is ['SCRAM'], but the array cannot contain
                # arbitrary strings because the config system validates them.
                valid_value = ['SCRAM', 'GSSAPI']

            if name == 'sasl_kerberos_principal_mapping':
                # The default value is ['DEFAULT'], but the array must contain
                # valid Kerberos mapping rules
                valid_value = [
                    'RULE:[1:$1]/L', 'RULE:[2:$1](Test.*)s/ABC///L', 'DEFAULT'
                ]

            if name == 'enable_coproc':
                # Don't try enabling coproc, it has external dependencies
                continue

            if name == 'admin_api_require_auth':
                # Don't lock ourselves out of the admin API!
                continue

            if name == 'cloud_storage_enabled':
                # Enabling cloud storage requires setting other properties too
                continue

            if name == 'storage_strict_data_init':
                # Enabling this property requires a file be manually added
                # to RP's data dir for it to start
                continue

            updates[name] = valid_value

        patch_result = self.admin.patch_cluster_config(upsert=updates,
                                                       remove=[])
        self._wait_for_version_status_sync(patch_result['config_version'])

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
                if k in SECRET_CONFIG_NAMES:
                    # Expect a redacted value for secrets. Redpanda redacts the
                    # values and we have no way of cross checking the actual
                    # values match.
                    expect = "[secret]"

                if isinstance(actual, bool):
                    # Lowercase because yaml and python capitalize bools differently.
                    actual = str(actual).lower()
                    # Not all expected bools originate from example values
                    if isinstance(expect, bool):
                        expect = str(expect).lower()
                else:
                    actual = str(actual)
                if actual != str(expect):
                    self.logger.error(
                        f"Config set failed ({k}) {actual}!={expect}")
                    mismatch.append((k, actual, expect))

            assert len(mismatch) == 0, mismatch

        check_status(properties_require_restart)
        check_values()
        self.redpanda.restart_nodes(self.redpanda.nodes)

        # We have to sleep here because in the success case there is no status update
        # being sent: it's a no-op after node startup when they realize their config
        # status is the same as the one already reported.
        time.sleep(10)

        # Check after restart that configuration persisted and status shows valid
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

        m = re.match(r"^.+New configuration version is (\d+).*$",
                     import_stdout,
                     flags=re.DOTALL)

        self.logger.debug(f"_import status: {import_stdout}")

        if m is None and allow_noop:
            return None, None

        assert m is not None, f"Config version not found: {import_stdout}"
        version = int(m.group(1))
        return version, import_stdout

    def _export_import_modify_one(self, before: str, after: str, all=False):
        return self._export_import_modify([(before, after)], all)

    def _export_import_modify(self, changes: list[tuple[str, str]], all=False):
        text = self._export(all)

        # Validate that RPK gives us valid yaml
        _ = yaml.full_load(text)

        self.logger.debug(f"Exported config before modification: {text}")

        for before, after in changes:
            self.logger.debug(f"Replacing \"{before}\" with \"{after}\"")

            # Intentionally not passing this through a YAML deserialize/serialize
            # step during edit, to more realistically emulate someone hand editing
            text = text.replace(before, after)

        self.logger.debug(f"Exported config after modification: {text}")

        # Edit a setting, import the resulting document
        version, _ = self._import(text, all)

        return version, text

    def _noop_export_import(self):
        # Intentionally enabling --all flag to test all properties
        text = self._export(True)

        # Validate that RPK gives us valid yaml
        _ = yaml.full_load(text)

        with tempfile.NamedTemporaryFile('w') as file:
            file.write(text)
            file.flush()
            return self.rpk.cluster_config_import(file.name, True)

    @cluster(num_nodes=3)
    def test_rpk_export_import(self):
        """
        Test `rpk cluster config [export|import]` and implicitly
        also `edit` (which is just an export/import cycle with
        a text editor run in the middle)
        """
        # A no-op export & import check:
        assert "No changes were made." in self._noop_export_import()

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
        noop_version, _ = self._import(text, allow_noop=True, all=True)
        assert noop_version is None

        # Now try setting a secret.
        text = text.replace("cloud_storage_secret_key:",
                            "cloud_storage_secret_key: different_secret")
        version_e, import_output = self._import(text, all)
        assert version_e is not None
        assert version_e > version_d

        # Check that rpk doesn't print the secret to stdout.
        assert "different_secret" not in import_output
        # Instead, prints a [redacted] text.
        assert "[redacted]" in import_output

        # Because rpk doesn't know the contents of the secrets, it can't
        # determine whether a secret is new. The request should be de-duped on
        # the server side, and the same config version should be returned.
        version_f, _ = self._import(text, all)
        assert version_f is not None
        assert version_f == version_e

        # Attempting to change the secret to the redacted version, on the other
        # hand, results in no version change, to prevent against accidentally
        # setting the secret to the redacted string.
        text = text.replace("cloud_storage_secret_key: different_secret",
                            "cloud_storage_secret_key: [secret]")
        noop_version, _ = self._import(text, allow_noop=True, all=True)
        assert noop_version is None

        # Removing a secret should succeed with a new version.
        text = text.replace("cloud_storage_secret_key: [secret]", "")
        version_g, _ = self._import(text, all)
        assert version_g is not None
        assert version_g > version_f

    @cluster(num_nodes=3)
    @parametrize(all=True)
    @parametrize(all=False)
    def test_rpk_import_sparse(self, all):
        """
        Verify that a user setting just their properties they're interested in
        gets a suitable terse output, stability across multiple calls, and
        that the resulting config is all-default apart from the values they set.

        This is a typical gitops-type use case, where they have defined their
        subset of configuration in a file somewhere, and periodically try
        to apply it to the cluster.
        """

        text = """
        superusers: [alice]
        """

        new_version, _ = self._import(text, all, allow_noop=True)
        self._wait_for_version_sync(new_version)

        schema_properties = self.admin.get_cluster_config_schema(
        )['properties']

        conf = self.admin.get_cluster_config(include_defaults=False)
        assert conf['superusers'] == ['alice']
        if all:
            # We should have wiped out any non-default property except the one we set,
            # and cluster_id which rpk doesn't touch.
            assert len(conf) == 2
        else:
            # Apart from the one we set, all the other properties should be tunables
            for key in conf.keys():
                if key == 'superusers' or key == 'cluster_id':
                    continue
                else:
                    property_schema = schema_properties[key]
                    is_tunable = property_schema['visibility'] == 'tunable'
                    if not is_tunable:
                        self.logger.error(
                            "Unexpected property {k} set in config")
                        self.logger.error("{k} schema: {property_schema}")

    @cluster(num_nodes=3)
    def test_rpk_import_validation(self):
        """
        Verify that RPK handles 400 responses on import nicely
        """

        # RPK should return an error with explanatory text
        try:
            _, out = self._export_import_modify(
                [("kafka_qdc_enable: false", "kafka_qdc_enable: rhubarb"),
                 ("topic_fds_per_partition: 5",
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
        self._quiesce_status()

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

    @cluster(num_nodes=3, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_rpk_force_reset(self):
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
            rpk_remote.cluster_config_force_reset("kafka_qdc_enable")
            self.redpanda.start_node(node)

        # Check that the reset property has reverted to its default
        self._check_value_everywhere("kafka_qdc_enable", False)

        # Check that the bystander config property was not reset
        self._check_value_everywhere("append_chunk_size", 65536)

    @cluster(num_nodes=1, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_rpk_lint(self):
        """
        Verify that if a redpanda config contains a cluster config
        property, then running `lint` cleans out that value, as it
        is no longer used.
        """
        node = self.redpanda.nodes[0]

        # Put an old-style property in the config
        self.logger.info("Restarting with legacy property")
        self.redpanda.restart_nodes([node],
                                    override_cfg_params={
                                        "kafka_qdc_enable": True,
                                    })
        old_conf = node.account.ssh_output(
            "cat /etc/redpanda/redpanda.yaml").decode('utf-8')
        assert 'kafka_qdc_enable' in old_conf

        # Run lint
        self.logger.info("Linting config")
        rpk_remote = RpkRemoteTool(self.redpanda, node)
        rpk_remote.cluster_config_lint()

        # Check that the old style config property was removed
        new_conf = node.account.ssh_output(
            "cat /etc/redpanda/redpanda.yaml").decode('utf-8')
        assert 'kafka_qdc_enable' not in new_conf

        # Check that the linted config file is not corrupt (redpanda loads with it)
        self.logger.info("Restarting with linted config")
        self.redpanda.stop_node(node)
        self.redpanda.start_node(node, write_config=False)

    @cluster(num_nodes=1)
    def test_rpk_get_set(self):
        """
        Test RPK's getter+setter helpers
        """
        class Example(NamedTuple):
            key: str
            strval: str
            yamlval: Any

        valid_examples = [
            Example("kafka_qdc_enable", "true", True),
            Example("append_chunk_size", "32768", 32768),
            Example("superusers", "['bob','alice']", ["bob", "alice"]),
            Example("storage_min_free_bytes", "1234567890", 1234567890)
        ]

        def yamlize(input) -> str:
            """Create a YAML representation that matches
            what yaml-cpp produces: PyYAML includes trailing
            ellipsis lines that must be removed."""
            return "\n".join([
                i for i in yaml.dump(e.yamlval).split("\n")
                if i.strip() != "..."
            ]).strip()

        # Check that valid changes are accepted, and the change is reflected
        # in the underlying API-visible configuration
        for e in valid_examples:
            self.logger.info(f"Checking {e.key}={e.strval} ({e.yamlval})")
            self.rpk.cluster_config_set(e.key, e.strval)

            # CLI readback should give same as we set
            cli_readback = self.rpk.cluster_config_get(e.key)

            expect_cli_readback = yamlize(e.yamlval)

            self.logger.info(
                f"CLI readback '{cli_readback}' expect '{expect_cli_readback}'"
            )
            assert cli_readback == expect_cli_readback

            # API readback should give properly structured+typed value
            api_readback = self.admin.get_cluster_config()[e.key]
            self.logger.info(f"API readback for {e.key} '{api_readback}'")
            assert api_readback == e.yamlval

        # Check that the `set` command hits proper validation paths
        invalid_examples = [
            ("kafka_qdc_enable", "rhubarb"),
            ("append_chunk_size", "-123"),
            ("superusers", "43"),
        ]
        for key, strval in invalid_examples:
            try:
                self.rpk.cluster_config_set(key, strval)
            except RpkException as e:
                pass
            else:
                self.logger.error(
                    f"Config setting {key}={strval} should have been rejected")
                assert False

        # Check that resetting properties to their default via `set` works
        default_examples = [
            ("kafka_qdc_enable", False),
            ("append_chunk_size", 16384),
            ("superusers", []),
        ]
        for key, expect_default in default_examples:
            self.rpk.cluster_config_set(key, "")
            api_readback = self.admin.get_cluster_config()[key]
            self.logger.info(
                f"API readback for {key} '{api_readback}' (expect {expect_default})"
            )
            assert api_readback == expect_default

    @cluster(num_nodes=3)
    def test_secret_redaction(self):
        def set_and_search(key, value, expect_log):
            patch_result = self.admin.patch_cluster_config(upsert={key: value})
            self._wait_for_version_sync(patch_result['config_version'])

            # Check value was/was not printed to log while applying
            assert self.redpanda.search_log_any(value) is expect_log

            # Check we do/don't print on next startup
            self.redpanda.restart_nodes(self.redpanda.nodes)
            assert self.redpanda.search_log_any(value) is expect_log

        # Default valued secrets are still shown.
        self._check_value_everywhere("cloud_storage_secret_key", None)

        secret_key = "cloud_storage_secret_key"
        secret_value = "ThePandaFliesTonight"
        set_and_search(secret_key, secret_value, False)

        # Once set, we shouldn't be able to access the secret values.
        self._check_value_everywhere("cloud_storage_secret_key", "[secret]")

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
        # Redpanda only support incremental config changes: the legacy
        # AlterConfig API is a bad user experience
        incremental = True

        # Set a property by its redpanda name
        out = self.client().alter_broker_config(
            {"log_message_timestamp_type": "CreateTime"}, incremental)
        # kcl does not set an error exist status when config set fails, so must
        # read its output text to validate that calls are successful
        assert 'OK' in out

        out = self.client().alter_broker_config(
            {"log_message_timestamp_type": "LogAppendTime"}, incremental)
        assert 'OK' in out
        if incremental:
            self.client().delete_broker_config(["log_message_timestamp_type"],
                                               incremental)
            assert 'OK' in out

        # Set a property by its Kafka-interop names and values
        kafka_props = {
            "log.message.timestamp.type": ["CreateTime", "LogAppendTime"],
            "log.cleanup.policy": ["compact", "delete"],
            "log.compression.type": ["gzip", "snappy", "lz4", "zstd"],
            "log.roll.ms": ["90000", "2400000"],
        }
        for property, value_list in kafka_props.items():
            for value in value_list:
                out = self.client().alter_broker_config({property: value},
                                                        incremental)
                assert 'OK' in out

        # Set a nonexistent property
        with expect_exception(RuntimeError,
                              lambda e: 'INVALID_CONFIG' in str(e)):
            self.client().alter_broker_config({"does_not_exist": "avalue"},
                                              incremental)

        # Set a malformed property
        with expect_exception(RuntimeError,
                              lambda e: 'INVALID_CONFIG' in str(e)):
            self.client().alter_broker_config(
                {"log_message_timestamp_type": "BadValue"}, incremental)

        # Set a property on a named broker: should fail because this
        # interface is only for cluster-wide properties
        with expect_exception(
                RuntimeError, lambda e: 'INVALID_CONFIG' in str(e) and
                "Setting broker properties on named brokers is unsupported" in
                str(e)):
            self.client().alter_broker_config(
                {"log_message_timestamp_type": "CreateTime"},
                incremental,
                broker=1)

    @cluster(num_nodes=3)
    def test_alter_configs(self):
        """
        We only support incremental config changes.  Check that AlterConfigs requests
        are correctly handled with an 'unsupported' response.
        """

        with expect_exception(
                RuntimeError, lambda e: 'INVALID_CONFIG' in str(e) and
                "changing broker properties isn't supported via this API" in
                str(e)):
            self.client().alter_broker_config(
                {"log_message_timestamp_type": "CreateTime"},
                incremental=False)

    @cluster(num_nodes=3, log_allow_list=IAM_ROLES_API_CALL_ALLOW_LIST)
    def test_cloud_validation(self):
        """
        Cloud storage configuration has special multi-property rules, check
        they are enforced.
        """

        # It is invalid to enable cloud storage without its accompanying properties
        invalid_update = {'cloud_storage_enabled': True}
        with expect_http_error(400):
            self.admin.patch_cluster_config(upsert=invalid_update, remove=[])

        # Required for STS to function correctly, the token file is just a placeholder
        # to make the refresh credentials system boot up.
        self.redpanda.set_environment({
            'AWS_ROLE_ARN':
            'role',
            'AWS_WEB_IDENTITY_TOKEN_FILE':
            '/etc/hosts'
        })

        # Exercise a set of valid combinations of access+secret keys and credentials sources
        valid_updates = [
            {
                'cloud_storage_enabled': True,
                'cloud_storage_credentials_source': 'aws_instance_metadata',
                'cloud_storage_region': 'us-east-1',
                'cloud_storage_bucket': 'dearliza'
            },
            {
                'cloud_storage_enabled': True,
                'cloud_storage_credentials_source': 'gcp_instance_metadata',
                'cloud_storage_region': 'us-east-1',
                'cloud_storage_bucket': 'dearliza'
            },
            {
                'cloud_storage_enabled': True,
                'cloud_storage_credentials_source': 'sts',
                'cloud_storage_region': 'us-east-1',
                'cloud_storage_bucket': 'dearliza'
            },
            {
                'cloud_storage_enabled': True,
                'cloud_storage_secret_key': 'open',
                'cloud_storage_access_key': 'sesame',
                'cloud_storage_credentials_source': 'config_file',
                'cloud_storage_region': 'us-east-1',
                'cloud_storage_bucket': 'dearliza'
            },
        ]
        for payload in valid_updates:
            # It is valid to remove keys from config when the credentials source is dynamic
            removed = []
            if 'cloud_storage_access_key' not in payload:
                removed.append('cloud_storage_access_key')
            if 'cloud_storage_secret_key' not in payload:
                removed.append('cloud_storage_secret_key')
            self.logger.debug(
                f'patching with {pprint.pformat(payload, indent=1)}, removed keys: {removed}'
            )
            patch_result = self.admin.patch_cluster_config(upsert=payload,
                                                           remove=removed)
            self._wait_for_version_sync(patch_result['config_version'])

            # Check we really set it properly, and Redpanda can restart without
            # hitting a validation issue on startup (this is what would happen
            # if the API validation wasn't working properly)
            self.redpanda.restart_nodes(self.redpanda.nodes)

        # Set the config to static for the next set of checks
        static_config = {
            'cloud_storage_enabled': True,
            'cloud_storage_secret_key': 'open',
            'cloud_storage_access_key': 'sesame',
            'cloud_storage_region': 'us-east-1',
            'cloud_storage_bucket': 'dearliza'
        }
        patch_result = self.admin.patch_cluster_config(upsert=static_config,
                                                       remove=[])
        self._wait_for_version_sync(patch_result['config_version'])
        self.redpanda.restart_nodes(self.redpanda.nodes)

        # It is invalid to clear any required cloud storage properties while
        # cloud storage is enabled and the credentials source is static.
        forbidden_to_clear = [
            'cloud_storage_secret_key', 'cloud_storage_access_key',
            'cloud_storage_region', 'cloud_storage_bucket'
        ]
        for key in forbidden_to_clear:
            with expect_http_error(400):
                self.admin.patch_cluster_config(upsert={}, remove=[key])

        # Switching off cloud storage is always valid, we can leave the other
        # properties set
        patch_result = self.admin.patch_cluster_config(
            upsert={'cloud_storage_enabled': False}, remove=[])
        self._wait_for_version_sync(patch_result['config_version'])

        # Clearing related properties is valid now that cloud storage is
        # disabled
        for key in forbidden_to_clear:
            self.admin.patch_cluster_config(upsert={}, remove=[key])

    @cluster(num_nodes=3)
    def test_status_read_after_write_consistency(self):
        """
        In general, status is updated asynchronously, and API clients
        may send a PUT to any node, and poll any node to see asynchronous
        updates to status.

        However, there is a special path for API clients that would like to
        get something a bit stricter: if they send a PUT to the controller
        leader and then read the status back from the leader, they will
        always see the status for this node updated with the new version
        in a subsequent GET cluster_config/status to the same node.

        Clearly doing fast reads isn't a guarantee of strict consistency
        rules, but it will detect violations on realistic timescales.  This
        test did fail in practice before the change to have /status return
        projected values.
        """

        admin = Admin(self.redpanda)

        # Don't want controller leadership changing while we run
        r = admin.patch_cluster_config(
            upsert={'enable_leader_balancer': False})
        config_version = r['config_version']
        self._wait_for_version_sync(config_version)

        controller_node = self.redpanda.controller()
        for i in range(0, 50):
            # Some config update, different each iteration
            r = admin.patch_cluster_config(
                upsert={'kafka_connections_max': 1000 + i},
                node=controller_node)
            new_config_version = r['config_version']
            assert new_config_version != config_version
            config_version = new_config_version

            # Immediately read back status from controller, it should reflect new version
            status = self.admin.get_cluster_config_status(node=controller_node)
            local_status = next(
                s for s in status
                if s['node_id'] == self.redpanda.idx(controller_node))
            assert local_status['config_version'] == config_version


class ClusterConfigClusterIdTest(RedpandaTest):
    @cluster(num_nodes=3)
    def test_cluster_id(self):
        """
        That the cluster_id exposed in Kafka metadata is automatically
        populated with a uuid, that it starts with redpanda. and that
        it can be overridden by setting the property to something else.
        """

        rpk = RpkTool(self.redpanda)

        # An example, we will compare lengths with this
        uuid_example = "redpanda.87e8c0c3-7c2a-4f7b-987f-11fc1d2443a4"

        def has_uuid_cluster_id():
            cluster_id = rpk.cluster_metadata_id()
            self.logger.info(f"cluster_id={cluster_id}")
            return cluster_id is not None and len(cluster_id) == len(
                uuid_example)

        # This is a wait_until because the initialization of cluster_id
        # is async and can happen after the cluster starts answering Kafka requests.
        wait_until(has_uuid_cluster_id, timeout_sec=20, backoff_sec=1)

        # Verify that the cluster_id does not change on a restart
        initial_cluster_id = rpk.cluster_metadata_id()
        self.redpanda.restart_nodes(self.redpanda.nodes)
        assert rpk.cluster_metadata_id() == initial_cluster_id

        # Verify that a manually set cluster_id is respected
        manual_id = "rhubarb"
        self.redpanda.set_cluster_config(values={"cluster_id": manual_id},
                                         expect_restart=False)

        assert rpk.cluster_metadata_id() == f"redpanda.{manual_id}"


class ClusterConfigNoKafkaTest(RedpandaTest):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, extra_node_conf={'kafka_api': []}, **kwargs)

    @cluster(num_nodes=3)
    def test_no_kafka(self):
        """
        That a cluster may be started with no Kafka listeners at all, perhaps
        to do some initial configuration before exposing its API to clients.
        """

        admin = Admin(self.redpanda)

        rpk = RpkTool(self.redpanda)
        try:
            rpk.create_topic("testtopic")
        except RpkException:
            pass
        else:
            raise RuntimeError("Kafka API shouldn't be available")

        user = "alice"
        password = "sekrit"
        admin.create_user(user, password, algorithm="SCRAM-SHA-256")

        # It takes a moment for the user creation to propagate, we
        # need it to have reached whichever node we next user to set
        # configuration
        time.sleep(5)

        alice_admin = Admin(self.redpanda, auth=(user, password))
        self.redpanda.set_cluster_config(
            {
                'superusers': [user],
                'enable_sasl': True,
                'admin_api_require_auth': True
            },
            expect_restart=False,
            admin_client=alice_admin)

        for n in self.redpanda.nodes:
            # Removing config override, on restart it will get kafka_api populated
            self.redpanda.set_extra_node_conf(n, {})

        self.redpanda.restart_nodes(self.redpanda.nodes)

        # Auth should be switched on: anonymous Kafka client should not work
        try:
            rpk.create_topic("testtopic")
        except RpkException:
            pass
        else:
            raise RuntimeError("Kafka auth should fail")

        # When I use the username+password I created, it should work.
        rpk = RpkTool(self.redpanda,
                      username=user,
                      password=password,
                      sasl_mechanism="SCRAM-SHA-256")
        rpk.create_topic("testtopic")


class ClusterConfigAzureSharedKey(RedpandaTest):
    segment_size = 1024 * 1024
    topics = (TopicSpec(
        partition_count=1,
        replication_factor=3,
    ), )

    def __init__(self, test_context):
        self.si_settings = SISettings(test_context,
                                      log_segment_size=self.segment_size,
                                      fast_uploads=True)
        super().__init__(test_context,
                         log_level="trace",
                         si_settings=self.si_settings,
                         extra_rp_conf={})

        self.kafka_cli = KafkaCliTools(self.redpanda)

    def get_cloud_log_size(self):
        s3_snapshot = BucketView(self.redpanda, topics=self.topics)
        return s3_snapshot.cloud_log_size_for_ntp(self.topic, 0)

    def wait_for_cloud_uploads(self, initial_count: int, delta: int):
        def segment_uploaded():
            return self.get_cloud_segment_count() >= initial_count + delta

        wait_until(lambda: segment_uploaded(initial_count, delta),
                   timeout_sec=30,
                   backoff_sec=5,
                   err_msg="Segments were not uploaded")

    def produce_records(self, records: int, record_size: int):
        self.kafka_cli.produce(self.topic, records, record_size)

    @cluster(num_nodes=3,
             log_allow_list=[
                 r"abs - .* Received .* AuthorizationFailure error response",
                 r"abs - .* Received .* AuthenticationFailed error response"
             ])
    @matrix(cloud_storage_type=get_cloud_storage_type(
        applies_only_on=[CloudStorageType.ABS]))
    def test_live_shared_key_change(self, cloud_storage_type):
        """
        This test ensures that 'cloud_storage_azure_shared_key' can
        be safely updated without a full restart of the cluster.

        The test performs the following steps:
        1. Begin with a key in-place
        2. Validate uploads work
        3. Replace the key with a bogus one
        4. Validate uploads are failing 
        5. Set the key back to the initial value
        6. Validate uploads work again
        7. Try to unset the key
        8. Validate that this is not allowed
        """

        initial_cloud_log_size = self.get_cloud_log_size()
        self.produce_records(10, 1024)
        wait_until(lambda: self.get_cloud_log_size() >= initial_cloud_log_size
                   + 10 * 1024,
                   timeout_sec=30,
                   backoff_sec=5,
                   err_msg="Data was not uploaded to cloud")

        topic_leader_node = self.redpanda.partitions(self.topic)[0].leader
        metric_check = MetricCheck(
            self.logger,
            self.redpanda,
            topic_leader_node, [
                "vectorized_cloud_storage_successful_uploads_total",
                "vectorized_cloud_storage_failed_uploads_total"
            ],
            reduce=sum)

        def check_uploads_failing():
            return metric_check.evaluate([
                ("vectorized_cloud_storage_successful_uploads_total",
                 lambda a, b: b == a),
                ("vectorized_cloud_storage_failed_uploads_total",
                 lambda a, b: b > a)
            ])

        self.redpanda.set_cluster_config(
            {
                "cloud_storage_azure_shared_key":
                "notakey02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
            },
            expect_restart=False)

        self.produce_records(10, 1024)
        wait_until(check_uploads_failing,
                   timeout_sec=30,
                   backoff_sec=5,
                   err_msg="Uploads did not fail")

        self.redpanda.set_cluster_config(
            {
                "cloud_storage_azure_shared_key":
                self.si_settings.cloud_storage_azure_shared_key
            },
            expect_restart=False)

        initial_cloud_log_size = self.get_cloud_log_size()
        self.produce_records(10, 1024)
        wait_until(lambda: self.get_cloud_log_size() >= initial_cloud_log_size
                   + 10 * 1024,
                   timeout_sec=30,
                   backoff_sec=5,
                   err_msg="Data was not uploaded to cloud")

        with expect_http_error(400):
            self.redpanda.set_cluster_config(
                {"cloud_storage_azure_shared_key": None}, expect_restart=False)
