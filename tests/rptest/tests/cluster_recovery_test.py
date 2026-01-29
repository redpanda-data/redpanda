# Copyright 2023 Redpanda Data, Inc.
#
# Licensed as a Redpanda Enterprise file under the Redpanda Community
# License (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
# https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

import random
import string
import time
import pytest
from typing import Any

import requests

import ducktape
import ducktape.errors
from ducktape.tests.test import TestContext
from ducktape.utils.util import wait_until

from rptest.services.admin import RoleMember
from rptest.clients.admin.proto.redpanda.core.admin.v2 import security_pb2
from rptest.clients.admin.v2 import Admin as AdminV2
from rptest.clients.rpk import RPKACLInput, RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import KgoVerifierProducer
from rptest.services.redpanda import SISettings
from rptest.tests.redpanda_test import RedpandaTest
from rptest.util import wait_until_result
from rptest.utils.si_utils import quiesce_uploads

KiB = 1024
MiB = KiB * KiB


def random_string(length):
    return "".join([random.choice(string.ascii_lowercase) for i in range(0, length)])


class ClusterRecoveryTest(RedpandaTest):
    segment_size = 1 * MiB
    message_size = 16 * KiB
    topics = [
        TopicSpec(
            name=f"topic-{n}",
            partition_count=3,
            replication_factor=1,
            redpanda_remote_write=True,
            redpanda_remote_read=True,
            retention_bytes=-1,
            retention_ms=-1,
            segment_bytes=1 * MiB,
        )
        for n in range(3)
    ]

    def __init__(self, test_context: TestContext):
        si_settings = SISettings(
            test_context, log_segment_size=self.segment_size, fast_uploads=True
        )
        extra_rp_conf = {
            # Test takes too much time otherwise.
            "group_topic_partitions": 2,
            "controller_snapshot_max_age_sec": 1,
            "cloud_storage_cluster_metadata_upload_interval_ms": 1000,
            "enable_cluster_metadata_upload_loop": True,
        }
        si_settings = SISettings(
            test_context,
            log_segment_size=self.segment_size,
            fast_uploads=True,
        )
        self.s3_bucket = si_settings.cloud_storage_bucket
        super(ClusterRecoveryTest, self).__init__(
            test_context=test_context,
            si_settings=si_settings,
            extra_rp_conf=extra_rp_conf,
        )

    @cluster(num_nodes=4)
    def test_basic_controller_snapshot_restore(self):
        """
        Tests that recovery of some fixed pieces of controller metadata get
        restored by cluster recovery.
        """
        rpk = RpkTool(self.redpanda)
        rpk.cluster_config_set("log_segment_size_max", 1000000)

        for t in self.topics:
            KgoVerifierProducer.oneshot(
                self.test_context,
                self.redpanda,
                t.name,
                self.message_size,
                100,
                batch_max_bytes=self.message_size * 8,
                timeout_sec=60,
            )
        quiesce_uploads(self.redpanda, [t.name for t in self.topics], timeout_sec=60)

        algorithm = "SCRAM-SHA-256"
        users = dict()
        users["admin"] = None  # Created by the RedpandaService.
        roles: dict[str, set[RoleMember]] = dict()
        for _ in range(3):
            user = f"user-{random_string(6)}"
            password = f"user-{random_string(9)}"
            users[user] = password
            self.redpanda._admin.create_user(user, password, algorithm)
            rpk.acl_create_allow_cluster(user, op="describe")

            role_name: str = f"role-{random_string(6)}"
            role_members = [RoleMember.User(user)]
            roles[role_name] = set(role_members)
            self.redpanda._admin.create_role(role_name)
            self.redpanda._admin.update_role_members(role_name, add=role_members)

            role_acl = RPKACLInput()
            role_acl.allow_role = [role_name]
            role_acl.cluster = True
            role_acl.operation = ["ALL"]
            rpk.acl_create(role_acl)
        rpk.acl_create_allow_cluster("admin", op="describe")

        time.sleep(5)

        self.redpanda.stop()
        for n in self.redpanda.nodes:
            self.redpanda.remove_local_data(n)
        self.redpanda.restart_nodes(
            self.redpanda.nodes, auto_assign_node_id=True, omit_seeds_on_idx_one=False
        )
        self.redpanda._admin.await_stable_leader(
            "controller", partition=0, namespace="redpanda", timeout_s=60, backoff_s=2
        )

        self.logger.info("Verifying that no data is present before recovery")

        assert len(rpk.list_topics()) == 0, "Expected no topics before recovery"
        assert len(self.redpanda._admin.list_users()) == 0, (
            "Expected no users before recovery"
        )
        # Expecting 1 line for the header only.
        assert len(rpk.acl_list().splitlines()) == 1, "Expected no ACLs before recovery"
        assert len(rpk.list_roles().get("roles", [])) == 0, (
            "Expected no roles before recovery"
        )

        self.logger.info("Initializing cluster recovery")

        self.redpanda._admin.initialize_cluster_recovery()

        def cluster_recovery_complete():
            return (
                "inactive"
                in self.redpanda._admin.get_cluster_recovery_status().json()["state"]
            )

        wait_until(cluster_recovery_complete, timeout_sec=30, backoff_sec=1)

        assert len(set(rpk.list_topics())) == 3, "Incorrect number of topics restored"
        segment_size_max_restored = rpk.cluster_config_get("log_segment_size_max")
        assert "1000000" == segment_size_max_restored, (
            f"1000000 vs {segment_size_max_restored}"
        )
        restored_users = self.redpanda._admin.list_users()
        assert set(restored_users) == set(users.keys()), (
            f"{restored_users} vs {users.keys()}"
        )

        acls = rpk.acl_list()
        acls_lines = acls.splitlines()
        for u in users:
            found = False
            for l in acls_lines:
                if u in l and "ALLOW" in l and "DESCRIBE" in l:
                    found = True
            assert found, f"Couldn't find {u} in {acls_lines}"

        self.logger.info("Verifying roles")

        restored_roles = rpk.list_roles().get("roles", [])
        assert set(restored_roles) == set(roles.keys()), (
            f"{restored_roles} vs {roles.keys()}"
        )

        for role_name, members in roles.items():
            res = rpk.describe_role(role_name)
            restored_role_members = set(
                RoleMember.User(member["name"])
                for member in res.get("members", [])
                if member["principal_type"] == RoleMember.PrincipalType.USER.value
            )

            assert restored_role_members == members, (
                f"Role {role_name} members mismatch: {restored_role_members} vs {members}"
            )

            found_role_acl = False
            for l in acls_lines:
                if role_name in l and "ALLOW" in l and "ALL" in l:
                    found_role_acl = True
            assert found_role_acl, f"Couldn't find {role_name} in {acls_lines}"

    @cluster(num_nodes=4)
    def test_group_acl_restore(self):
        """
        Tests that Group ACLs (ACLs with Group: principal prefix) are properly
        backed up and restored by cluster recovery.
        """
        rpk = RpkTool(self.redpanda)

        for t in self.topics:
            KgoVerifierProducer.oneshot(
                self.test_context,
                self.redpanda,
                t.name,
                self.message_size,
                100,
                batch_max_bytes=self.message_size * 8,
                timeout_sec=60,
            )
        quiesce_uploads(self.redpanda, [t.name for t in self.topics], timeout_sec=60)

        # Create Group ACLs with different operations and resource types
        group_acls = []
        for i in range(3):
            group_name = f"test_group_{random_string(6)}"

            # Create a Group ACL on the cluster resource
            cluster_acl = RPKACLInput()
            cluster_acl.allow_principal = [f"Group:{group_name}"]
            cluster_acl.allow_host = ["*"]
            cluster_acl.operation = ["describe"]
            cluster_acl.cluster = True
            rpk.acl_create(cluster_acl)

            # Create a Group ACL on a topic resource
            topic_acl = RPKACLInput()
            topic_acl.allow_principal = [f"Group:{group_name}"]
            topic_acl.allow_host = ["*"]
            topic_acl.operation = ["read", "describe"]
            topic_acl.topic = [self.topics[i].name]
            rpk.acl_create(topic_acl)

            group_acls.append(group_name)

        self.logger.info(f"Created Group ACLs for groups: {group_acls}")

        # Verify ACLs were created
        acls_before = rpk.acl_list(format="json")
        for group_name in group_acls:
            group_acl_found = any(
                acl.get("principal") == f"Group:{group_name}"
                for acl in acls_before.get("matches", [])
            )
            assert group_acl_found, f"Group:{group_name} ACL not found before recovery"

        time.sleep(5)

        # Stop and wipe the cluster
        self.redpanda.stop()
        for n in self.redpanda.nodes:
            self.redpanda.remove_local_data(n)
        self.redpanda.restart_nodes(
            self.redpanda.nodes, auto_assign_node_id=True, omit_seeds_on_idx_one=False
        )
        self.redpanda._admin.await_stable_leader(
            "controller", partition=0, namespace="redpanda", timeout_s=60, backoff_s=2
        )

        self.logger.info("Verifying that no ACLs are present before recovery")
        assert len(rpk.acl_list().splitlines()) == 1, "Expected no ACLs before recovery"

        self.logger.info("Initializing cluster recovery")
        self.redpanda._admin.initialize_cluster_recovery()

        def cluster_recovery_complete():
            return (
                "inactive"
                in self.redpanda._admin.get_cluster_recovery_status().json()["state"]
            )

        wait_until(cluster_recovery_complete, timeout_sec=30, backoff_sec=1)

        self.logger.info("Verifying Group ACLs are restored")

        acls_after = rpk.acl_list(format="json")
        for group_name in group_acls:
            # Check for cluster ACL
            cluster_acl_found = any(
                acl.get("principal") == f"Group:{group_name}"
                and acl.get("resource_type") == "CLUSTER"
                for acl in acls_after.get("matches", [])
            )
            assert cluster_acl_found, f"Group:{group_name} cluster ACL not restored"

            # Check for topic ACL
            topic_acl_found = any(
                acl.get("principal") == f"Group:{group_name}"
                and acl.get("resource_type") == "TOPIC"
                for acl in acls_after.get("matches", [])
            )
            assert topic_acl_found, f"Group:{group_name} topic ACL not restored"

        self.logger.info("All Group ACLs successfully restored")

    @cluster(num_nodes=4)
    def test_mixed_principal_acl_restore(self):
        """
        Tests that a mix of User, Role, and Group ACLs are all properly
        backed up and restored by cluster recovery.
        """
        rpk = RpkTool(self.redpanda)

        for t in self.topics:
            KgoVerifierProducer.oneshot(
                self.test_context,
                self.redpanda,
                t.name,
                self.message_size,
                100,
                batch_max_bytes=self.message_size * 8,
                timeout_sec=60,
            )
        quiesce_uploads(self.redpanda, [t.name for t in self.topics], timeout_sec=60)

        algorithm = "SCRAM-SHA-256"

        # Create a user with ACL
        user_name = f"user-{random_string(6)}"
        user_password = f"pass-{random_string(9)}"
        self.redpanda._admin.create_user(user_name, user_password, algorithm)
        rpk.acl_create_allow_cluster(user_name, op="describe")

        # Create a role with ACL
        role_name = f"role-{random_string(6)}"
        self.redpanda._admin.create_role(role_name)
        self.redpanda._admin.update_role_members(
            role_name, add=[RoleMember.User(user_name)]
        )
        role_acl = RPKACLInput()
        role_acl.allow_role = [role_name]
        role_acl.cluster = True
        role_acl.operation = ["ALL"]
        rpk.acl_create(role_acl)

        # Create a group ACL
        group_name = f"group-{random_string(6)}"
        group_acl = RPKACLInput()
        group_acl.allow_principal = [f"Group:{group_name}"]
        group_acl.allow_host = ["*"]
        group_acl.operation = ["describe", "read"]
        group_acl.cluster = True
        rpk.acl_create(group_acl)

        self.logger.info(
            f"Created mixed ACLs - User: {user_name}, Role: {role_name}, Group: {group_name}"
        )

        time.sleep(5)

        # Stop and wipe the cluster
        self.redpanda.stop()
        for n in self.redpanda.nodes:
            self.redpanda.remove_local_data(n)
        self.redpanda.restart_nodes(
            self.redpanda.nodes, auto_assign_node_id=True, omit_seeds_on_idx_one=False
        )
        self.redpanda._admin.await_stable_leader(
            "controller", partition=0, namespace="redpanda", timeout_s=60, backoff_s=2
        )

        self.logger.info("Initializing cluster recovery")
        self.redpanda._admin.initialize_cluster_recovery()

        def cluster_recovery_complete():
            return (
                "inactive"
                in self.redpanda._admin.get_cluster_recovery_status().json()["state"]
            )

        wait_until(cluster_recovery_complete, timeout_sec=30, backoff_sec=1)

        self.logger.info("Verifying all ACL types are restored")

        acls = rpk.acl_list()
        acls_lines = acls.splitlines()

        # Check user ACL
        user_acl_found = any(
            user_name in line and "ALLOW" in line and "DESCRIBE" in line
            for line in acls_lines
        )
        assert user_acl_found, f"User ACL for {user_name} not restored"

        # Check role ACL
        role_acl_found = any(
            role_name in line and "ALLOW" in line and "ALL" in line
            for line in acls_lines
        )
        assert role_acl_found, f"Role ACL for {role_name} not restored"

        # Check group ACL
        group_acl_found = any(
            f"Group:{group_name}" in line and "ALLOW" in line for line in acls_lines
        )
        assert group_acl_found, f"Group ACL for {group_name} not restored"

        # Verify role membership is also restored
        restored_roles = rpk.list_roles().get("roles", [])
        assert role_name in restored_roles, f"Role {role_name} not restored"

        role_details = rpk.describe_role(role_name)
        restored_members = [
            m["name"]
            for m in role_details.get("members", [])
            if m["principal_type"] == RoleMember.PrincipalType.USER.value
        ]
        assert user_name in restored_members, (
            f"User {user_name} not in role {role_name} members after restore"
        )

        self.logger.info("All mixed principal ACLs successfully restored")

    @cluster(num_nodes=4)
    def test_role_with_group_members_restore(self):
        """
        Tests that roles with Group members (not just User members) are properly
        backed up and restored by cluster recovery.
        """
        for t in self.topics:
            KgoVerifierProducer.oneshot(
                self.test_context,
                self.redpanda,
                t.name,
                self.message_size,
                100,
                batch_max_bytes=self.message_size * 8,
                timeout_sec=60,
            )
        quiesce_uploads(self.redpanda, [t.name for t in self.topics], timeout_sec=60)

        # Use Admin v2 API to create roles with Group members
        superuser = self.redpanda.SUPERUSER_CREDENTIALS
        admin_v2 = AdminV2(self.redpanda, auth=(superuser.username, superuser.password))

        # Create a role with only Group members
        group_only_role = f"group-only-role-{random_string(6)}"
        group_name_1 = f"admin-group-{random_string(4)}"
        group_name_2 = f"dev-group-{random_string(4)}"
        group_members = [
            security_pb2.RoleMember(group=security_pb2.RoleGroup(name=group_name_1)),
            security_pb2.RoleMember(group=security_pb2.RoleGroup(name=group_name_2)),
        ]
        admin_v2.security().create_role(
            security_pb2.CreateRoleRequest(
                role=security_pb2.Role(name=group_only_role, members=group_members)
            )
        )

        # Create a role with mixed User and Group members
        mixed_role = f"mixed-role-{random_string(6)}"
        user_name = f"user-{random_string(6)}"
        user_password = f"pass-{random_string(9)}"
        self.redpanda._admin.create_user(user_name, user_password, "SCRAM-SHA-256")

        ops_group_name = f"ops-group-{random_string(4)}"
        mixed_members = [
            security_pb2.RoleMember(user=security_pb2.RoleUser(name=user_name)),
            security_pb2.RoleMember(group=security_pb2.RoleGroup(name=ops_group_name)),
        ]
        admin_v2.security().create_role(
            security_pb2.CreateRoleRequest(
                role=security_pb2.Role(name=mixed_role, members=mixed_members)
            )
        )

        self.logger.info(
            f"Created roles - group_only_role: {group_only_role} with groups [{group_name_1}, {group_name_2}], "
            f"mixed_role: {mixed_role} with user [{user_name}] and group [{ops_group_name}]"
        )

        # Helper to extract members from protobuf Role
        def get_group_members(role: security_pb2.Role) -> list[str]:
            return [
                m.group.name for m in role.members if m.WhichOneof("member") == "group"
            ]

        def get_user_members(role: security_pb2.Role) -> list[str]:
            return [
                m.user.name for m in role.members if m.WhichOneof("member") == "user"
            ]

        # Verify roles were created correctly before recovery using AdminV2
        group_only_role_obj = (
            admin_v2.security()
            .get_role(security_pb2.GetRoleRequest(name=group_only_role))
            .role
        )
        assert len(group_only_role_obj.members) == 2, (
            f"Expected 2 members in {group_only_role}, got {group_only_role_obj.members}"
        )

        def get_role_if_it_exists(
            role_name: str,
        ) -> tuple[bool, security_pb2.Role | None]:
            try:
                role = (
                    admin_v2.security()
                    .get_role(security_pb2.GetRoleRequest(name=role_name))
                    .role
                )
                return True, role
            except Exception:
                return False, None

        mixed_role_obj = wait_until_result(
            lambda: get_role_if_it_exists(mixed_role),
            timeout_sec=30,
            backoff_sec=1,
            err_msg=f"Unable to find role {mixed_role}",
        )

        assert len(mixed_role_obj.members) == 2, (
            f"Expected 2 members in {mixed_role}, got {mixed_role_obj.members}"
        )

        time.sleep(5)

        # Stop and wipe the cluster
        self.redpanda.stop()
        for n in self.redpanda.nodes:
            self.redpanda.remove_local_data(n)
        self.redpanda.restart_nodes(
            self.redpanda.nodes, auto_assign_node_id=True, omit_seeds_on_idx_one=False
        )
        self.redpanda._admin.await_stable_leader(
            "controller", partition=0, namespace="redpanda", timeout_s=60, backoff_s=2
        )

        # Recreate AdminV2 client after cluster restart
        admin_v2 = AdminV2(self.redpanda, auth=(superuser.username, superuser.password))

        self.logger.info("Verifying that no roles are present before recovery")
        roles_before_recovery = (
            admin_v2.security().list_roles(security_pb2.ListRolesRequest()).roles
        )
        assert len(roles_before_recovery) == 0, "Expected no roles before recovery"

        self.logger.info("Initializing cluster recovery")
        self.redpanda._admin.initialize_cluster_recovery()

        def cluster_recovery_complete():
            return (
                "inactive"
                in self.redpanda._admin.get_cluster_recovery_status().json()["state"]
            )

        wait_until(cluster_recovery_complete, timeout_sec=30, backoff_sec=1)

        self.logger.info("Verifying roles with Group members are restored")

        # Verify group-only role using AdminV2
        restored_roles = (
            admin_v2.security().list_roles(security_pb2.ListRolesRequest()).roles
        )
        restored_role_names = [r.name for r in restored_roles]
        assert group_only_role in restored_role_names, (
            f"Role {group_only_role} not restored"
        )

        group_only_restored = (
            admin_v2.security()
            .get_role(security_pb2.GetRoleRequest(name=group_only_role))
            .role
        )
        restored_group_members = get_group_members(group_only_restored)
        assert len(restored_group_members) == 2, (
            f"Expected 2 Group members in {group_only_role}, got {restored_group_members}"
        )
        for group_name in [group_name_1, group_name_2]:
            assert group_name in restored_group_members, (
                f"Group member {group_name} not restored in {group_only_role}"
            )

        # Verify mixed role using AdminV2
        assert mixed_role in restored_role_names, f"Role {mixed_role} not restored"

        mixed_restored = (
            admin_v2.security()
            .get_role(security_pb2.GetRoleRequest(name=mixed_role))
            .role
        )
        restored_user_members = get_user_members(mixed_restored)
        restored_mixed_group_members = get_group_members(mixed_restored)

        assert user_name in restored_user_members, (
            f"User {user_name} not restored as member of {mixed_role}"
        )
        assert len(restored_mixed_group_members) == 1, (
            f"Expected 1 Group member in {mixed_role}, got {restored_mixed_group_members}"
        )
        assert ops_group_name in restored_mixed_group_members, (
            f"Group member {ops_group_name} not restored in {mixed_role}"
        )

        self.logger.info("All roles with Group members successfully restored")

    @cluster(num_nodes=4)
    def test_bootstrap_with_recovery(self):
        """
        Smoke test that configuring automated recovery at bootstrap will kick
        in as appropriate.
        """
        rpk = RpkTool(self.redpanda)
        rpk.cluster_config_set(
            "cloud_storage_attempt_cluster_restore_on_bootstrap", True
        )
        for t in self.topics:
            KgoVerifierProducer.oneshot(
                self.test_context,
                self.redpanda,
                t.name,
                self.message_size,
                100,
                batch_max_bytes=self.message_size * 8,
                timeout_sec=60,
            )
        quiesce_uploads(self.redpanda, [t.name for t in self.topics], timeout_sec=60)
        time.sleep(5)

        self.redpanda.stop()
        for n in self.redpanda.nodes:
            self.redpanda.remove_local_data(n)

        # Restart the nodes, overriding the recovery bootstrap config.
        extra_rp_conf = dict(cloud_storage_attempt_cluster_restore_on_bootstrap=True)
        self.redpanda.set_extra_rp_conf(extra_rp_conf)
        self.redpanda.write_bootstrap_cluster_config()
        self.redpanda.restart_nodes(
            self.redpanda.nodes, override_cfg_params=extra_rp_conf
        )

        # We should see a recovery begin automatically.
        self.redpanda._admin.await_stable_leader(
            "controller", partition=0, namespace="redpanda", timeout_s=60, backoff_s=2
        )

        def cluster_recovery_complete():
            return (
                "inactive"
                in self.redpanda._admin.get_cluster_recovery_status().json()["state"]
            )

        wait_until(cluster_recovery_complete, timeout_sec=60, backoff_sec=1)
        self.redpanda.restart_nodes(self.redpanda.nodes)
        assert len(set(rpk.list_topics())) == len(self.topics), (
            "Incorrect number of topics restored"
        )


class ClusterRecoveryWithNameTest(RedpandaTest):
    """
    Tests for Whole Cluster Recovery (WCR) with custom name. I.e. when bucket
    contains data and metadata from multiple clusters.

    This test covers only the name matching logic. The actual recovery
    functionality is covered by ClusterRecoveryTest.
    """

    topics = [
        TopicSpec(
            name=f"topic-{n}",
            replication_factor=1,
        )
        for n in range(3)
    ]

    def __init__(self, test_context: TestContext):
        si_settings = SISettings(test_context, fast_uploads=True)
        self.base_rp_conf = {
            # Test takes too much time otherwise.
            "group_topic_partitions": 2,
            "controller_snapshot_max_age_sec": 1,
            "cloud_storage_cluster_metadata_upload_interval_ms": 1000,
        }
        self.s3_bucket = si_settings.cloud_storage_bucket
        super().__init__(
            test_context=test_context,
            num_brokers=1,
            si_settings=si_settings,
            extra_rp_conf=self.base_rp_conf,
        )

    @cluster(
        num_nodes=1,
        log_allow_list=[
            "Error starting cluster recovery request. Check logs for details.",
            "Error starting cluster recovery request: No matching metadata",
        ],
    )
    def test_admin_recovery(self):
        self.redpanda._admin.patch_cluster_config(
            {
                "cloud_storage_cluster_name": "test-rpk-restore-cluster",
            }
        )
        self._wait_for_metadata_upload()

        self.logger.info("Recreating a brand new cluster without a name")
        self._stop_and_recreate_cluster({})

        with pytest.raises(requests.exceptions.HTTPError) as excinfo:
            self.redpanda._admin.initialize_cluster_recovery()
        assert excinfo.value.response.status_code == 500
        assert (
            excinfo.value.response.json()["message"]
            == "Error starting cluster recovery request. Check logs for details."
        ), excinfo.value.response.json()["message"]

        self.logger.info(
            "Updating cluster name to a random value and attempting recovery"
        )
        self.redpanda._admin.patch_cluster_config(
            {
                "cloud_storage_cluster_name": "some-random-name",
            }
        )

        with pytest.raises(requests.exceptions.HTTPError) as excinfo:
            self.redpanda._admin.initialize_cluster_recovery()
        assert excinfo.value.response.status_code == 500
        assert (
            excinfo.value.response.json()["message"]
            == "Error starting cluster recovery request: No matching metadata"
        ), excinfo.value.response.json()["message"]

        self.logger.info("Setting the correct cluster name and attempting recovery")
        self.redpanda._admin.patch_cluster_config(
            {
                "cloud_storage_cluster_name": "test-rpk-restore-cluster",
            }
        )
        self.redpanda._admin.initialize_cluster_recovery()
        wait_until(self._cluster_recovery_complete, timeout_sec=60, backoff_sec=1)

    @cluster(num_nodes=1)
    def test_admin_recovery_uuid_override(self):
        rpk = RpkTool(self.redpanda)

        self.redpanda._admin.patch_cluster_config(
            {
                "cloud_storage_cluster_name": "test-my-cluster",
            }
        )
        self._wait_for_metadata_upload()
        initial_cluster_uuid = self.redpanda._admin.get_cluster_uuid()
        self.logger.debug(f"Initial cluster uuid: {initial_cluster_uuid}")

        self.logger.info(
            "Recreating the cluster with the same name but do not attempt recovery"
        )
        self._stop_and_recreate_cluster(
            {
                "cloud_storage_cluster_name": "test-my-cluster",
            }
        )
        self._wait_for_metadata_upload()

        self.logger.info("Verifying that no recovery happened")
        topics_on_cluster = set(rpk.list_topics())
        assert len(topics_on_cluster) == 0, (
            f"Expected no topics to be restored but have: {topics_on_cluster}"
        )

        self.logger.info("Recreate the cluster and attempt recovery from the first one")
        self._stop_and_recreate_cluster(
            {
                # Do not set the name yet, we want to validate that recovery fails without it first.
                "cloud_storage_cluster_name": None,
            }
        )
        self.logger.info(
            "Verifying that recovery without name fails if uuid override is provided, name is not set but bucket contains multiple clusters"
        )
        with pytest.raises(requests.exceptions.HTTPError) as excinfo:
            self.redpanda._admin.initialize_cluster_recovery(
                cluster_uuid_override=initial_cluster_uuid
            )
        assert excinfo.value.response.status_code == 400, (
            f"Status: {excinfo.value.response.status_code}"
        )
        assert (
            excinfo.value.response.json()["message"]
            == "Cluster is misconfigured for recovery. Check logs for details."
        ), excinfo.value.response.json()["message"]

        self.logger.info("Set a different cluster name and attempt to recovery")
        self.redpanda._admin.patch_cluster_config(
            {
                "cloud_storage_cluster_name": "some-random-name",
            }
        )
        with pytest.raises(requests.exceptions.HTTPError) as excinfo:
            self.redpanda._admin.initialize_cluster_recovery(
                cluster_uuid_override=initial_cluster_uuid
            )
        assert excinfo.value.response.status_code == 400, (
            f"Status: {excinfo.value.response.status_code}"
        )
        assert (
            excinfo.value.response.json()["message"]
            == "Cluster is misconfigured for recovery. Check logs for details."
        ), excinfo.value.response.json()["message"]

        self.logger.info("Set the correct cluster name and attempt to recovery")
        self.redpanda._admin.patch_cluster_config(
            {
                "cloud_storage_cluster_name": "test-my-cluster",
            }
        )
        self.redpanda._admin.initialize_cluster_recovery(
            cluster_uuid_override=initial_cluster_uuid
        )
        wait_until(self._cluster_recovery_complete, timeout_sec=60, backoff_sec=1)

        topics_on_cluster = set(rpk.list_topics())
        assert len(topics_on_cluster) == len(self.topics), (
            f"Incorrect topics restored {topics_on_cluster} but expected {[t.name for t in self.topics]}"
        )

    @cluster(num_nodes=1)
    def test_bootstrap_with_recovery(self):
        bootstrap_wcr_conf = {
            "cloud_storage_attempt_cluster_restore_on_bootstrap": True,
            "cloud_storage_cluster_name": "the-cool-cluster",
        }

        self.redpanda._admin.patch_cluster_config(bootstrap_wcr_conf)

        # Wait for eventual metadata upload.
        self._wait_for_metadata_upload()

        self.logger.info("Recreating cluster to trigger recovery")
        self._stop_and_recreate_cluster(bootstrap_wcr_conf)

        def _assert_restore_original_cluster():
            rpk = RpkTool(self.redpanda)
            wait_until(self._cluster_recovery_complete, timeout_sec=60, backoff_sec=1)
            topics_on_cluster = set(rpk.list_topics())
            assert len(topics_on_cluster) == len(self.topics), (
                f"Incorrect topics restored {topics_on_cluster} but expected {[t.name for t in self.topics]}"
            )

        _assert_restore_original_cluster()

        # Give it time to upload metadata again so that we can restore it later.
        self._wait_for_metadata_upload()

        # It should be possible to create a new cluster explicitly with different name on the same bucket.
        self.logger.info("Creating a new cluster with different name")
        self._stop_and_recreate_cluster(
            {
                "cloud_storage_attempt_cluster_restore_on_bootstrap": True,
                "cloud_storage_cluster_name": "some-other-name",
            }
        )
        assert (
            "inactive"
            in self.redpanda._admin.get_cluster_recovery_status().json()["state"]
        )

        # Wait for eventual metadata upload.
        self._wait_for_metadata_upload()

        rpk = RpkTool(self.redpanda)
        topics_on_cluster = set(rpk.list_topics())
        assert len(topics_on_cluster) == 0, (
            f"Incorrect topics restored {topics_on_cluster} but expected none"
        )

        # Restore original cluster again.
        self.logger.info("Restoring original cluster again")
        self._stop_and_recreate_cluster(bootstrap_wcr_conf)
        _assert_restore_original_cluster()

    @cluster(
        num_nodes=1,
        log_allow_list=["Error looking for cluster recovery material in cloud"],
    )
    def test_bootstrap_with_recovery_mixed(self):
        """
        Test that recovery fails when cluster name is not set but bucket
        contains data from multiple clusters.
        """
        bootstrap_wcr_conf = {
            "cloud_storage_attempt_cluster_restore_on_bootstrap": True,
            "cloud_storage_cluster_name": "foo-cluster",
        }

        self.redpanda._admin.patch_cluster_config(bootstrap_wcr_conf)
        self._wait_for_metadata_upload()

        self.logger.info("Recreating cluster to trigger recovery and expect failure")
        self._stop_and_recreate_cluster(
            {
                "cloud_storage_attempt_cluster_restore_on_bootstrap": True,
                # No cluster name configured.
            },
            expect_fail=True,
        )
        assert self.redpanda.search_log_any(
            "Error looking for cluster recovery material in cloud, retrying: Cluster misconfiguration"
        )

    def _wait_for_metadata_upload(self):
        self.redpanda.wait_for_controller_snapshot(self.redpanda.nodes[0])
        time.sleep(5)

    def _stop_and_recreate_cluster(
        self, config: dict[str, Any], *, expect_fail: bool = False
    ):
        self.redpanda.stop()
        for n in self.redpanda.nodes:
            self.redpanda.remove_local_data(n)

        # Restart the nodes, overriding the recovery bootstrap config.
        self.redpanda.set_extra_rp_conf(
            {
                **self.base_rp_conf,
                **config,
            }
        )
        self.redpanda.write_bootstrap_cluster_config()
        try:
            self.redpanda.restart_nodes(self.redpanda.nodes)
        except ducktape.errors.TimeoutError:
            if expect_fail:
                self.logger.info(
                    "Cluster failed to start as expected due to recovery failure"
                )
                return
        assert not expect_fail, "Cluster restart was expected to fail but didn't"

        self.redpanda._admin.await_stable_leader(
            "controller",
            partition=0,
            namespace="redpanda",
            timeout_s=60,
            backoff_s=2,
        )

    def _cluster_recovery_complete(self):
        state = self.redpanda._admin.get_cluster_recovery_status().json()["state"]
        self.logger.debug(f"Cluster recovery state: {state}")
        if "failed" in state:
            raise RuntimeError(f"Cluster recovery failed with state: {state}")
        return "inactive" in state
