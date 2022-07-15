# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.cluster.cluster_spec import ClusterSpec
from ducktape.mark.resource import cluster
from ducktape.mark import matrix
from ducktape.tests.test import Test
from re import match
from rptest.services.redpanda import RedpandaService


class MaterializeTest(Test):
    MATERIALZE_HOME = '/opt/materialize'
    SERVICE_FILE_PY = f'{MATERIALZE_HOME}/misc/python/materialize/mzcompose/services.py'
    TESTDRIVE_DIR = f'{MATERIALZE_HOME}/test/testdrive'

    # These are the versions of the test suite that each
    # minor release of Redpanda should be tested against.
    VERSION_COMMIT_MAP = {
        # RP minor release --> SHA @ github.com/MaterializeInc/materialize
        'v21.11.13': '2f0e02098aad303f25b758cb922f5134fb1d781c',
        'v22.1.3': 'a6ef2866462b9c34c53c9f60f413353f69ae8d2d',
        'v22.1.4': 'a6ef2866462b9c34c53c9f60f413353f69ae8d2d',
        'v22.1.5-rc1': 'a6ef2866462b9c34c53c9f60f413353f69ae8d2d'
    }

    def __init__(self, test_context):
        super(MaterializeTest, self).__init__(test_context=test_context)
        # Capturing dedicated nodes manually instead of via RedpandaService
        # because the materialize test suite already sets up a RP cluster.
        # So there is no need to startup our own RP cluster.
        self.dedicated_nodes = self.test_context.globals.get(
            RedpandaService.DEDICATED_NODE_KEY, False)

        self.node = None

    def setUp(self):
        super().setUp()

        self.node = self.test_context.cluster.alloc(
            ClusterSpec.simple_linux(1))[0]

        rp_version = self.test_context.injected_args_name.split('=')[-1]

        # Checkout the appropriate SHA in the materialize repo
        git_checkout_cmd = f'cd {self.MATERIALZE_HOME} && git checkout {self.VERSION_COMMIT_MAP[rp_version]}'
        checkout_failure = Exception('git checkout failed')

        for line in self.node.account.ssh_capture(
                git_checkout_cmd,
                allow_fail=True):  # Allow fail for full output
            line = line.strip()

            m = match('(HEAD is now at )(.*)', line)
            if m != None:
                checkout_failure = None

        # Raise if the checkout condition was never met
        if checkout_failure != None:
            raise checkout_failure

        # Set target redpanda version
        set_version_cmd = f'sed -i \"/version: str = /s@\\"[^\\"]*\\"@\\"{rp_version}\\"@\" \"{self.SERVICE_FILE_PY}\"'
        self.node.account.ssh(set_version_cmd)  # Will raise on non-zero status

        if '-' in rp_version and not rp_version.startswith('v21'):
            self.logger.debug(f'Version {rp_version} requires unstable repo')
            set_image_cmd = f'sed -i\'\' \"s@vectorized/redpanda@vectorized/redpanda-unstable@\" \"{self.SERVICE_FILE_PY}\"'
            self.node.account.ssh(set_image_cmd)

    def tearDown(self):
        # Teardown materialize tests
        teardown_cmd = f'cd {self.TESTDRIVE_DIR} && ./mzcompose down -v'
        for line in self.node.account.ssh_capture(teardown_cmd):
            self.logger.debug(line.strip())

        # Undo changes in materialize repo such as changing the
        # redpanda version. Otherwise running materialize tests
        # on the same node will fail to checkout other SHA
        git_stash_cmd = f'cd {self.MATERIALZE_HOME} && git stash push && git stash drop'
        self.node.account.ssh(git_stash_cmd)

        super().tearDown()

    def free_nodes(self):
        self.test_context.cluster.free_single(self.node)

        super().free_nodes()

    @cluster(num_nodes=1)
    @matrix(rp_version=['v21.11.13', 'v22.1.3', 'v22.1.4', 'v22.1.5-rc1'])
    def test_materialize(self, rp_version):
        # Let's only run materialize tests on
        # CDT nightly (i.e., envs with dedicated nodes)
        if not self.dedicated_nodes:
            self.logger.info('Only run materialize tests on dedicated_nodes')
            return

        # Run materialize test
        run_test_cmd = f'cd {self.TESTDRIVE_DIR} && ./mzcompose run default --redpanda'
        test_failure = Exception('mzcompose run failed')
        for line in self.node.account.ssh_capture(run_test_cmd,
                                                  allow_fail=True):
            line = line.strip()
            self.logger.debug(line)

            m = match('.*test case workflow-default succeeded.*', line)
            if m != None:
                test_failure = None

        # Raise if the test condition was never met
        if test_failure != None:
            raise test_failure