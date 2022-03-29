# Copyright 2021 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
import requests

from rptest.services.admin import Admin


class HoneyBadger:
    @staticmethod
    def is_enabled(node):
        return requests.get(Admin._url(node,
                                       'failure-probes')).json()['enabled']

    @staticmethod
    def list_failure_probes(node):
        return requests.get(Admin._url(node, 'failure-probes')).json()

    @staticmethod
    def set_delay(node, module, probe):
        requests.post(
            Admin._url(node, f'failure-probes/{module}/{probe}/delay'))

    @staticmethod
    def set_terminate(node, module, probe):
        requests.post(
            Admin._url(node, f'failure-probes/{module}/{probe}/terminate'))

    @staticmethod
    def set_exception(node, module, probe):
        requests.post(
            Admin._url(node, f'failure-probes/{module}/{probe}/exception'))

    @staticmethod
    def unset_failures(node, module, probe):
        requests.delete(Admin._url(node, f'failure-probes/{module}/{probe}'))
