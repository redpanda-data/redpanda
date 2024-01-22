import requests
import json
from ducktape.tests.test import Test
from types import SimpleNamespace
from io import BytesIO

from rptest.services.cluster import cluster
from rptest.tests.redpanda_cloud_test import RedpandaCloudTest


class HTObserveTest(RedpandaCloudTest):
    """
    Cloudv2 only - ensure no firing alarms for cloud cluster - should be ran after all other tests
    this is acomplished by setting @cluster(num_nodes=0) which is good enough
    """
    def __init__(self, test_context):
        super(HTObserveTest, self).__init__(test_context=test_context)
        self._ctx = test_context
        self._token = self.redpanda._cloud_cluster.config.grafana_token
        self._endpoint = self.redpanda._cloud_cluster.config.grafana_alerts_url

    def setUp(self):
        self.redpanda.start()
        self._clusterId = self.redpanda._cloud_cluster.cluster_id

    def load_grafana_rules(self):
        headers = {'Authorization': "Bearer {}".format(self._token)}
        with requests.get(self._endpoint, headers=headers, stream=True) as r:
            if r.status_code != requests.status_codes.codes.ok:
                r.raise_for_status()
            return json.load(BytesIO(r.content),
                             object_hook=lambda d: SimpleNamespace(**d))

    def cluster_alerts(self, rule_groups):
        alerts = []
        for group in rule_groups:
            for rule in group.rules:
                if rule.state != 'firing' or len(rule.alerts) == 0:
                    continue

                if rule.health == 'error':
                    continue

                for alert in rule.alerts:
                    if alert.state != 'Alerting':
                        continue

                    if hasattr(
                            alert.labels, 'redpanda_agent'
                    ) and alert.labels.redpanda_agent == self._clusterId:
                        alerts.append(alert)

                    if hasattr(
                            alert.labels, 'redpanda_id'
                    ) and alert.labels.redpanda_id == self._clusterId:
                        alerts.append(alert)

        return alerts

    @cluster(num_nodes=0, check_allowed_error_logs=False)
    def test_cloud_observe(self):
        self.logger.debug("Here we go")

        rule_groups = self.load_grafana_rules()

        alerts = self.cluster_alerts(rule_groups.data.groups)

        for alert in alerts:
            self.logger.warn(
                f'alert firing for cluster: {alert.labels.grafana_folder} / {alert.labels.alertname}'
            )

        assert len(alerts) == 0
