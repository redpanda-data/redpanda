import subprocess
import time
import json


class KafkaCat:
    """
    Wrapper around the kafkacat utility.

    This tools is useful because it offers a JSON output format.
    """
    def __init__(self, redpanda):
        self._redpanda = redpanda

    def metadata(self):
        return self._cmd(["-L"])

    def _cmd(self, cmd):
        brokers = ",".join(
            map(lambda n: "{}:9092".format(n.account.hostname),
                self._redpanda.nodes))
        for retry in reversed(range(10)):
            try:
                res = subprocess.check_output(
                    ["kafkacat", "-b", brokers, "-J"] + cmd,
                    stderr=subprocess.STDOUT)
                res = json.loads(res)
                self._redpanda.logger.debug(json.dumps(res, indent=2))
                return res
            except subprocess.CalledProcessError as e:
                if retry == 0:
                    raise
                self._redpanda.logger.debug(
                    "kafkacat retrying after exit code {}: {}".format(
                        e.returncode, e.output))
                time.sleep(2)
