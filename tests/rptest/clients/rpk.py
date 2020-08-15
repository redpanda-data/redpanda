import subprocess


class RpkTool:
    """
    Wrapper around rpk.
    """
    def __init__(self, redpanda):
        self._redpanda = redpanda

    def create_topic(self, topic, partitions=1):
        cmd = ["topic", "create", topic]
        cmd += ["--partitions", str(partitions)]
        return self._run_api(cmd)

    def list_topics(self):
        cmd = ["topic", "list"]

        output = self._run_api(cmd)
        if "No topics found." in output:
            return []

        def topic_line(line):
            parts = line.split()
            assert len(parts) == 3
            return parts[0]

        lines = output.splitlines()
        for i, line in enumerate(lines):
            if line.split() == ["Name", "Partitions", "Replicas"]:
                return map(topic_line, lines[i + 1:])

        assert False, "Unexpected output format"

    def _run_api(self, cmd):
        cmd = [
            self._rpk_binary(), "api", "--brokers",
            self._redpanda.brokers(1)
        ] + cmd
        return self._execute(cmd)

    def _execute(self, cmd):
        self._redpanda.logger.debug("Executing command: %s", cmd)
        try:
            res = subprocess.check_output(cmd, stderr=subprocess.STDOUT)
            self._redpanda.logger.debug(res)
            return res
        except subprocess.CalledProcessError as e:
            self._redpanda.logger.debug("Error (%d) executing command: %s",
                                        e.returncode, e.output)

    def _rpk_binary(self):
        return self._redpanda.find_binary("rpk")
