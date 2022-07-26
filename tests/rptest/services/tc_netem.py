from enum import Enum
from statistics import correlation

import typing

# for more information about netem options visit `tc-netem` man7 page


class NetemDelayDistribution(Enum):
    UNIFORM = "uniform"
    NORMAL = "normal"
    PARETO = "pareto"
    PARETO_NORMAL = "paretonormal"


class NetemDelay(typing.NamedTuple):
    delay_us: int
    jitter_us: int
    correlation_us: str
    distribution: NetemDelayDistribution

    def validate(self):
        if not self.delay_us:
            raise Exception("delay_us is required for netem delay option")
        if self.correlation_us and not self.jitter_us:
            raise Exception(
                "when correlation_us is provided jitter is required for netem delay"
            )
        if self.distribution and not self.jitter_us:
            raise Exception(
                "when distribution is provided jitter is required for netem delay"
            )

    def get_command(self) -> str:
        cmd = ["netem delay", str(self.delay_us)]

        if self.jitter_us:
            cmd.append(str(self.jitter_us))

            if self.correlation_us:
                cmd.append(str(self.jitter_us))
            if self.distribution:
                cmd.append(f"distribution {self.distribution.value}")

        return " ".join(cmd)


class NetemLoss(typing.NamedTuple):
    percent: float

    def validate(self):
        pass

    def get_command(self) -> str:
        cmd = ["netem loss", str(self.percent)]
        return " ".join(cmd)


class NetemCorrupt(typing.NamedTuple):
    percent: float
    correlation: float

    def validate(self):
        pass

    def get_command(self) -> str:
        cmd = ["netem corrupt", str(self.percent)]
        if self.correlation:
            cmd.append(str(self.correlation))

        return " ".join(cmd)


class NetemDuplicate(typing.NamedTuple):
    percent: float
    correlation: float

    def validate(self):
        pass

    def get_command(self) -> str:
        cmd = ["netem duplicate", str(self.percent)]
        if self.correlation:
            cmd.append(str(self.correlation))

        return " ".join(cmd)


def _list_network_devices(node):
    lines = node.account.ssh_output('ls /sys/class/net/').decode('utf-8')
    return [d for d in lines.splitlines() if d != 'lo']


def _tc_netem_execute(
    node,
    operation,
    command,
    device=None,
):
    ret = {}
    devices = []
    if device is None:
        devices = _list_network_devices(node)
    else:
        devices.append(device)
    for d in devices:
        cmd = f"tc qdisc {operation} dev {d} root {command}"
        res = node.account.ssh_output(cmd)
        ret[d] = res
    return ret


def tc_netem_add(node, option, device=None):
    option.validate()
    return _tc_netem_execute(node, 'add', option.get_command(), device)


def tc_netem_delete(node, device=None):
    return _tc_netem_execute(node, 'delete', "netem", device)


def tc_netem_list(node, device=None):
    return _tc_netem_execute(node, 'list', "", device)
