import unittest

import vtools.punisher.commands


class TestPunisher(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testParsingHostLine(self):
        line = '54.201.253.32 ansible_user=admin ansible_become=True private_ip=172.31.18.222 id=1'
        n = vtools.punisher.commands._parse_redpanda_host_line(line)
        assert n['ip'] == '54.201.253.32'
        assert n['ansible_user'] == 'admin'
        assert n['private_ip'] == '172.31.18.222'
        assert n['id'] == '1'

    def testWeightedChoice(self):
        commands_weights = [
            vtools.punisher.commands.PunisherCommand('nop', 5),
            vtools.punisher.commands.PunisherCommand('transient_kill', 80),
            vtools.punisher.commands.PunisherCommand('stop', 10),
            vtools.punisher.commands.PunisherCommand('kill', 0),
        ]
        choice = vtools.punisher.commands._weighted_choice(commands_weights)
        assert choice in ['nop', 'transient_kill', 'stop']
