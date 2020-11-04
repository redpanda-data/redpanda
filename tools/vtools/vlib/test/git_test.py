import unittest

from .. import git


class TestGit(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testValidEmail(self):
        assert git.verify_is_vectorized_address("alex@vectorized.io")

    def testInvalidEmail(self):
        assert not git.verify_is_vectorized_address("alex@gmail.io")
