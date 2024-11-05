from ducktape.mark.resource import cluster
from ducktape.tests.test import Test


class EmptyTest(Test):
    def __init__(self, test_context):
        super(EmptyTest, self).__init__(test_context)

    @cluster(num_nodes=0)
    def test_empty(self):
        # ducktape throws an error when its loader finds no tests after
        # processing a suite (defined in a YAML file). In some cases, this
        # limitation ends up causing a ducktape session to fail. For example, if
        # all tests included in a suite are tagged with `@skip_debug_mode`, the
        # suite ends up being empty for `BUILD_TYPE=debug`. To avoid this
        # scenario, this empty test can be included in a suite so that at least
        # one test gets loaded.
        pass
