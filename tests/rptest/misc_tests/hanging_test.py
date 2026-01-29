from ducktape.mark.resource import cluster
from ducktape.tests.test import Test
import threading
import time


class HangingTest(Test):
    @cluster(num_nodes=0)
    def test_hanging_thread(self):
        # this test illustrates the hanging issue in CORE-13453
        # see also: https://github.com/redpanda-data/redpanda/pull/28745
        # https://github.com/redpanda-data/ducktape/pull/52
        # https://redpandadata.atlassian.net/wiki/spaces/CORE/pages/1360658457/Diagnosing+ducktape+failures#Type-IV%3A-Hang-after-tear-down

        # The whole HangingTest should be run to reproduce, test_zafter_2 will fail
        # in addition to `test_hanging_thread` due to the cross-test corruption
        self.logger.info("Test begin")

        # create a daemon thread then fail
        def worker():
            while True:
                time.sleep(1)

        t0 = threading.Thread(target=worker)
        t0.start()

        t0 = threading.Thread(target=worker, daemon=True)
        t0.start()

        self.logger.info("Started non-daemon and daemon threads, now failing test.")

        raise RuntimeError("Failing test after starting daemon")

    @cluster(num_nodes=0)
    def test_zafter_1(self):
        self.logger.info("This test will now wait for 60 seconds.")
        time.sleep(1)

    @cluster(num_nodes=0)
    def test_zafter_2(self):
        self.logger.info("This test will now wait for 60 seconds.")
        time.sleep(1)
