import sys
import re
import socket
import threading


class SpammingConnections(threading.Thread):
    def __init__(self, host_ip):
        super(SpammingConnections, self).__init__()
        host_ip = re.split(":", host_ip)
        self.host = host_ip[0]
        self.port = int(host_ip[1])

    def run(self):
        while True:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((self.host, self.port))


if __name__ == "__main__":
    broker = sys.argv[1]

    n_workers = 20
    workers = []
    for n in range(0, n_workers):
        worker = SpammingConnections(broker)
        workers.append(worker)

    for worker in workers:
        worker.start()

    for worker in workers:
        worker.join()
