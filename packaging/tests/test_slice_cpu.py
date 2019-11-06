#!/usr/bin/env python3

import argparse
from datetime import datetime
import os
import shutil
import subprocess
import sys
import time
import urllib

hog_cmd = 'while true; do true; done'


def _run_test(id, duration, interval, n_hogs, use_slice):
    print(f'''running test:
    - id: {id}
    - duration: {duration}s
    - interval: {interval}s
    - number of cpu hogs: {n_hogs}
    - using slice: {use_slice}
''')
    ps_file = _open(f'ps-{id}.txt')
    svc_name = f'slice-test-{id}'

    hogs = []

    try:
        _reload_systemctl()

        pid = _start_service(svc_name, duration, use_slice)
        print(f'Service running with PID {pid}')

        ps = f'ps --pid {pid} --no-headers -o %cpu,comm --cols 400'

        for i in range(0, n_hogs):
            hogs.append(_popen(hog_cmd))

        # This loop ends when one of the commands fails (e.g when ps fails to
        # find info on the service after it's finished)
        while True:
            time.sleep(interval)
            ps_output = _check_output(ps)
            if ps_output is None: break
            _write_timestamped(ps_file, ps_output)

    except (KeyboardInterrupt, Exception) as ex:
        print(f'the test has finished: {ex}')

    _cleanup(svc_name, hogs)


def _start_service(svc_name, duration, use_slice):
    start_cmd = f'''systemd-run {"--slice redpanda.slice" if use_slice else ""} \
    --unit {svc_name} \
    /usr/bin/iotune --duration {duration} --evaluation-directory ./'''
    _check_output(start_cmd)
    return _check_output(
        f'systemctl show {svc_name} --property MainPID --value').strip()


def _cleanup(svc_name, hogs):
    _check_output(f'systemctl kill {svc_name} || :')
    print('killing cpu hog processes')
    for i in range(len(hogs)):
        print(f'killing hog #{i}')
        hogs[i].kill()


def _reload_systemctl():
    _check_output('systemctl daemon-reload')
    _check_output('systemctl reset-failed')


def _write_timestamped(file, content):
    file.write(f'{datetime.now()} {content}')


def _check_output(cmd):
    print(f'(_check_output) running {cmd}')
    ret = subprocess.check_output(cmd, shell=True)
    if ret is None: return ret
    return str(ret.decode("utf-8"))


def _popen(cmd, env=os.environ):
    print(f'(_popen) running {cmd}')
    return subprocess.Popen(f"exec bash -c '{cmd}'",
                            stdout=sys.stdout,
                            stderr=sys.stderr,
                            env=env,
                            shell=True)


def _open(path):
    print(f'creating {path}')
    return open(path, 'w+')


def main():
    parser = _build_parser()
    options = parser.parse_args()

    id = options.id
    duration = options.duration
    interval = options.interval
    n_hogs = options.hogs

    _run_test(id, duration, interval, n_hogs, True)

    id = f'{id}-no-slice'
    _run_test(id, duration, interval, n_hogs, False)


def _build_parser():
    parser = argparse.ArgumentParser(description='systemd slice test')
    parser.add_argument(
        '--id',
        type=str,
        default='1',
        help=
        'A unique id for the test. Used to avoid conflicts with existing systemd files.'
    )
    parser.add_argument('--duration',
                        type=int,
                        default=1200,
                        help="The test's duration (in seconds)")
    parser.add_argument('--interval',
                        type=int,
                        default=10,
                        help="""The sampling interval (in seconds).
                        Depending on the hardware, sampling the CPU usage with
                        can take more than 1s while running the tests with the
                        slice, because it prioritizes the processes running
                        inside it over anything else.
                        This means intervals lower than ~2s might not be
                        effective during the part of the test that uses the
                        redpanda.slice config""")
    parser.add_argument(
        '--hogs',
        type=int,
        default=16,
        help="The number of cpu hog processes to run in parallel")
    return parser


if __name__ == '__main__':
    main()
