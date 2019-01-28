#!/usr/bin/env python3


def fmt(num, suffix='B'):
    for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)


if __name__ == '__main__':
    import argparse

    def generate_options():
        parser = argparse.ArgumentParser(description='human bytes helper')
        parser.add_argument(
            '--size', type=str, help='i.e: --size 32345234 or --size "2**20"')
        return parser

    parser = generate_options()
    options, program_options = parser.parse_known_args()
    if options.size is None:
        parser.print_help()
        exit(1)
    options.size = int(eval(options.size))
    print(fmt(options.size))
