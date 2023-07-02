import sys
import os
from pathlib import Path

if __name__ == "__main__":
    size = 0
    dir = Path(sys.argv[1])
    for curr, _, files in os.walk(dir):
        for file in files:
            path = os.path.join(curr, file)
            try:
                size += os.path.getsize(path)
            except FileNotFoundError:
                continue
    sys.stdout.write(f"{size}")
