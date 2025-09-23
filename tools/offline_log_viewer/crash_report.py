from typing import Any

from reader import Reader


def decode_crash_report(path: str) -> dict[str, Any]:
    return Reader(open(path, "rb")).read_envelope(
        lambda rdr, _: {
            "type": rdr.read_int32(),
            "timestamp": rdr.read_int64(),
            "crash_message": rdr.read_string(),
            "stacktrace": rdr.read_string(),
            "app_version": rdr.read_string(),
            "arch": rdr.read_string(),
        }
    )
