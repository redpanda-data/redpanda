import json
import logging


def safe_decode(val):
    if isinstance(val, bytes):
        return val.decode("utf-8", errors="replace")
    if isinstance(val, memoryview):
        return val.tobytes().decode("utf-8", errors="replace")
    return str(val) if val is not None else ""


def canonical_json(val):
    """Serialize dicts to canonical JSON strings for comparison."""
    return json.dumps(val, sort_keys=True) if isinstance(val, dict) else val


def verify_rows(rows, expected_records, logger):
    logger.debug("Starting verify_rows")

    # Build lookup map of expected records
    expected_lookup = {
        (key, canonical_json(value), frozenset(headers.items())):
        (key, value, headers)
        for key, value, headers in expected_records
    }

    found_keys = set()
    errors = []

    for row in rows:
        key_str = safe_decode(row[0])
        value_str = safe_decode(row[1])
        headers = row[2]

        header_dict = {}
        if headers:
            for h in headers:
                header_key = safe_decode(h.get("key"))
                header_value = safe_decode(h.get("value"))
                header_dict[header_key] = header_value

        logger.debug(
            f"Actual row: key={key_str}, value={value_str}, headers={header_dict}"
        )

        try:
            parsed_json = json.loads(value_str)
        except json.JSONDecodeError:
            parsed_json = None

        actual_val = canonical_json(
            parsed_json if parsed_json is not None else value_str)
        key_tuple = (key_str, actual_val, frozenset(header_dict.items()))

        logger.debug(f"Checking:\n"
                     f"  Key     : {key_str}\n"
                     f"  Value   : {actual_val}\n"
                     f"  Headers : {header_dict}")

        if key_tuple in expected_lookup:
            logger.debug(f"Match found for key={key_str}")
            found_keys.add(key_tuple)
        else:
            logger.debug(
                f"No match for key={key_str}, value={actual_val}, headers={header_dict}"
            )

    # Identify missing records
    missing = set(expected_lookup.keys()) - found_keys
    for key in missing:
        key_str, val_str, headers = key
        original = expected_lookup[key]
        msg = (
            f"Missing record:\n"
            f"  Key     : {original[0]}\n"
            f"  Value   : {json.dumps(original[1], indent=2) if isinstance(original[1], dict) else original[1]}\n"
            f"  Headers : {original[2]}")
        logger.error(msg)
        errors.append(msg)

    if errors:
        logger.warning(f"⚠️ Missing {len(errors)} record(s).")
        return False, errors

    logger.info("All expected records found.")
    return True, []
