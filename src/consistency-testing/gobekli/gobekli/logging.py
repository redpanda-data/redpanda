import json
import time
import logging
import logging.handlers


def setup_logger(logger_name,
                 log_file,
                 maxBytes,
                 backupCount,
                 level=logging.INFO):
    logger = logging.getLogger(logger_name)
    formatter = logging.Formatter('%(message)s')
    fileHandler = logging.handlers.RotatingFileHandler(log_file,
                                                       maxBytes=maxBytes,
                                                       backupCount=backupCount,
                                                       mode='w')
    fileHandler.setFormatter(formatter)
    logger.setLevel(level)
    logger.addHandler(fileHandler)


def init_logs(cmd_log_file, latency_file):
    setup_logger("cmd", cmd_log_file, 2 * 1024 * 1024, 5)
    setup_logger("latency", latency_file, 10 * 1024 * 1024, 5)


def strtime():
    return time.strftime("%Y-%m-%d %H:%M:%S")


def log_write_started(node, pid, write_id, key, prev_write_id, version, value):
    log = logging.getLogger("cmd")
    log.info(
        json.dumps({
            "time": strtime(),
            "type": "write_stared",
            "node": node,
            "pid": pid,
            "key": key,
            "write_id": write_id,
            "prev_write_id": prev_write_id,
            "version": version,
            "value": value
        }))


def log_write_ended(node, pid, key, write_id, value):
    log = logging.getLogger("cmd")
    log.info(
        json.dumps({
            "time": strtime(),
            "type": "write_ended",
            "node": node,
            "pid": pid,
            "key": key,
            "write_id": write_id,
            "value": value
        }))


def log_write_timeouted(node, pid, key):
    log = logging.getLogger("cmd")
    log.info(
        json.dumps({
            "time": strtime(),
            "type": "write_timedout",
            "node": node,
            "pid": pid,
            "key": key
        }))


def log_write_failed(node, pid, key):
    log = logging.getLogger("cmd")
    log.info(
        json.dumps({
            "time": strtime(),
            "type": "write_canceled",
            "node": node,
            "pid": pid,
            "key": key
        }))


def log_read_started(node, pid, key):
    log = logging.getLogger("cmd")
    log.info(
        json.dumps({
            "time": strtime(),
            "type": "read_started",
            "node": node,
            "pid": pid,
            "key": key
        }))


def log_read_none(node, pid, key):
    log = logging.getLogger("cmd")
    log.info(
        json.dumps({
            "time": strtime(),
            "type": "read_404",
            "node": node,
            "pid": pid,
            "key": key
        }))


def log_read_ended(node, pid, key, write_id, value):
    log = logging.getLogger("cmd")
    log.info(
        json.dumps({
            "time": strtime(),
            "type": "read_ended",
            "node": node,
            "pid": pid,
            "key": key,
            "write_id": write_id,
            "value": value
        }))


def log_read_timeouted(node, pid, key):
    log = logging.getLogger("cmd")
    log.info(
        json.dumps({
            "time": strtime(),
            "type": "read_timedout",
            "node": node,
            "pid": pid,
            "key": key
        }))


def log_read_failed(node, pid, key):
    log = logging.getLogger("cmd")
    log.info(
        json.dumps({
            "time": strtime(),
            "type": "read_canceled",
            "node": node,
            "pid": pid,
            "key": key
        }))


def log_violation(pid, message):
    log = logging.getLogger("cmd")
    log.info(
        json.dumps({
            "time": strtime(),
            "type": "linearizability_violation",
            "pid": pid,
            "message": message
        }))


def log_latency(type, time_s, latency_s):
    log = logging.getLogger("latency")
    log.info(f"{int(time_s)}\t{int(latency_s*1000)}\t{type}")
