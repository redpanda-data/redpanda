import json
import time
import logging
import logging.handlers


class m:
    def __init__(self, message=None, **kwargs):
        self.kwargs = kwargs
        if message is not None:
            self.kwargs["message"] = message

    def with_time(self):
        self.kwargs["time"] = int(time.time())
        return self

    def __str__(self):
        return json.dumps(self.kwargs)


latency_metrics = []


def setup_logger(logger_name,
                 log_file,
                 maxBytes,
                 backupCount,
                 level=logging.INFO):
    logger = logging.getLogger(logger_name)
    logger.handlers = []
    formatter = logging.Formatter('%(message)s')
    fileHandler = logging.handlers.RotatingFileHandler(log_file,
                                                       maxBytes=maxBytes,
                                                       backupCount=backupCount,
                                                       mode='w')
    fileHandler.setFormatter(formatter)
    logger.setLevel(level)
    logger.addHandler(fileHandler)
    return logger


def init_logs(cmd_log_file, latency_file, stat_file, ss_metrics):
    global latency_metrics
    latency_metrics = ss_metrics
    setup_logger("gobekli-latency", latency_file, 10 * 1024 * 1024, 5)
    setup_logger("gobekli-cmd", cmd_log_file, 1 * 1024 * 1024, 5)
    setup_logger("gobekli-availability", stat_file, 10 * 1024 * 1024, 5)

    # adding console handler
    console = logging.getLogger("gobekli-stdout")
    console.handlers = []
    formatter = logging.Formatter('%(message)s')
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    console.setLevel(logging.INFO)
    console.addHandler(ch)


def strtime():
    return time.strftime("%Y-%m-%d %H:%M:%S")


def log_write_started(node, pid, write_id, key, prev_write_id, version, value):
    log = logging.getLogger("gobekli-cmd")
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
    log = logging.getLogger("gobekli-cmd")
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
    log = logging.getLogger("gobekli-cmd")
    log.info(
        json.dumps({
            "time": strtime(),
            "type": "write_timedout",
            "node": node,
            "pid": pid,
            "key": key
        }))


def log_write_failed(node, pid, key):
    log = logging.getLogger("gobekli-cmd")
    log.info(
        json.dumps({
            "time": strtime(),
            "type": "write_canceled",
            "node": node,
            "pid": pid,
            "key": key
        }))


def log_read_started(node, pid, key):
    log = logging.getLogger("gobekli-cmd")
    log.info(
        json.dumps({
            "time": strtime(),
            "type": "read_started",
            "node": node,
            "pid": pid,
            "key": key
        }))


def log_read_none(node, pid, key):
    log = logging.getLogger("gobekli-cmd")
    log.info(
        json.dumps({
            "time": strtime(),
            "type": "read_404",
            "node": node,
            "pid": pid,
            "key": key
        }))


def log_read_ended(node, pid, key, write_id, value):
    log = logging.getLogger("gobekli-cmd")
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
    log = logging.getLogger("gobekli-cmd")
    log.info(
        json.dumps({
            "time": strtime(),
            "type": "read_timedout",
            "node": node,
            "pid": pid,
            "key": key
        }))


def log_read_failed(node, pid, key):
    log = logging.getLogger("gobekli-cmd")
    log.info(
        json.dumps({
            "time": strtime(),
            "type": "read_canceled",
            "node": node,
            "pid": pid,
            "key": key
        }))


def log_violation(pid, message):
    log = logging.getLogger("gobekli-cmd")
    log.info(
        json.dumps({
            "time": strtime(),
            "type": "linearizability_violation",
            "pid": pid,
            "message": message
        }))


def log_latency(type, time_s, latency_s, metrics=None):
    log = logging.getLogger("gobekli-latency")
    message = f"{int(time_s*1000*1000)}\t{int(latency_s*1000*1000)}\t{type}"
    if metrics != None:
        for key in latency_metrics:
            message += "\t"
            if key in metrics:
                message += str(metrics[key])
    log.info(message)


def log_stat(entry):
    log = logging.getLogger("gobekli-availability")
    log.info(json.dumps(entry))


def log_console(message):
    log = logging.getLogger("gobekli-stdout")
    log.info(message)
