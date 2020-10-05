import json
import time
import logging
import logging.handlers

cmdlog = logging.getLogger("gobekli-cmd")
latlog = logging.getLogger("gobekli-latency")


class m:
    def __init__(self, message=None, **kwargs):
        self.kwargs = kwargs
        if message is not None:
            self.kwargs["message"] = message

    def with_time(self):
        self.kwargs["time_ms"] = int(time.time() * 1000)
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


def log_violation(pid, message):
    cmdlog.info(
        m(type="linearizability_violation", pid=pid,
          message=message).with_time())


def log_latency(type, time_s, latency_s, metrics=None):
    message = f"{int(time_s*1000*1000)}\t{int(latency_s*1000*1000)}\t{type}"
    if metrics != None:
        for key in latency_metrics:
            message += "\t"
            if key in metrics:
                message += str(metrics[key])
    latlog.info(message)
