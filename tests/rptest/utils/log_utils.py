from ducktape.utils.util import wait_until

from rptest.services.redpanda import RedpandaService


def wait_until_nag_is_set(redpanda: RedpandaService,
                          check_interval_sec: int,
                          timeout_sec: int = 30):
    """
    Waits until the log message indicating override of the periodic reminder
    interval has been set

    Parameters:
    redpanda (RedpandaService): Redpanda service instance to query the logs from
    check_interval_sec (int): The interval in seconds that should be logged by Redpanda
    timeotu_sec (int): The maximum time to wait for the log message to appear
    """
    def nag_check() -> bool:
        return redpanda.search_log_all(
            f"Overriding default reminder period interval to: {check_interval_sec}s"
        )

    wait_until(nag_check,
               timeout_sec=timeout_sec,
               err_msg="Failed to set periodic reminder interval")
