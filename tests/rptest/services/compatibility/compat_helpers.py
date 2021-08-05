import os


#A factory method to produce the time out exception
#operatr-io experienced. The exception is caused by
#the coordinator_load_in_progress error.
def operatrio_example(redpanda):
    EXAMPLE_DIR = os.path.join("/opt", "redpanda-reproducer-1")
    cmd = f"cd {EXAMPLE_DIR} && lein run {redpanda.brokers()}"

    return cmd
