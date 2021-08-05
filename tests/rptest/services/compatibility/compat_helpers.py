import os


#A factory method to produce the time out exception
#operatr-io experienced. The exception is caused by
#the coordinator_load_in_progress error.
def operatrio_example(redpanda):
    script = os.path.join("/opt", "operatrio/operatrio.py")
    cmd = f"python3 {script} {redpanda.brokers()}"

    return cmd
