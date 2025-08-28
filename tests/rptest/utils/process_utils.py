import subprocess


class CalledProcessError(subprocess.CalledProcessError):
    """An extension of subprocess.CalledProcessError which includes the last line of stderr
    in the __str__ method."""

    def __init__(self, orignal: subprocess.CalledProcessError):
        super().__init__(
            orignal.returncode, orignal.cmd, orignal.output, orignal.stderr
        )

    def __str__(self):
        last_line = self.stderr.splitlines()[-1] if self.stderr else ""
        return f"{super().__str__()} last line of stderr: {last_line}"
