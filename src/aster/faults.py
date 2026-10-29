import os
import signal

FAULT_POINT = "ASTER_FAULT_POINT"


def hit(point):
    if os.environ.get(FAULT_POINT) == point:
        os.kill(os.getpid(), signal.SIGKILL)
