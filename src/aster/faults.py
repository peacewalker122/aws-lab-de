import os
import signal

FAULT_POINT = "ASTER_FAULT_POINT"
FAILURE_POINT = "ASTER_FAULT_FAILURE"


class InjectedFault(RuntimeError):
    pass


def hit(point):
    if os.environ.get(FAULT_POINT) == point:
        os.kill(os.getpid(), signal.SIGKILL)


def fail(point):
    if os.environ.get(FAILURE_POINT) == point:
        raise InjectedFault(f"Injected fault at {point}")
