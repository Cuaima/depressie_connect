from __future__ import annotations
import itertools
import sys
import threading
import time


class Spinner:
    """Thread-based terminal spinner for long blocking operations.

    Usage::

        with Spinner("Building PDF"):
            expensive_call()
    """

    def __init__(self, message: str = "Working"):
        self.message = message
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._spin, daemon=True)

    def _spin(self) -> None:
        for ch in itertools.cycle(r"|/-\\"):
            if self._stop.is_set():
                break
            sys.stdout.write(f"\r{self.message}... {ch}")
            sys.stdout.flush()
            time.sleep(0.1)
        # clear the spinner line
        sys.stdout.write("\r" + " " * (len(self.message) + 6) + "\r")
        sys.stdout.flush()

    def __enter__(self) -> "Spinner":
        self._thread.start()
        return self

    def __exit__(self, *exc) -> None:
        self._stop.set()
        self._thread.join()
