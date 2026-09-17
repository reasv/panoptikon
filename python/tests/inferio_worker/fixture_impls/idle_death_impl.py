"""Test fixture impl whose worker dies while *idle*, some time after load.

`load()` starts a daemon thread that sleeps `die_after_seconds` and then
`os._exit(9)` — abrupt in the way an out-of-band kill (kernel OOM killer,
driver) is: no traceback, no unload, the parent simply finds the process
gone. It is an *exit code* 9, not a signal 9, so the death record reads
`exit status: 9` with `signal: None`; the terminating-signal half of the
record is covered by worker.rs' own tests, which SIGKILL a real child.
The death happens with **no request in flight**, which is the whole point:
nothing reads the worker's pipe, so EOF is never noticed and only the
manager's liveness sweep can discover it.

The delay is deliberately long enough for the load and one predict to
finish first, so the model is genuinely resident-and-idle when it dies.
"""

import os
import threading
import time


class IdleDeathModel:
    def __init__(self, **config):
        self.config = config
        self.die_after_seconds = float(config.get("die_after_seconds", 2.0))

    @classmethod
    def name(cls) -> str:
        return "idle_death_test"

    def load(self) -> None:
        threading.Thread(target=self._die_later, daemon=True).start()

    def _die_later(self) -> None:
        time.sleep(self.die_after_seconds)
        os._exit(9)

    def predict(self, inputs):
        return [{"echo": inp.data} for inp in inputs]

    def unload(self) -> None:
        pass


IMPL_CLASS = IdleDeathModel
