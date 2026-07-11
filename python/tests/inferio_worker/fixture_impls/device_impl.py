"""Test fixture impl reporting its CUDA_VISIBLE_DEVICES pin.

Every output is `{"device": os.environ.get("CUDA_VISIBLE_DEVICES")}`, so a
multi-replica test can observe which device pin the serving replica was
spawned with — no GPU involved, the env var is just read back. The short
sleep keeps one replica busy long enough for concurrent requests to spill
onto the other replica, making "both replicas actually served" observable.
"""

import os
import time


class DeviceModel:
    def __init__(self, **config):
        self.config = config

    @classmethod
    def name(cls) -> str:
        return "device_test"

    def load(self) -> None:
        pass

    def predict(self, inputs):
        time.sleep(0.5)
        device = os.environ.get("CUDA_VISIBLE_DEVICES")
        return [{"device": device} for _ in inputs]

    def unload(self) -> None:
        pass


IMPL_CLASS = DeviceModel
