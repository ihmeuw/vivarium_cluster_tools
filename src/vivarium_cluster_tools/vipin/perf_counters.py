"""
==========================
vipin Performance Counters
==========================

Structs for counting performance metrics.

"""
import json
from time import time
from typing import Any

import psutil


class CounterSnapshot:
    def __init__(
        self,
        cpu: Any = None,
        disk: Any = None,
        freq: Any = None,
        net: Any = None,
        timestamp: float | None = None,
    ) -> None:
        self.cpu = psutil.cpu_stats() if cpu is None else cpu
        self.disk = (
            psutil.disk_io_counters(perdisk=False, nowrap=True) if disk is None else disk
        )
        self.freq = psutil.cpu_freq(percpu=False) if freq is None else freq
        self.net = psutil.net_io_counters(pernic=False, nowrap=True) if net is None else net
        self.timestamp = time() if timestamp is None else timestamp

    def to_dict(self) -> dict[str, Any]:
        c_dict: dict[str, Any] = dict()
        c_dict["cpu"] = dict(self.cpu._asdict()) if self.cpu else {}
        c_dict["freq"] = dict(self.freq._asdict()) if self.freq else {}
        c_dict["disk"] = dict(self.disk._asdict()) if self.disk else {}
        c_dict["net"] = dict(self.net._asdict()) if self.net else {}
        c_dict["time"] = self.timestamp
        return c_dict

    def __sub__(self, other: "CounterSnapshot") -> "CounterSnapshot":
        # Handle potential None values by using defaults or skipping operations
        if other.cpu is not None and self.cpu is not None:
            cpu = type(other.cpu)(
                *tuple(self.cpu[i] - other.cpu[i] for i in range(len(other.cpu)))
            )
        else:
            cpu = None
            
        if other.disk is not None and self.disk is not None:
            disk = type(other.disk)(
                *tuple(self.disk[i] - other.disk[i] for i in range(len(other.disk)))
            )
        else:
            disk = None
            
        if other.freq is not None and self.freq is not None:
            freq = type(other.freq)(
                *tuple((self.freq[i] + other.freq[i]) / 2 for i in range(len(other.freq)))
            )
        else:
            freq = None
            
        if other.net is not None and self.net is not None:
            net = type(other.net)(
                *tuple(self.net[i] - other.net[i] for i in range(len(other.net)))
            )
        else:
            net = None
            
        timestamp = self.timestamp - other.timestamp
        return CounterSnapshot(cpu, disk, freq, net, timestamp)

    def __repr__(self) -> str:
        return json.dumps(self.to_dict())
