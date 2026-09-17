#!/usr/bin/env python3
"""Deliberate external RAM motion: allocate 6 GB, touch it, free it, every 5 s.

The control for N1: the ledger's decision band is meant to read "something
outside this ledger was moving", so this is that something, on purpose.
"""
import ctypes, sys, time

GB = 1024 * 1024 * 1024
size = 6 * GB
deadline = time.time() + float(sys.argv[1] if len(sys.argv) > 1 else 900)
while time.time() < deadline:
    buf = bytearray(size)          # allocate
    for off in range(0, size, 4096):   # touch every page so it is resident
        buf[off] = 1
    del buf                        # free
    time.sleep(5)
