import os
import hashlib
from typing import Tuple

# --- tweakables -------------------------------------------------------------
PORT_BASE  = 55_000        # first port in your private block
PORT_SPAN  = 1_000         # how many distinct ports you want to reserve
OCTET_MIN  = 2             # start at 127.2.x.x (skip .0 and .1 for clarity)
OCTET_MAX  = 254           # avoid .255 broadcasts
# ---------------------------------------------------------------------------

def endpoint_for(name: str, seed: str) -> Tuple[str, int]:
    """
    Deterministically map (name, seed) ➜ (ip, port).
    Seed lets multiple app instances coexist without clashes.
    """
    # 1. Hash the composite key
    digest = hashlib.sha256(f"{seed}:{name}".encode()).digest()
    v      = int.from_bytes(digest, "big")        # 256-bit integer

    # 2. Carve bits out of the hash -------------------------
    #   •  6 bits → second octet (2–63)   – plenty for 60+ parallel instances
    #   • 16 bits → last two octets       – 65 536 actor slots per instance
    #   • 10 bits → port offset (0–1023)  – mapped into your private block
    octet2 = OCTET_MIN + (v        & 0x3F) % (OCTET_MAX - OCTET_MIN + 1)
    octet3 = (v >>  6) & 0xFF
    octet4 = (v >> 14) & 0xFF
    port   = PORT_BASE + ((v >> 22) & (PORT_SPAN - 1))

    ip = f"127.{octet2}.{octet3}.{octet4}"
    return ip, port

def gendpoint(name: str) -> str:
    """
    Generate a deterministic endpoint for a given actor name.
    The endpoint is based on the actor name and a seed (instance ID).
    The seed allows multiple instances of the app to run without port conflicts.
    """
    seed = os.getenv("PANOPTIKON_INSTANCE_ID", "default")
    ip, port = endpoint_for(name, seed)
    return f"{ip}:{port}"
