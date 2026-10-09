"""Modul bersama untuk master.py dan worker.py (salin ke kedua PC)."""
import json
import math
import struct


# ---------- Protokol jaringan: [4B len header][4B len payload][header JSON][payload] ----------
def recvall(conn, n):
    buf = bytearray()
    while len(buf) < n:
        chunk = conn.recv(min(n - len(buf), 1 << 20))
        if not chunk:
            raise ConnectionError("Koneksi terputus")
        buf.extend(chunk)
    return bytes(buf)


def send_msg(conn, header: dict, payload: bytes = b""):
    h = json.dumps(header).encode()
    conn.sendall(struct.pack("!II", len(h), len(payload)) + h + payload)


def recv_msg(conn):
    hl, pl = struct.unpack("!II", recvall(conn, 8))
    header = json.loads(recvall(conn, hl).decode())
    payload = recvall(conn, pl) if pl else b""
    return header, payload


# ---------- Komputasi ----------
def is_prime(n: int) -> bool:
    if n < 2:
        return False
    if n % 2 == 0:
        return n == 2
    for d in range(3, math.isqrt(n) + 1, 2):
        if n % d == 0:
            return False
    return True


def process_chunk(numbers) -> dict:
    """Hitung statistik + jumlah bilangan prima pada satu potongan data."""
    s = 0
    sq = 0
    primes = 0
    mn = None
    mx = None
    for x in numbers:
        s += x
        sq += x * x
        if mn is None or x < mn:
            mn = x
        if mx is None or x > mx:
            mx = x
        if is_prime(x):
            primes += 1
    return {"count": len(numbers), "sum": s, "sumsq": sq,
            "min": mn, "max": mx, "primes": primes}


def merge_results(a: dict, b: dict) -> dict:
    r = {
        "count": a["count"] + b["count"],
        "sum": a["sum"] + b["sum"],
        "sumsq": a["sumsq"] + b["sumsq"],
        "min": min(a["min"], b["min"]),
        "max": max(a["max"], b["max"]),
        "primes": a["primes"] + b["primes"],
    }
    r["mean"] = r["sum"] / r["count"]
    r["std"] = math.sqrt(max(r["sumsq"] / r["count"] - r["mean"] ** 2, 0))
    return r
