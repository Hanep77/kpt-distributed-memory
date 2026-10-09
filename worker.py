"""PC 2 (Worker): menerima potongan data dari Master, menghitung, mengirim hasil.
Jalankan:  python worker.py --port 5000
"""
import argparse
import socket
import time
from array import array

from common import process_chunk, recv_msg, send_msg


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--port", type=int, default=5000)
    args = ap.parse_args()

    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((args.host, args.port))
    srv.listen(1)
    print(f"[WORKER] Menunggu Master di port {args.port} ...")

    while True:
        conn, addr = srv.accept()
        with conn:
            print(f"\n[WORKER] Terhubung dengan Master {addr[0]}:{addr[1]}")
            header, payload = recv_msg(conn)
            data = array("q")
            data.frombytes(payload)
            print(f"[WORKER] Menerima {len(data):,} data "
                  f"({len(payload) / 1024 / 1024:.2f} MB) ke memori lokal PC 2")

            t0 = time.perf_counter()
            result = process_chunk(data)
            t_comp = time.perf_counter() - t0
            print(f"[WORKER] Selesai menghitung dalam {t_comp:.4f} detik")
            print(f"[WORKER] Hasil PC 2: {result}")

            result["compute_time"] = t_comp
            send_msg(conn, result)
            print("[WORKER] Hasil dikirim kembali ke Master")


if __name__ == "__main__":
    main()
