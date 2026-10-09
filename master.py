"""PC 1 (Master/Coordinator).
Jalankan:  python master.py --worker-ip 192.168.1.20 --n 1000000
"""
import argparse
import random
import socket
import time
from array import array

from common import merge_results, process_chunk, recv_msg, send_msg


def run_sequential(data):
    t0 = time.perf_counter()
    res = process_chunk(data)
    return res, time.perf_counter() - t0


def run_distributed(data, ip, port):
    mid = len(data) // 2
    part1, part2 = data[:mid], data[mid:]

    t0 = time.perf_counter()
    with socket.create_connection((ip, port), timeout=600) as conn:
        # 1) kirim bagian 2 ke Worker
        t_send0 = time.perf_counter()
        send_msg(conn, {"cmd": "process", "count": len(part2)}, part2.tobytes())
        t_send = time.perf_counter() - t_send0

        # 2) Master menghitung bagian 1 (paralel dengan Worker)
        t_c0 = time.perf_counter()
        res1 = process_chunk(part1)
        t_comp1 = time.perf_counter() - t_c0

        # 3) terima hasil Worker
        res2, _ = recv_msg(conn)
    # 4) gabungkan
    final = merge_results(res1, res2)
    total = time.perf_counter() - t0
    return {"part1": len(part1), "part2": len(part2), "res1": res1, "res2": res2,
            "final": final, "t_send": t_send, "t_comp1": t_comp1,
            "t_comp2": res2.pop("compute_time"), "total": total}


def fmt(r):
    return (f"count={r['count']:,} sum={r['sum']:,} min={r['min']:,} "
            f"max={r['max']:,} prima={r['primes']:,}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--worker-ip", required=True)
    ap.add_argument("--port", type=int, default=5000)
    ap.add_argument("--n", type=int, default=1_000_000, help="jumlah data")
    ap.add_argument("--max", type=int, default=10_000_000, help="nilai maksimum acak")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--repeat", type=int, default=1, help="ulang percobaan, ambil rata-rata")
    args = ap.parse_args()

    print("=" * 60)
    print("DISTRIBUTED NUMBER PROCESSING (2 PC)")
    print("=" * 60)
    rng = random.Random(args.seed)
    t0 = time.perf_counter()
    data = array("q", (rng.randint(1, args.max) for _ in range(args.n)))
    print(f"[MASTER] Membangkitkan {args.n:,} data acak (1..{args.max:,}) "
          f"dalam {time.perf_counter() - t0:.2f} detik")

    seq_times, dist_times = [], []
    for i in range(args.repeat):
        print(f"\n--- Percobaan {i + 1}/{args.repeat} ---")
        seq_res, t_seq = run_sequential(data)
        print(f"[SEQUENTIAL - 1 PC] {fmt(seq_res)}  waktu={t_seq:.4f} s")

        d = run_distributed(data, args.worker_ip, args.port)
        print(f"[DISTRIBUTED] Data PC 1 = {d['part1']:,} | Data PC 2 = {d['part2']:,}")
        print(f"  Hasil PC 1 : {fmt(d['res1'])}  (hitung {d['t_comp1']:.4f} s)")
        print(f"  Hasil PC 2 : {fmt(d['res2'])}  (hitung {d['t_comp2']:.4f} s)")
        f = d["final"]
        print(f"  HASIL AKHIR: {fmt(f)} mean={f['mean']:.2f} std={f['std']:.2f}")
        print(f"  Waktu kirim data ke PC 2 : {d['t_send']:.4f} s")
        print(f"  Waktu total distributed  : {d['total']:.4f} s")
        ok = all(f[k] == seq_res[k] for k in ("count", "sum", "min", "max", "primes"))
        print(f"  Verifikasi vs sequential : {'COCOK' if ok else 'TIDAK COCOK!'}")
        seq_times.append(t_seq)
        dist_times.append(d["total"])

    ts = sum(seq_times) / len(seq_times)
    td = sum(dist_times) / len(dist_times)
    speedup = ts / td
    eff = speedup / 2 * 100
    print("\n" + "=" * 60)
    print(f"RINGKASAN  n={args.n:,}  (rata-rata {args.repeat}x)")
    print(f"  T_sequential  = {ts:.4f} s")
    print(f"  T_distributed = {td:.4f} s")
    print(f"  Speedup       = {speedup:.3f}")
    print(f"  Efficiency    = {eff:.2f} %  (P = 2)")
    print("=" * 60)


if __name__ == "__main__":
    main()
