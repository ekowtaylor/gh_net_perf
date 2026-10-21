#!/usr/bin/env python3
"""Local post-processing for the Ghana cell_tower_info_v2 extracts.

Every warehouse query is run per day (the meta CLI has a 120s dispatch ceiling) and
written to one CSV per day. These functions pool those per-day CSVs into the
deliverables in the Drive folder.

tile_cells / merge_shards / churn_join were rebuilt from the original session after its
/tmp working directory was lost; they reproduce the logic that produced the delivered
files but were not re-run after reconstruction. pool_5g was run to produce
gh_5g_tac_28d.csv and gh_5g_cell_28d.csv.
"""

from __future__ import annotations

import ast
import csv
import glob
import math
from collections import defaultdict
from typing import Iterable, Optional

Row = dict[str, str]


def num(v: Optional[str]) -> Optional[float]:
    try:
        return float(v) if v not in ("", "NULL", None) else None
    except ValueError:
        return None


def has_header(path: str, header_prefix: str) -> bool:
    # A failed extract leaves a non-empty error file, so check the header, not the size.
    try:
        with open(path) as fh:
            return fh.readline().startswith(header_prefix)
    except OSError:
        return False


def complete_day_files(directory: str) -> list[str]:
    """Per-day CSVs, using a day's two half-day files only when BOTH succeeded.

    A lone half-day would silently count as a (smaller) full day in any pooled total.
    """
    files: list[str] = []
    for path in sorted(glob.glob(f"{directory}/*.csv")):
        name = path.rsplit("/", 1)[-1]
        if name.endswith(("_h1.csv", "_h2.csv")):
            continue
        day = name[: -len(".csv")]
        halves = [f"{directory}/{day}_h1.csv", f"{directory}/{day}_h2.csv"]
        if has_header(path, "ds,"):
            files.append(path)
        elif all(has_header(h, "ds,") for h in halves):
            files.extend(halves)
    return files


def read_rows(pattern: str | list[str], header_prefix: str) -> Iterable[Row]:
    paths = sorted(glob.glob(pattern)) if isinstance(pattern, str) else pattern
    for path in paths:
        with open(path) as fh:
            first = fh.readline()
            if not first.startswith(header_prefix):
                continue
            fh.seek(0)
            yield from csv.DictReader(fh)


def quadkey(lat: float, lon: float, z: int) -> str:
    # Web Mercator is undefined at the poles; clip, and clamp lon=180 into the last tile.
    lat = max(-85.05112878, min(85.05112878, lat))
    s = math.sin(math.radians(lat))
    n = 2**z
    x = min(max(int(((lon + 180) / 360) * n), 0), n - 1)
    y = min(max(int((0.5 - math.log((1 + s) / (1 - s)) / (4 * math.pi)) * n), 0), n - 1)
    return "".join(
        str((1 if x & (1 << (i - 1)) else 0) + (2 if y & (1 << (i - 1)) else 0))
        for i in range(z, 0, -1)
    )


def tile_cells(day_glob: str, out: str) -> None:
    """cube_cell.sql per-day output -> one row per cell, with z10/z12/z14 quadkeys.

    Sums pool exactly; percentiles become est_sessions-weighted means of daily values.
    """
    sums = ["raw_rows", "est_sessions", "pkts_w", "pkts_lost_w"]
    wavg = ["centroid_lat", "centroid_long", "lat_p10", "lat_p90", "long_p10", "long_p90",
            "p50_gps_acc_m", "p50_rtt", "p90_rtt", "p50_ttfb", "p50_ttlb", "p50_signal_dbm",
            "p50_rsrp_4g", "p50_rsrq_4g", "p50_ta_4g", "nr2_p50_ss_rsrp", "nr2_p50_ss_sinr",
            "http5xx_pct", "latlong_coverage_pct"]
    maxes = ["distinct_bands", "distinct_pcis"]
    acc: dict[tuple, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    for r in read_rows(day_glob, "op_mcc,"):
        a = acc[(r["op_mcc"], r["op_mnc"], r["ci"], r["tac"], r["generation"])]
        w = num(r["est_sessions"]) or 0.0
        for c in sums:
            a[c] += num(r[c]) or 0.0
        for c in maxes:
            a[c] = max(a[c], num(r[c]) or 0.0)
        for c in wavg:
            v = num(r[c])
            if v is not None:
                a[c] += v * w
                a["_w" + c] += w
    hdr = ["op_mcc", "op_mnc", "ci", "tac", "generation", *sums, *maxes, *wavg,
           "loss_pct", "quadkey_z10", "quadkey_z12", "quadkey_z14"]
    with open(out, "w", newline="") as fh:
        wr = csv.writer(fh)
        wr.writerow(hdr)
        for k, a in sorted(acc.items(), key=lambda kv: -kv[1]["est_sessions"]):
            mean = {c: a[c] / a["_w" + c] if a.get("_w" + c) else None for c in wavg}
            lat, lon = mean["centroid_lat"], mean["centroid_long"]
            ok = lat is not None and lon is not None and -90 <= lat <= 90 and -180 <= lon <= 180
            wr.writerow([*k, *(int(a[c]) for c in sums), *(int(a[c]) for c in maxes),
                         *("" if mean[c] is None else round(mean[c], 6) for c in wavg),
                         round(100 * a["pkts_lost_w"] / a["pkts_w"], 4) if a["pkts_w"] else "",
                         *((quadkey(lat, lon, z) for z in (10, 12, 14)) if ok else ("", "", ""))])


QUANTILES = {
    "rtt_q": ["p10", "p25", "p50", "p75", "p90", "p95", "p99"], "ttfb_q": ["p50", "p90", "p95"],
    "ttlb_q": ["p50", "p90"], "upstream_q": ["p50", "p90"], "upload_q": ["p50", "p90"],
    "rtt_var_q": ["p50", "p90"], "signal_dbm_q": ["p10", "p50", "p90"],
    "rsrp_4g_q": ["p10", "p50", "p90"], "rsrq_4g_q": ["p50"], "rssnr_4g_q": ["p50"],
    "rssi_q": ["p10", "p50", "p90"], "ta_4g_q": ["p50", "p90"], "lte_sinr_q": ["p50"],
    "ss_rsrp_5g_q": ["p50"], "ss_rsrq_5g_q": ["p50"], "ss_sinr_5g_q": ["p50"],
    "nr2_ss_rsrp_q": ["p10", "p50", "p90"], "nr2_ss_rsrq_q": ["p50"], "nr2_ss_sinr_q": ["p50"],
    "resp_bytes_q": ["p50", "p90"], "req_bytes_q": ["p50"], "resp_hdr_q": ["p50"],
    "req_hdr_q": ["p50"], "congestion_q": ["p50"],
}
SHARD_KEY = ("ds", "op_mcc", "op_mnc", "generation", "tac", "ci")


def merge_shards(shard_globs: list[str], tiled_csv: str, out: str) -> None:
    """sh_s1/s2/s3/s5 per-day outputs -> gh_cell_28d_full.csv.

    The shards share one GROUP BY, so the join is exact; identical row counts across
    shards confirm it. Location comes from gh_cell_28d_tiled.csv, matched per cell.
    """
    merged: dict[tuple, dict[str, str]] = defaultdict(dict)
    for pattern in shard_globs:
        for r in read_rows(pattern, "ds,"):
            m = merged[tuple(r[c] for c in SHARD_KEY)]
            for col, val in r.items():
                if col in SHARD_KEY or (col in ("raw_rows", "est_sessions") and col in m):
                    continue
                if col in QUANTILES:
                    try:
                        vals = ast.literal_eval(val) if val not in ("", "NULL") else []
                    except (ValueError, SyntaxError):
                        vals = []
                    for i, label in enumerate(QUANTILES[col]):
                        m[f"{label}_{col[:-2]}"] = str(vals[i]) if i < len(vals) else ""
                else:
                    m[col] = val
    loc_cols = ["centroid_lat", "centroid_long", "lat_p10", "lat_p90", "long_p10", "long_p90",
                "p50_gps_acc_m", "latlong_coverage_pct", "quadkey_z10", "quadkey_z12", "quadkey_z14"]
    loc = {(r["op_mcc"], r["op_mnc"], r["ci"], r["generation"]): r
           for r in read_rows(tiled_csv, "op_mcc,")}
    for k, m in merged.items():
        src = loc.get((k[1], k[2], k[5], k[3]), {})
        m.update({c: src.get(c, "") for c in loc_cols})
    cols = list(SHARD_KEY) + sorted({c for m in merged.values() for c in m})
    with open(out, "w", newline="") as fh:
        wr = csv.writer(fh)
        wr.writerow(cols)
        for k, m in merged.items():
            wr.writerow([*k, *(m.get(c, "") for c in cols[len(SHARD_KEY):])])


def churn_join(churn_glob: str, cell_full_csv: str, out: str) -> None:
    """churn_cell.sql per-day outputs x gh_cell_28d_full.csv on (mcc, mnc, tac, ci)."""
    churn: dict[tuple, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    for r in read_rows(churn_glob, "op_mcc,"):
        c = churn[(r["op_mcc"], r["op_mnc"], r["tac"], r["ci"])]
        for col in ("visits", "churners", "uniq_users"):
            c[col] += num(r[col]) or 0.0
    metrics = ["p50_rtt", "p90_rtt", "p99_rtt", "p50_signal_dbm", "p50_rsrp_4g", "loss_pct", "p50_ttfb"]
    q: dict[tuple, dict] = defaultdict(lambda: defaultdict(float))
    for r in read_rows(cell_full_csv, "ds,"):
        a = q[(r["op_mcc"], r["op_mnc"], r["tac"], r["ci"])]
        w = num(r["est_sessions"]) or 0.0
        a["est_sessions"] += w
        a["generation"] = r["generation"]
        for m in metrics:
            v = num(r.get(m))
            if v is not None:
                a[m] += v * w
                a["_w" + m] += w
    with open(out, "w", newline="") as fh:
        wr = csv.writer(fh)
        wr.writerow(["op_mcc", "op_mnc", "tac", "ci", "generation", "est_sessions", "visits",
                     "uniq_users", "churners", "churn_pct", *metrics])
        for k in churn.keys() & q.keys():
            c, a = churn[k], q[k]
            wr.writerow([*k, a["generation"], int(a["est_sessions"]), int(c["visits"]),
                         int(c["uniq_users"]), int(c["churners"]),
                         round(100 * c["churners"] / c["visits"], 4) if c["visits"] else "",
                         *(round(a[m] / a["_w" + m], 3) if a.get("_w" + m) else "" for m in metrics)])


G5_SUMS = ["raw_rows", "est_sessions", "matched_w", "capable_5g_w", "capable_5g_not_on_5g_w",
           "priced_w", "price_x_w", "price_lt100_w", "price_100_200_w", "price_200_400_w",
           "price_ge400_w", "aged_w", "age_x_w"]


def pool_5g(directory: str, keys: list[str], out: str) -> tuple[int, int]:
    """gh5g_{tac,cell}.sql per-day (or half-day) outputs -> one row per key, pooled exactly.

    Every input column is a sum, so full days and half-day splits add up without loss.
    Returns (rows written, complete days pooled).
    """
    files = complete_day_files(directory)
    days = {f.rsplit("/", 1)[-1][:10] for f in files}
    acc: dict[tuple, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    for r in read_rows(files, "ds,"):
        a = acc[tuple(r[c] for c in keys)]
        for c in G5_SUMS:
            a[c] += num(r[c]) or 0.0

    def pct(n: float, d: float) -> object:
        return round(100 * n / d, 3) if d else ""

    with open(out, "w", newline="") as fh:
        wr = csv.writer(fh)
        wr.writerow([*keys, *G5_SUMS, "pct_matched", "pct_5g_capable", "pct_5g_capable_not_on_5g",
                     "mean_price_eur", "pct_price_lt100", "pct_price_100_200",
                     "pct_price_200_400", "pct_price_ge400", "mean_device_age_days"])
        for k, a in sorted(acc.items(), key=lambda kv: -kv[1]["est_sessions"]):
            m = a["matched_w"]
            wr.writerow([*k, *(int(a[c]) for c in G5_SUMS),
                         pct(m, a["est_sessions"]), pct(a["capable_5g_w"], m),
                         pct(a["capable_5g_not_on_5g_w"], m),
                         round(a["price_x_w"] / a["priced_w"], 1) if a["priced_w"] else "",
                         *(pct(a[c], a["priced_w"]) for c in
                           ("price_lt100_w", "price_100_200_w", "price_200_400_w", "price_ge400_w")),
                         round(a["age_x_w"] / a["aged_w"], 0) if a["aged_w"] else ""])
    return len(acc), len(days)


if __name__ == "__main__":
    n, d = pool_5g("g5_tac", ["op_mcc", "op_mnc", "generation", "tac"], "gh_5g_tac_28d.csv")
    print(f"gh_5g_tac_28d.csv: {n:,} rows from {d} complete days")
    n, d = pool_5g("g5_cell", ["op_mcc", "op_mnc", "generation", "tac", "ci"], "gh_5g_cell_28d.csv")
    print(f"gh_5g_cell_28d.csv: {n:,} rows from {d} complete days")
