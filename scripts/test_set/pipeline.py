# -*- coding: utf-8 -*-
"""
End-to-end pipeline:
1) Read sequences (one per line .txt)
2) Compute pairwise identity% with Bio.Align.PairwiseAligner (global, BLOSUM62)
3) Save reports + similarity upper triangle .npz
4) Build test/train split from matrix (by nearest-neighbor buckets + constraints)
5) Export indices and per-sequence CSVs (train_sequences.csv, test_sequences.csv)

Fix: multiprocessing pickling on macOS/Python3.12 using top-level Pool initializer/worker.
"""

from __future__ import annotations
import os
import json
from typing import List, Tuple, Dict
import random
import numpy as np
import pandas as pd

# ---- progress bar (optional) ----
try:
    from tqdm import tqdm
except Exception:
    def tqdm(x, **kwargs): return x

# ---- Bio.Align ----
from Bio.Align import PairwiseAligner, substitution_matrices


# ========================
#        CONFIG
# ========================
CONFIG = {
    # I/O
    "input_txt": "/Users/shulei/PycharmProjects/Dataset/scripts/test_set/matrix_out/sparse_matrix_v0.3.1_row_ids.txt",  # 每行一个序列（忽略空行与以 '>' 开头的行）
    "output_dir": "run3/pipeline_out",

    # Preprocess
    "case": "upper",             # "upper" 或 "keep"
    "strip_whitespace": True,
    "min_len": 1,

    # Alignment (academic defaults)
    "gap_open": -10.0,
    "gap_extend": -0.5,
    "substitution_matrix_name": "BLOSUM62",
    # Identity definition
    "identity_mode": "matches_over_aligned_nongap",

    # Parallel
    "n_jobs": 4,                 # macOS/py3.12 OK (spawn)
    "chunk_size": 256,

    # Reporting
    "hist_bin_edges": [0.0, 0.5, 0.7, 0.85, 0.90, 0.95, 0.98, 0.99, 0.995, 1.001],
    "percentiles": [1, 5, 10, 25, 50, 75, 90, 95, 98, 99],
    "top_pairs_N": 200,
    "random_seed": 41,

    # Split (buckets by nearest-neighbor to ALL)
    "TH_HIGH": 0.95,             # Easy:   nn_total >= 0.95
    "TH_LOW": 0.75,              # Hard:   nn_total <  0.75  (Medium = [0.75, 0.95))
    "TARGET_EASY": 30,
    "TARGET_MED": 30,
    "TARGET_HARD": 30,

    # Constraints
    "ENFORCE_NEAREST_STAYS_IN_TRAIN": True,  # 测试样本的最近邻必须留在训练集
    "TEST_INTERNAL_MAX": 0.99,               # 任意两测试样本相似度 <= 该阈值；None 关闭

    # Selection relaxation
    "TOLERANCE_STEP": 0.01,
    "TOLERANCE_MAX": 0.10,
}


# ========================
#     Helper functions
# ========================
VALID = set("ACDEFGHIKLMNPQRSTVWYX")  # 20 aa + X
NONSTD_MAP_TO_X = set("*BJOUZ-?")

def read_sequences(path: str, case: str = "upper", strip_ws: bool = True) -> List[str]:
    seqs: List[str] = []
    with open(path, "r") as f:
        for line in f:
            s = line.rstrip("\n")
            if strip_ws:
                s = "".join(s.split())
            if not s or s.startswith(">"):
                continue
            if case == "upper":
                s = s.upper()
            seqs.append(s)
    return seqs

def deduplicate_preserve_order(items: List[str]) -> List[str]:
    seen = set()
    out = []
    for x in items:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out

def sanitize_seq(s: str) -> str:
    s = s.upper().replace(" ", "")
    out = []
    for ch in s:
        if ch in VALID:
            out.append(ch)
        elif ch in NONSTD_MAP_TO_X or ("A" <= ch <= "Z" and ch not in VALID):
            out.append("X")
        else:
            out.append("X")
    return "".join(out)

def report_nonstandard_counts(raw_seqs: List[str]) -> Dict[str, int]:
    from collections import Counter
    cnt = Counter()
    for s in raw_seqs:
        for ch in s.upper():
            if ch not in VALID and ch != " ":
                cnt[ch] += 1
    if cnt:
        print("[Warn] Non-standard residues found. They will be mapped to 'X':")
        print(dict(cnt))
    return dict(cnt)

def build_aligner(gap_open: float, gap_extend: float, matrix_name: str) -> PairwiseAligner:
    aligner = PairwiseAligner()
    aligner.mode = "global"
    aligner.substitution_matrix = substitution_matrices.load(matrix_name)
    aligner.open_gap_score = gap_open
    aligner.extend_gap_score = gap_extend
    aligner.internal_open_gap_score = gap_open
    aligner.internal_extend_gap_score = gap_extend
    aligner.end_open_gap_score = gap_open
    aligner.end_extend_gap_score = gap_extend
    return aligner

def identity_from_alignment(aln, mode: str = "matches_over_aligned_nongap") -> float:
    # 使用 Alignment.coordinates 计算：matches / (aligned non-gap columns)
    seqA, seqB = map(str, aln.sequences)
    coords = aln.coordinates
    matches = 0
    nongap = 0
    for k in range(coords.shape[1] - 1):
        i1, i2 = int(coords[0, k]), int(coords[0, k + 1])
        j1, j2 = int(coords[1, k]), int(coords[1, k + 1])
        di, dj = i2 - i1, j2 - j1
        if di > 0 and dj > 0:
            L = min(di, dj)
            a_seg = seqA[i1:i1 + L]
            b_seg = seqB[j1:j1 + L]
            nongap += L
            matches += sum(aa == bb for aa, bb in zip(a_seg, b_seg))
        # gaps 不计入分母
    return matches / max(nongap, 1)


# =====================================================
#  Multiprocessing globals & workers  (TOP-LEVEL!)
# =====================================================
_ALIGNER = None
_IDMODE = None
_SEQS = None

def _mp_init(cfg, seqs):
    """Pool initializer (runs in child process)."""
    from Bio.Align import PairwiseAligner, substitution_matrices
    global _ALIGNER, _IDMODE, _SEQS
    _ALIGNER = PairwiseAligner()
    _ALIGNER.mode = "global"
    _ALIGNER.substitution_matrix = substitution_matrices.load(cfg["substitution_matrix_name"])
    _ALIGNER.open_gap_score = cfg["gap_open"]
    _ALIGNER.extend_gap_score = cfg["gap_extend"]
    _ALIGNER.internal_open_gap_score = cfg["gap_open"]
    _ALIGNER.internal_extend_gap_score = cfg["gap_extend"]
    _ALIGNER.end_open_gap_score = cfg["gap_open"]
    _ALIGNER.end_extend_gap_score = cfg["gap_extend"]
    _IDMODE = cfg["identity_mode"]
    _SEQS = seqs

def _mp_worker(pair):
    """Actual worker (TOP-LEVEL so it's picklable)."""
    i, j = pair
    aln = next(iter(_ALIGNER.align(_SEQS[i], _SEQS[j])))
    s = identity_from_alignment(aln, _IDMODE)
    return i, j, float(s)


# ========================
#   Compute matrix
# ========================
def compute_matrix_parallel(seqs: List[str], cfg: dict) -> np.ndarray:
    n = len(seqs)
    M = np.zeros((n, n), dtype=np.float32)
    np.fill_diagonal(M, 1.0)

    pairs = [(i, j) for i in range(n) for j in range(i + 1, n)]
    n_jobs = max(int(cfg["n_jobs"]), 1)
    chunk = int(cfg["chunk_size"])

    if n_jobs == 1:
        aligner = build_aligner(cfg["gap_open"], cfg["gap_extend"], cfg["substitution_matrix_name"])
        for i, j in tqdm(pairs, desc="Aligning (serial)"):
            aln = next(iter(aligner.align(seqs[i], seqs[j])))
            s = identity_from_alignment(aln, cfg["identity_mode"])
            M[i, j] = s
            M[j, i] = s
        return M

    import multiprocessing as mp
    ctx = mp.get_context("spawn")
    with ctx.Pool(processes=n_jobs, initializer=_mp_init, initargs=(cfg, seqs)) as pool:
        for i, j, s in tqdm(pool.imap_unordered(_mp_worker, pairs, chunksize=chunk),
                            total=len(pairs), desc=f"Aligning (mp, {n_jobs} workers)"):
            M[i, j] = s
            M[j, i] = s
    return M


# ========================
#     Reporting utils
# ========================
def summarize_upper_triangle(M: np.ndarray, percentiles: List[int]) -> Dict[str, float]:
    n = M.shape[0]
    vals = M[np.triu_indices(n, k=1)].astype(np.float64)
    stats = {
        "count_pairs": int(vals.size),
        "mean": float(vals.mean()) if vals.size else float("nan"),
        "median": float(np.median(vals)) if vals.size else float("nan"),
        "std": float(vals.std()) if vals.size else float("nan"),
        "min": float(vals.min()) if vals.size else float("nan"),
        "max": float(vals.max()) if vals.size else float("nan"),
    }
    for p in percentiles:
        stats[f"p{p}"] = float(np.percentile(vals, p)) if vals.size else float("nan")
    return stats

def make_histogram(M: np.ndarray, bin_edges: List[float]) -> pd.DataFrame:
    n = M.shape[0]
    vals = M[np.triu_indices(n, k=1)].astype(np.float64)
    counts, edges = np.histogram(vals, bins=np.array(bin_edges, dtype=np.float64))
    rows = []
    for i in range(len(counts)):
        lo, hi = edges[i], edges[i+1]
        label = f"[{lo:.3f},{hi:.3f})" if i < len(counts)-1 else f"[{lo:.3f},{hi:.3f}]"
        rows.append({"bin": label, "count": int(counts[i])})
    return pd.DataFrame(rows)

def nearest_neighbor_by_row(M: np.ndarray) -> np.ndarray:
    n = M.shape[0]
    if n <= 1: return np.zeros(n, dtype=np.float32)
    A = M.copy()
    np.fill_diagonal(A, -np.inf)
    return A.max(axis=1).astype(np.float32)

def export_top_pairs(M: np.ndarray, seqs: List[str], N: int, seed: int = 42) -> pd.DataFrame:
    n = M.shape[0]
    pairs = [(i, j, float(M[i, j])) for i in range(n) for j in range(i+1, n)]
    rng = np.random.RandomState(seed)
    pairs = [(i, j, s + 1e-12 * rng.rand()) for i, j, s in pairs]
    pairs.sort(key=lambda x: x[2], reverse=True)
    top = pairs[:N]
    rows = []
    for i, j, _ in top:
        rows.append({
            "i": i, "j": j, "identity": float(M[i, j]),
            "len_i": len(seqs[i]), "len_j": len(seqs[j]),
            "seq_i": seqs[i], "seq_j": seqs[j],
        })
    return pd.DataFrame(rows)


# ========================
#   Split from matrix
# ========================
def nearest_neighbor_all(M: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
    A = M.copy()
    np.fill_diagonal(A, -np.inf)
    nn_arg = np.argmax(A, axis=1).astype(np.int32)
    nn_val = A[np.arange(A.shape[0]), nn_arg].astype(np.float32)
    return nn_val, nn_arg

def bucket_of_val(x: float, th_low: float, th_high: float) -> str:
    if x >= th_high: return "Easy"
    if x < th_low:   return "Hard"
    return "Medium"

def in_bucket(val: float, bucket: str, th_low: float, th_high: float,
              tol_low: float = 0.0, tol_high: float = 0.0) -> bool:
    if bucket == "Easy":
        return val >= (th_high - tol_high)
    elif bucket == "Hard":
        return val < (th_low + tol_low)
    else:
        return (th_low - tol_low) <= val < (th_high + tol_high)

def summarize_matrix(M: np.ndarray) -> Dict[str, float]:
    n = M.shape[0]
    tri = M[np.triu_indices(n, k=1)].astype(np.float64)
    out = {
        "n": int(n),
        "pairs": int(tri.size),
        "mean": float(np.mean(tri)) if tri.size else float("nan"),
        "median": float(np.median(tri)) if tri.size else float("nan"),
        "min": float(np.min(tri)) if tri.size else float("nan"),
        "max": float(np.max(tri)) if tri.size else float("nan"),
        "p5": float(np.percentile(tri, 5)) if tri.size else float("nan"),
        "p25": float(np.percentile(tri, 25)) if tri.size else float("nan"),
        "p50": float(np.percentile(tri, 50)) if tri.size else float("nan"),
        "p75": float(np.percentile(tri, 75)) if tri.size else float("nan"),
        "p95": float(np.percentile(tri, 95)) if tri.size else float("nan"),
    }
    return out

def fill_bucket(
    bucket_name: str,
    target_k: int,
    candidates: List[int],
    nn_total: np.ndarray,
    nn_arg_total: np.ndarray,
    M: np.ndarray,
    th_low: float,
    th_high: float,
    enforce_nn_train: bool,
    internal_cap: float | None,
    tol_step: float,
    tol_max: float,
    rng: random.Random,
    preselected_test_set: set,
) -> Tuple[List[int], Dict]:
    selected: List[int] = []
    selected_set: set = set()

    def internal_ok(i: int) -> bool:
        if internal_cap is None: return True
        if len(selected) > 0 and float(np.max(M[i, selected])) > internal_cap:
            return False
        if len(preselected_test_set) > 0 and float(np.max(M[i, list(preselected_test_set)])) > internal_cap:
            return False
        return True

    def score_for_bucket(x: float) -> float:
        if bucket_name == "Medium":
            mid = 0.5 * (th_low + th_high)
            return -abs(x - mid)
        elif bucket_name == "Easy":
            return x
        else:
            return -x

    cand = list(candidates)
    rng.shuffle(cand)
    cand.sort(key=lambda idx: score_for_bucket(float(nn_total[idx])), reverse=True)

    tol = 0.0
    debug = {"bucket": bucket_name, "target": target_k, "initial_candidates": len(cand), "tolerance_steps": []}

    while len(selected) < target_k and tol <= tol_max:
        picked_this_round = 0
        for i in cand:
            if i in selected_set:
                continue
            val = float(nn_total[i])
            if not in_bucket(val, bucket_name, th_low, th_high, tol, tol):
                continue
            if enforce_nn_train:
                j_star = int(nn_arg_total[i])
                if (j_star in selected_set) or (j_star in preselected_test_set):
                    continue
            if not internal_ok(i):
                continue
            selected.append(i)
            selected_set.add(i)
            picked_this_round += 1
            if len(selected) >= target_k:
                break
        debug["tolerance_steps"].append({"tol": tol, "picked": picked_this_round})
        if len(selected) >= target_k: break
        tol = round(tol + tol_step, 6)

    return selected, debug


# ========================
#           Main
# ========================
def main():
    cfg = CONFIG
    os.makedirs(cfg["output_dir"], exist_ok=True)

    # ---- 1) Load & preprocess ----
    raw = read_sequences(cfg["input_txt"], case=cfg["case"], strip_ws=cfg["strip_whitespace"])
    total_raw = len(raw)
    seqs0 = deduplicate_preserve_order(raw)
    removed_dups = total_raw - len(seqs0)
    report_nonstandard_counts(seqs0)
    seqs = [sanitize_seq(s) for s in seqs0]
    if cfg["min_len"] is not None:
        seqs = [s for s in seqs if len(s) >= cfg["min_len"]]
    n = len(seqs)
    print(f"[Load] raw={total_raw}, dedup_filtered={n}, removed_dups={removed_dups}")
    if n == 0:
        print("No sequences left after filtering. Exit.")
        return

    # ---- 2) Compute pairwise matrix ----
    M = compute_matrix_parallel(seqs, cfg)

    # ---- 3) Save analysis artifacts ----
    # 3.1 summary
    stats = summarize_upper_triangle(M, cfg["percentiles"])
    with open(os.path.join(cfg["output_dir"], "summary_report.json"), "w") as f:
        json.dump({
            "config": cfg,
            "n_sequences_after_dedup": n,
            "stats_upper_no_diag": stats,
            "hist_bins": cfg["hist_bin_edges"],
        }, f, indent=2)

    # 3.2 nearest-neighbor per seq
    nn_all = nearest_neighbor_by_row(M)
    pd.DataFrame({
        "index": np.arange(n, dtype=int),
        "length": [len(s) for s in seqs],
        "nearest_neighbor_identity": nn_all,
        "sequence": seqs,
    }).to_csv(os.path.join(cfg["output_dir"], "nearest_neighbor_per_seq.csv"), index=False)

    # 3.3 histogram
    make_histogram(M, cfg["hist_bin_edges"]).to_csv(
        os.path.join(cfg["output_dir"], "overall_histogram.csv"), index=False
    )

    # 3.4 top pairs
    export_top_pairs(M, seqs, cfg["top_pairs_N"], seed=cfg["random_seed"]).to_csv(
        os.path.join(cfg["output_dir"], "top_pairs.csv"), index=False
    )

    # 3.5 upper triangle npz
    tri_i, tri_j = np.triu_indices(n, k=1)
    tri_v = M[tri_i, tri_j].astype(np.float32)
    np.savez_compressed(
        os.path.join(cfg["output_dir"], "similarity_matrix_upper.npz"),
        i=tri_i.astype(np.int32), j=tri_j.astype(np.int32),
        v=tri_v, n=np.int32(n)
    )

    # ---- 4) Build split (by nearest-neighbor to ALL) ----
    th_low, th_high = float(cfg["TH_LOW"]), float(cfg["TH_HIGH"])
    nn_total, nn_arg_total = nearest_neighbor_all(M)

    buckets = {"Easy": [], "Medium": [], "Hard": []}
    for idx in range(n):
        buckets[bucket_of_val(float(nn_total[idx]), th_low, th_high)].append(idx)

    rng = random.Random(cfg["random_seed"])
    test_set: List[int] = []

    def pick(name: str, target: int) -> Tuple[List[int], Dict]:
        chosen, dbg = fill_bucket(
            bucket_name=name,
            target_k=int(target),
            candidates=buckets[name],
            nn_total=nn_total,
            nn_arg_total=nn_arg_total,
            M=M,
            th_low=th_low,
            th_high=th_high,
            enforce_nn_train=bool(cfg["ENFORCE_NEAREST_STAYS_IN_TRAIN"]),
            internal_cap=cfg["TEST_INTERNAL_MAX"],
            tol_step=float(cfg["TOLERANCE_STEP"]),
            tol_max=float(cfg["TOLERANCE_MAX"]),
            rng=rng,
            preselected_test_set=set(test_set),
        )
        # 复核（跨桶冲突）
        final = []
        for i in chosen:
            if cfg["ENFORCE_NEAREST_STAYS_IN_TRAIN"]:
                j_star = int(nn_arg_total[i])
                if (j_star in test_set) or (j_star in final):
                    continue
            if cfg["TEST_INTERNAL_MAX"] is not None:
                if (len(test_set) > 0 and float(np.max(M[i, test_set])) > cfg["TEST_INTERNAL_MAX"]) or \
                   (len(final) > 0 and float(np.max(M[i, final])) > cfg["TEST_INTERNAL_MAX"]):
                    continue
            final.append(i)
        return final, dbg

    easy_sel, dbg_e = pick("Easy", cfg["TARGET_EASY"]);  test_set += easy_sel
    med_sel,  dbg_m = pick("Medium", cfg["TARGET_MED"]); test_set += med_sel
    hard_sel, dbg_h = pick("Hard", cfg["TARGET_HARD"]);  test_set += hard_sel
    test_set = list(dict.fromkeys(test_set))
    train_set = [i for i in range(n) if i not in set(test_set)]

    # 复核明细
    rows = []
    test_set_set = set(test_set)
    for i_idx in test_set:
        sims = M[i_idx].copy()
        sims[list(test_set_set)] = -np.inf
        sims[i_idx] = -np.inf
        j = int(np.argmax(sims)); v = float(sims[j])
        rows.append({
            "index": int(i_idx),
            "bucket_by_nn_all": bucket_of_val(float(nn_total[i_idx]), th_low, th_high),
            "nn_all_identity": float(nn_total[i_idx]),
            "nn_all_index": int(nn_arg_total[i_idx]),
            "nn_train_identity": v,
            "nn_train_index": j,
            "sequence": seqs[i_idx],
        })
    df_val = pd.DataFrame(rows).sort_values(["bucket_by_nn_all", "nn_all_identity"], ascending=[True, False])

    # ---- 5) Save split artifacts & per-seq CSVs ----
    out_dir = cfg["output_dir"]
    with open(os.path.join(out_dir, "test_indices.txt"), "w") as f:
        f.write("\n".join(str(x) for x in test_set))
    with open(os.path.join(out_dir, "train_indices.txt"), "w") as f:
        f.write("\n".join(str(x) for x in train_set))

    df_val.to_csv(os.path.join(out_dir, "testset_per_seq_validation.csv"), index=False)

    with open(os.path.join(out_dir, "bucket_summary.json"), "w") as f:
        json.dump({
            "buckets_count": {k: len(v) for k, v in buckets.items()},
            "selected": {"Easy": len(easy_sel), "Medium": len(med_sel), "Hard": len(hard_sel), "Total": len(test_set)},
            "config": cfg,
        }, f, indent=2)

    with open(os.path.join(out_dir, "matrix_stats.json"), "w") as f:
        json.dump(summarize_matrix(M), f, indent=2)

    # 导出 train/test 的序列 CSV（便于下游直接用）
    pd.DataFrame({"index": train_set, "sequence": [seqs[i] for i in train_set]}).to_csv(
        os.path.join(out_dir, "train_sequences.csv"), index=False
    )
    pd.DataFrame({"index": test_set, "sequence": [seqs[i] for i in test_set]}).to_csv(
        os.path.join(out_dir, "test_sequences.csv"), index=False
    )

    print(f"[Done] test={len(test_set)}  train={len(train_set)}")
    print(f" - Saved: train_sequences.csv, test_sequences.csv")
    print(f" - Saved: test_indices.txt, train_indices.txt")
    print(f" - Saved: testset_per_seq_validation.csv, bucket_summary.json, matrix_stats.json")
    print(f" - Saved: nearest_neighbor_per_seq.csv, overall_histogram.csv, top_pairs.csv, similarity_matrix_upper.npz, summary_report.json")


if __name__ == "__main__":
    main()