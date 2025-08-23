# -*- coding: utf-8 -*-
"""
Pairwise identity% via Bio.Align.PairwiseAligner
------------------------------------------------
- Global alignment (Needleman–Wunsch)
- BLOSUM62 substitution matrix
- Gap penalties: open=-10.0, extend=-0.5
- Identity% = matches / (aligned non-gap columns)

Inputs
------
- TXT file: one sequence per line (empty lines ignored; lines starting with '>' treated as comments)

Outputs
-------
- {matrix_out}/nearest_neighbor_per_seq.csv
- {matrix_out}/overall_histogram.csv
- {matrix_out}/top_pairs.csv
- {matrix_out}/similarity_matrix_upper.npz    (compressed upper triangle)
- {matrix_out}/summary_report.json

Notes
-----
- For N~500 this is OK. For very large N, consider MMseqs2/BLAST.
"""

from __future__ import annotations
import os
import json
import numpy as np
import pandas as pd
from typing import List, Tuple, Dict

# Progress bar (optional)
try:
    from tqdm import tqdm
except Exception:
    def tqdm(x, **kwargs): return x

# Biopython (new API)
from Bio.Align import PairwiseAligner, substitution_matrices


# ========================
#        CONFIG
# ========================
CONFIG = {
    # I/O
    "input_txt": "/Users/shulei/PycharmProjects/Dataset/scripts/test_set/matrix_out/sparse_matrix_v0.3.1_row_ids.txt",    # 每行一个序列；忽略空行与以 '>' 开头的行
    "output_dir": "./pairwise_identity_out",

    # Preprocess
    "case": "upper",                          # "upper" 或 "keep"
    "strip_whitespace": True,
    "min_len": 1,                             # 过滤过短序列

    # Academic default scoring (global alignment)
    "gap_open": -10.0,
    "gap_extend": -0.5,
    "substitution_matrix_name": "BLOSUM62",

    # Identity definition: matches / aligned non-gap columns
    "identity_mode": "matches_over_aligned_nongap",  # 预留切换接口

    # Parallel
    "n_jobs": 4,                              # 499 序列建议 4~8
    "chunk_size": 256,

    # Reporting
    "hist_bin_edges": [0.0, 0.5, 0.7, 0.85, 0.90, 0.95, 0.98, 0.99, 0.995, 1.001],
    "percentiles": [1, 5, 10, 25, 50, 75, 90, 95, 98, 99],
    "top_pairs_N": 200,
    "random_seed": 42,

    # Save matrix
    "save_matrix_npz": True,                  # 保存上三角 .npz（i, j, v, n）
}


# ========================
#     Helper functions
# ========================
VALID = set("ACDEFGHIKLMNPQRSTVWYX")  # 20 aa + X
NONSTD_MAP_TO_X = set("*BJOUZ-?")     # 常见非标准/占位/不明字符 → X

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
            # 其它奇怪字符（数字、标点等）也统一为 X
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
    # 保守设置：使路径更“常规”（可按需调整）
    aligner.internal_open_gap_score = gap_open
    aligner.internal_extend_gap_score = gap_extend
    aligner.end_open_gap_score = gap_open
    aligner.end_extend_gap_score = gap_extend
    return aligner

def identity_from_alignment(aln, mode: str = "matches_over_aligned_nongap") -> float:
    """
    使用 Bio.Align.Alignment.coordinates 计算 identity：
    identity = matches / (aligned non-gap columns)
    """
    # 取原序列字符串
    seqA, seqB = map(str, aln.sequences)

    # coordinates 形状为 (2, m)，相邻列组成路径的“拐点”
    # 对每个相邻坐标段 (i1->i2, j1->j2)：
    # - di>0,dj>0 为对角段（无 gap），需要逐位比较
    # - di>0,dj=0 为 B 中 gap（删除），不计入分母
    # - di=0,dj>0 为 A 中 gap（插入），不计入分母
    coords = aln.coordinates
    matches = 0
    nongap = 0

    for k in range(coords.shape[1] - 1):
        i1, i2 = int(coords[0, k]), int(coords[0, k + 1])
        j1, j2 = int(coords[1, k]), int(coords[1, k + 1])
        di, dj = i2 - i1, j2 - j1

        if di > 0 and dj > 0:
            # 对角连续块（可能长度 >1），逐位比较
            L = min(di, dj)
            a_seg = seqA[i1:i1 + L]
            b_seg = seqB[j1:j1 + L]
            nongap += L
            matches += sum(aa == bb for aa, bb in zip(a_seg, b_seg))
        elif di > 0 and dj == 0:
            # gap in B：不计入分母
            continue
        elif di == 0 and dj > 0:
            # gap in A：不计入分母
            continue
        else:
            # 正常不会出现 di<0 或 dj<0
            raise RuntimeError(f"Unexpected step in alignment coordinates: di={di}, dj={dj}")

    return matches / max(nongap, 1)

def align_identity(seq1: str, seq2: str, gap_open: float, gap_extend: float, matrix_name: str, mode: str) -> float:
    aligner = build_aligner(gap_open, gap_extend, matrix_name)
    # 只取一条最优比对
    aln = next(iter(aligner.align(seq1, seq2)))
    return float(identity_from_alignment(aln, mode))

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
    if n <= 1:
        return np.zeros(n, dtype=np.float32)
    A = M.copy()
    np.fill_diagonal(A, -np.inf)
    nn = A.max(axis=1)
    return nn.astype(np.float32)

def export_top_pairs(M: np.ndarray, seqs: List[str], N: int, seed: int = 42) -> pd.DataFrame:
    n = M.shape[0]
    pairs = []
    for i in range(n):
        for j in range(i+1, n):
            pairs.append((i, j, float(M[i, j])))
    rng = np.random.RandomState(seed)
    perturbed = [(i, j, s + 1e-12 * rng.rand()) for i, j, s in pairs]
    perturbed.sort(key=lambda x: x[2], reverse=True)
    top = perturbed[:N]
    rows = []
    for i, j, _ in top:
        rows.append({
            "i": i,
            "j": j,
            "identity": float(M[i, j]),
            "len_i": len(seqs[i]),
            "len_j": len(seqs[j]),
            "seq_i": seqs[i],
            "seq_j": seqs[j],
        })
    return pd.DataFrame(rows)


# ========================
#     Multiprocessing
# ========================
def _worker_init(_cfg):
    global _CFG_MP, _ALIGNER
    _CFG_MP = _cfg
    _ALIGNER = build_aligner(_cfg["gap_open"], _cfg["gap_extend"], _cfg["substitution_matrix_name"])

def _worker_align(pair):
    i, j, seqs = pair
    aln = next(iter(_ALIGNER.align(seqs[i], seqs[j])))
    s = identity_from_alignment(aln, _CFG_MP["identity_mode"])
    return i, j, float(s)

def compute_matrix_parallel(seqs: List[str], cfg: dict) -> np.ndarray:
    n = len(seqs)
    M = np.zeros((n, n), dtype=np.float32)
    np.fill_diagonal(M, 1.0)

    pairs = [(i, j, seqs) for i in range(n) for j in range(i+1, n)]
    n_jobs = max(int(cfg["n_jobs"]), 1)
    chunk = int(cfg["chunk_size"])

    if n_jobs == 1:
        # 单进程：用本地 aligner（少一次重复加载）
        aligner = build_aligner(cfg["gap_open"], cfg["gap_extend"], cfg["substitution_matrix_name"])
        for i, j, _ in tqdm(pairs, desc="Aligning (serial)"):
            aln = next(iter(aligner.align(seqs[i], seqs[j])))
            s = identity_from_alignment(aln, cfg["identity_mode"])
            M[i, j] = s
            M[j, i] = s
        return M

    import multiprocessing as mp
    with mp.get_context("spawn").Pool(
        processes=n_jobs,
        initializer=_worker_init,
        initargs=(cfg,)
    ) as pool:
        for i, j, s in tqdm(pool.imap_unordered(_worker_align, pairs, chunksize=chunk),
                            total=len(pairs), desc=f"Aligning (mp, {n_jobs} workers)"):
            M[i, j] = s
            M[j, i] = s
    return M


# ========================
#           Main
# ========================
def main():
    cfg = CONFIG
    os.makedirs(cfg["output_dir"], exist_ok=True)

    # 1) 读取 & 预处理
    raw = read_sequences(cfg["input_txt"], case=cfg["case"], strip_ws=cfg["strip_whitespace"])
    total_raw = len(raw)
    seqs0 = deduplicate_preserve_order(raw)
    removed_dups = total_raw - len(seqs0)
    report_nonstandard_counts(seqs0)
    seqs = [sanitize_seq(s) for s in seqs0]
    if cfg["min_len"] is not None:
        seqs = [s for s in seqs if len(s) >= cfg["min_len"]]
    n = len(seqs)
    print(f"Loaded sequences: raw={total_raw}, dedup={n}, removed_dups={removed_dups}")
    if n == 0:
        print("No sequences to process after filtering. Exit.")
        return

    # 2) 两两全局比对并计算 identity%
    M = compute_matrix_parallel(seqs, cfg)

    # 3) 统计输出
    stats = summarize_upper_triangle(M, cfg["percentiles"])
    print("\n=== Pairwise identity% summary (upper triangle, excl. diagonal) ===")
    for k_, v in stats.items():
        if isinstance(v, float):
            print(f"{k_:>12}: {v:.6f}")
        else:
            print(f"{k_:>12}: {v}")

    # 最近邻
    nn = nearest_neighbor_by_row(M)
    nn_df = pd.DataFrame({
        "index": np.arange(n, dtype=int),
        "length": [len(s) for s in seqs],
        "nearest_neighbor_identity": nn,
        "sequence": seqs,
    })
    nn_out = os.path.join(cfg["output_dir"], "nearest_neighbor_per_seq.csv")
    nn_df.to_csv(nn_out, index=False)
    print(f"Saved nearest-neighbor per sequence to: {nn_out}")

    # 直方图
    hist_df = make_histogram(M, cfg["hist_bin_edges"])
    hist_out = os.path.join(cfg["output_dir"], "overall_histogram.csv")
    hist_df.to_csv(hist_out, index=False)
    print(f"Saved overall histogram to: {hist_out}")

    # TopN 配对
    top_df = export_top_pairs(M, seqs, cfg["top_pairs_N"], seed=cfg["random_seed"])
    top_out = os.path.join(cfg["output_dir"], "top_pairs.csv")
    top_df.to_csv(top_out, index=False)
    print(f"Saved top-{cfg['top_pairs_N']} pairs to: {top_out}")

    # 保存矩阵（上三角）
    npz_path = None
    if cfg["save_matrix_npz"]:
        tri_i, tri_j = np.triu_indices(n, k=1)
        tri_v = M[tri_i, tri_j].astype(np.float32)
        npz_path = os.path.join(cfg["output_dir"], "similarity_matrix_upper.npz")
        np.savez_compressed(npz_path, i=tri_i.astype(np.int32), j=tri_j.astype(np.int32), v=tri_v, n=np.int32(n))
        print(f"Saved similarity matrix (upper triangle) to: {npz_path}")

    # 汇总报告
    report = {
        "config": CONFIG,
        "n_sequences_after_dedup": n,
        "stats_upper_no_diag": stats,
        "hist_bins": CONFIG["hist_bin_edges"],
        "outputs": {
            "nearest_neighbor_csv": nn_out,
            "histogram_csv": hist_out,
            "top_pairs_csv": top_out,
            "matrix_upper_npz": npz_path,
        }
    }
    rep_path = os.path.join(cfg["output_dir"], "summary_report.json")
    with open(rep_path, "w") as f:
        json.dump(report, f, indent=2)
    print(f"Saved summary report to: {rep_path}")


if __name__ == "__main__":
    main()