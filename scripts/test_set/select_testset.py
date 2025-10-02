# -*- coding: utf-8 -*-
"""
Build test/train split from similarity_matrix_upper.npz
-------------------------------------------------------
- Input:  similarity_matrix_upper.npz  (keys: i, j, v, n)
          where (i[k], j[k]) is an upper-triangle pair and v[k] is the identity in [0,1]
- Reconstruct full symmetric matrix M (float32), diag=1.0
- Bucket by each sequence's nearest-neighbor identity to the WHOLE set (excl. self):
    Easy   : nn_total >= TH_HIGH
    Medium : TH_LOW <= nn_total < TH_HIGH
    Hard   : nn_total < TH_LOW
- Greedy selection with constraints:
    (C1) The selected sample's nearest neighbor must remain in TRAIN (i.e. not selected into test)
    (C2) (optional) Max similarity between any two TEST samples <= TEST_INTERNAL_MAX
- If a bucket underfills, expand tolerance around its boundary by step (e.g. 0.01, 0.02, ...)

Outputs
-------
- {matrix_out}/test_indices.txt
- {matrix_out}/train_indices.txt
- {matrix_out}/testset_per_seq_validation.csv
- {matrix_out}/bucket_summary.json
- {matrix_out}/matrix_stats.json
"""

from __future__ import annotations
import os
import json
import math
import random
from typing import Dict, List, Tuple
import numpy as np
import pandas as pd


# ========================
#          CONFIG
# ========================
CONFIG = {
    # --- I/O ---
    "npz_path": "/Users/shulei/PycharmProjects/Dataset/scripts/test_set/pairwise_identity_out/similarity_matrix_upper.npz",
    "output_dir": "./testset_from_npz_out",

    # --- Bucketing by nearest-neighbor to ALL samples (excl. self) ---
    # Easy boundary (>=), Hard boundary (<). Medium is the band between them.
    "TH_HIGH": 0.95,   # Easy:   nn_total >= 0.95
    "TH_LOW":  0.75,   # Hard:    nn_total <  0.75
                        # Medium:  0.75 <= nn_total < 0.95

    # --- Target sizes ---
    "TARGET_EASY": 30,
    "TARGET_MED" : 30,
    "TARGET_HARD": 30,

    # --- Constraints ---
    "ENFORCE_NEAREST_STAYS_IN_TRAIN": True,   # (C1)
    "TEST_INTERNAL_MAX": 0.99,                # (C2) None to disable; else any two in TEST must have sim <= this
                                              # 建议 0.99 或 0.98；若你允许极高相似的重复进测试，设为 None

    # --- Selection strategy ---
    "RANDOM_SEED": 42,        # 随机打散候选顺序，保证可复现
    "TOLERANCE_STEP": 0.01,   # 每次放宽边界 0.01
    "TOLERANCE_MAX": 0.10,    # 最多放宽到 0.10（即边界 ±0.10）
}


# ========================
#       Core utils
# ========================
def load_upper_npz(path: str) -> Tuple[np.ndarray, np.ndarray, np.ndarray, int]:
    data = np.load(path)
    i = data["i"].astype(np.int64)
    j = data["j"].astype(np.int64)
    v = data["v"].astype(np.float32)
    n = int(data["n"])
    return i, j, v, n

def reconstruct_dense(i: np.ndarray, j: np.ndarray, v: np.ndarray, n: int) -> np.ndarray:
    M = np.zeros((n, n), dtype=np.float32)
    M[i, j] = v
    M[j, i] = v
    np.fill_diagonal(M, 1.0)
    return M

def nearest_neighbor_all(M: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
    """Return (nn_val, nn_arg) for each row vs ALL (excluding self)."""
    A = M.copy()
    np.fill_diagonal(A, -np.inf)
    nn_arg = np.argmax(A, axis=1).astype(np.int32)
    nn_val = A[np.arange(A.shape[0]), nn_arg].astype(np.float32)
    return nn_val, nn_arg

def bucket_of_val(x: float, th_low: float, th_high: float) -> str:
    if x >= th_high:
        return "Easy"
    elif x < th_low:
        return "Hard"
    else:
        return "Medium"

def in_bucket(val: float, bucket: str, th_low: float, th_high: float,
              tol_low: float = 0.0, tol_high: float = 0.0) -> bool:
    """Check if val falls in the (possibly expanded) bucket range."""
    lo = th_low
    hi = th_high
    if bucket == "Easy":
        return val >= (hi - tol_high)
    elif bucket == "Hard":
        return val < (lo + tol_low)
    else:  # Medium
        return (lo - tol_low) <= val < (hi + tol_high)

def validate_internal_cap(idx_list: List[int], new_idx: int, M: np.ndarray, cap: float) -> bool:
    if cap is None or len(idx_list) == 0:
        return True
    sims = M[new_idx, idx_list]
    return float(np.max(sims)) <= cap

def nearest_to_train(i: int, test_set: set, M: np.ndarray) -> Tuple[int, float]:
    """Nearest neighbor of i restricted to TRAIN (= all \ TEST)."""
    n = M.shape[0]
    # candidates: all indices except i and except current test_set
    # we can compute by masking sims to test_set with -inf
    sims = M[i].copy()
    sims[list(test_set)] = -np.inf
    sims[i] = -np.inf
    j = int(np.argmax(sims))
    v = float(sims[j]) if j >= 0 else float("nan")
    return j, v

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


# ========================
#   Greedy bucket fill
# ========================
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
    internal_cap: float,
    tol_step: float,
    tol_max: float,
    rng: random.Random,
) -> Tuple[List[int], Dict]:
    """
    Bucket-wise greedy selection with constraints and gradual tolerance expansion.
    Returns (selected_indices, debug_info)
    """
    selected: List[int] = []
    selected_set: set = set()
    test_set_global: set = set()  # grows as we select across buckets at a higher level; Placeholder, replaced outside

    # 我们只在本函数内判断与“当前即将加入的测试样本集合 selected”之间的内部上限；
    # 跨桶的内部限制由外层驱动（本函数外部会把已有 test_set 传入）。为保持简单，这里让外层传入：
    # 我们改成参数：preselected_test_set，避免跨桶冲突。
    # 为了不改变签名，我们将在上层调用时闭包（参见 builder 中的局部函数）

    # 真实实现会在闭包里替换这两个符号
    preselected_test_set: set = set()

    # ---- 通过闭包注入外层 test_set ----
    def set_preselected(s: set):
        nonlocal preselected_test_set
        preselected_test_set = s

    def internal_ok(i: int) -> bool:
        # 与已经选入的（当前桶）+ 预先存在的（其他桶）均需满足 internal_cap
        if internal_cap is None:
            return True
        if len(selected) > 0:
            if float(np.max(M[i, selected])) > internal_cap:
                return False
        if len(preselected_test_set) > 0:
            if float(np.max(M[i, list(preselected_test_set)])) > internal_cap:
                return False
        return True

    # 候选顺序：随机打散 + 轻微“中心优先”（Medium 中靠近中间；Easy 靠近高端；Hard 靠近低端）
    def score_for_bucket(x: float) -> float:
        if bucket_name == "Medium":
            # 越靠近中点越优
            mid = 0.5 * (th_low + th_high)
            return -abs(x - mid)
        elif bucket_name == "Easy":
            return x  # 越高越优
        else:
            return -x  # 越低越优（Hard）

    cand = list(candidates)
    rng.shuffle(cand)
    cand.sort(key=lambda idx: score_for_bucket(float(nn_total[idx])), reverse=True)

    # 逐步放宽
    tol = 0.0
    debug = {
        "bucket": bucket_name,
        "target": target_k,
        "initial_candidates": len(cand),
        "tolerance_steps": [],
    }

    while len(selected) < target_k and tol <= tol_max:
        picked_this_round = 0
        for i in cand:
            if i in selected_set:
                continue
            val = float(nn_total[i])
            if not in_bucket(val, bucket_name, th_low, th_high, tol, tol):
                continue

            # (C1) 最近邻必须留在训练集中
            if enforce_nn_train:
                j_star = int(nn_arg_total[i])
                # 该最近邻不可以已经在 test（本桶已选 + 其他桶已选）
                if (j_star in selected_set) or (j_star in preselected_test_set):
                    continue

            # (C2) 测试集内部相似度上限
            if not internal_ok(i):
                continue

            selected.append(i)
            selected_set.add(i)
            picked_this_round += 1
            if len(selected) >= target_k:
                break

        debug["tolerance_steps"].append({"tol": tol, "picked": picked_this_round})
        if len(selected) >= target_k:
            break
        tol = round(tol + tol_step, 6)  # 控制累积误差

    return selected, debug, set_preselected


# ========================
#       Builder
# ========================
def build_testset_from_npz(cfg: Dict):
    os.makedirs(cfg["output_dir"], exist_ok=True)

    # 1) 读取与重建
    i, j, v, n = load_upper_npz(cfg["npz_path"])
    M = reconstruct_dense(i, j, v, n)

    # 2) 全局最近邻（对全集）
    nn_total, nn_arg_total = nearest_neighbor_all(M)

    # 3) 分桶候选
    th_low  = float(cfg["TH_LOW"])
    th_high = float(cfg["TH_HIGH"])

    buckets = {"Easy": [], "Medium": [], "Hard": []}
    for idx in range(n):
        b = bucket_of_val(float(nn_total[idx]), th_low, th_high)
        buckets[b].append(idx)

    # 4) 逐桶受约束抽样（跨桶共享 test_set 以检查内部上限/近邻留训）
    rng = random.Random(cfg["RANDOM_SEED"])
    test_set: List[int] = []

    def select_for_bucket(name: str, target_k: int) -> Tuple[List[int], Dict]:
        cand = buckets[name]
        chosen, dbg, set_hook = fill_bucket(
            bucket_name=name,
            target_k=target_k,
            candidates=cand,
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
        )
        # 把外层已选 test_set 注入，用于内部相似度限制与“近邻留训”检查
        set_hook(set(test_set))
        # 重新跑一遍（带外部约束的最终检查）
        # 为了简单，这里直接过滤一次（通常 chosen 已满足；若有被过滤，稍有欠额也可接受）
        final = []
        for i in chosen:
            # (C1)
            if cfg["ENFORCE_NEAREST_STAYS_IN_TRAIN"]:
                j_star = int(nn_arg_total[i])
                if (j_star in test_set) or (j_star in final):
                    continue
            # (C2)
            if cfg["TEST_INTERNAL_MAX"] is not None:
                if (len(test_set) > 0 and float(np.max(M[i, test_set])) > cfg["TEST_INTERNAL_MAX"]) or \
                   (len(final) > 0 and float(np.max(M[i, final])) > cfg["TEST_INTERNAL_MAX"]):
                    continue
            final.append(i)

        return final, dbg

    easy_sel,  dbg_e = select_for_bucket("Easy",   int(cfg["TARGET_EASY"]))
    test_set.extend(easy_sel)

    med_sel,   dbg_m = select_for_bucket("Medium", int(cfg["TARGET_MED"]))
    test_set.extend(med_sel)

    hard_sel,  dbg_h = select_for_bucket("Hard",   int(cfg["TARGET_HARD"]))
    test_set.extend(hard_sel)

    test_set = list(dict.fromkeys(test_set))  # 去重（理论上无重复）

    # 5) 训练集 = 其余
    all_idx = np.arange(n, dtype=int).tolist()
    train_set = [x for x in all_idx if x not in set(test_set)]

    # 6) 复核：每个测试样本对训练集最近邻
    rows = []
    test_set_set = set(test_set)
    for i_idx in test_set:
        j_train, s_train = nearest_to_train(i_idx, test_set_set, M)
        rows.append({
            "index": int(i_idx),
            "bucket_by_nn_all": bucket_of_val(float(nn_total[i_idx]), th_low, th_high),
            "nn_all_identity": float(nn_total[i_idx]),
            "nn_all_index": int(nn_arg_total[i_idx]),
            "nn_train_identity": float(s_train),
            "nn_train_index": int(j_train),
        })
    df_val = pd.DataFrame(rows).sort_values(["bucket_by_nn_all", "nn_all_identity"], ascending=[True, False])

    # 7) 导出
    out_dir = cfg["output_dir"]
    with open(os.path.join(out_dir, "test_indices.txt"), "w") as f:
        f.write("\n".join(str(x) for x in test_set))
    with open(os.path.join(out_dir, "train_indices.txt"), "w") as f:
        f.write("\n".join(str(x) for x in train_set))

    df_val.to_csv(os.path.join(out_dir, "testset_per_seq_validation.csv"), index=False)

    # 汇总与矩阵统计
    debug_all = {
        "buckets_count": {k: len(v) for k, v in buckets.items()},
        "selected": {
            "Easy":   len(easy_sel),
            "Medium": len(med_sel),
            "Hard":   len(hard_sel),
            "Total":  len(test_set),
        },
        "tolerance_log": {
            "Easy":   dbg_e,
            "Medium": dbg_m,
            "Hard":   dbg_h,
        },
        "config": cfg,
    }
    with open(os.path.join(out_dir, "bucket_summary.json"), "w") as f:
        json.dump(debug_all, f, indent=2)

    with open(os.path.join(out_dir, "matrix_stats.json"), "w") as f:
        json.dump(summarize_matrix(M), f, indent=2)

    print(f"[Done] test={len(test_set)}  train={len(train_set)}")
    print(f" - Saved: test_indices.txt, train_indices.txt")
    print(f" - Saved: testset_per_seq_validation.csv, bucket_summary.json, matrix_stats.json")


if __name__ == "__main__":
    build_testset_from_npz(CONFIG)