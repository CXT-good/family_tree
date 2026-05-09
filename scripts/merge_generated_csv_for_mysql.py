#!/usr/bin/env python3
"""
将 generated_data 目录下的分散 CSV 合并为 5 个与数据库表一一对应的文件，便于 Navicat / LOAD DATA 一次性导入。

输出（默认 load_data/）：
  users.csv          ← generated_data/users.csv
  family_trees.csv   ← tree_*/family_tree.csv 合并
  members.csv        ← tree_*/members.csv 合并
  marriages.csv      ← tree_*/marriages.csv 合并
  tree_managers.csv  ← tree_*/tree_managers.csv 合并

用法:
  python scripts/merge_generated_csv_for_mysql.py
  python scripts/merge_generated_csv_for_mysql.py --src generated_data --out load_data
"""

from __future__ import annotations

import argparse
import csv
import os
import re
import sys


def sorted_tree_dirs(src_root: str) -> list[str]:
    paths = [
        os.path.join(src_root, name)
        for name in os.listdir(src_root)
        if name.startswith("tree_") and os.path.isdir(os.path.join(src_root, name))
    ]

    def key(p: str) -> int:
        m = re.search(r"tree_(\d+)$", os.path.basename(p))
        return int(m.group(1)) if m else 0

    return sorted(paths, key=key)


def merge_csv_files(src_paths: list[str], dest_path: str) -> tuple[int, int]:
    """
    合并多个同结构 CSV（仅首文件写表头）。
    返回 (写入行数不含表头, 源文件数)。
    """
    os.makedirs(os.path.dirname(os.path.abspath(dest_path)) or ".", exist_ok=True)
    first = True
    header: list[str] | None = None
    n_rows = 0
    n_sources = 0

    with open(dest_path, "w", newline="", encoding="utf-8-sig") as out:
        writer = csv.writer(out)
        for src in src_paths:
            if not os.path.isfile(src):
                continue
            n_sources += 1
            with open(src, newline="", encoding="utf-8-sig") as f:
                reader = csv.reader(f)
                try:
                    h = next(reader)
                except StopIteration:
                    continue
                if first:
                    writer.writerow(h)
                    header = h
                    first = False
                else:
                    if list(h) != list(header):
                        raise ValueError(
                            f"表头不一致:\n  期望 {header}\n  文件 {src}\n  得到 {h}"
                        )
                for row in reader:
                    writer.writerow(row)
                    n_rows += 1

    if first:
        raise FileNotFoundError(f"没有可用的源文件写入 {dest_path}")
    return n_rows, n_sources


def main() -> int:
    ap = argparse.ArgumentParser(description="合并族谱 CSV 为 5 个库表文件")
    ap.add_argument(
        "--src",
        default="generated_data",
        help="生成数据根目录（含 users.csv 与 tree_*/）",
    )
    ap.add_argument(
        "--out",
        default="load_data",
        help="输出目录",
    )
    args = ap.parse_args()

    src_root = os.path.abspath(args.src)
    out_root = os.path.abspath(args.out)

    if not os.path.isdir(src_root):
        print(f"[错误] 源目录不存在: {src_root}", file=sys.stderr)
        return 2

    users_src = os.path.join(src_root, "users.csv")
    if not os.path.isfile(users_src):
        print(f"[错误] 缺少 {users_src}", file=sys.stderr)
        return 2

    tree_dirs = sorted_tree_dirs(src_root)
    if not tree_dirs:
        print(f"[错误] {src_root} 下未找到 tree_* 子目录", file=sys.stderr)
        return 2

    print(f"源目录: {src_root}")
    print(f"输出目录: {out_root}")
    print(f"族谱子目录: {len(tree_dirs)} 个\n")

    # 1) users
    dst_users = os.path.join(out_root, "users.csv")
    n_u, k_u = merge_csv_files([users_src], dst_users)
    print(f"[users]           <- users.csv          行数={n_u}  (源文件 {k_u})")

    # 2) family_trees ← family_tree.csv
    ft_sources = [os.path.join(d, "family_tree.csv") for d in tree_dirs]
    dst_ft = os.path.join(out_root, "family_trees.csv")
    n_ft, k_ft = merge_csv_files(ft_sources, dst_ft)
    print(f"[family_trees]    <- tree_*/family_tree.csv  行数={n_ft}  (树 {k_ft})")

    # 3) members
    mem_sources = [os.path.join(d, "members.csv") for d in tree_dirs]
    dst_mem = os.path.join(out_root, "members.csv")
    n_m, k_m = merge_csv_files(mem_sources, dst_mem)
    print(f"[members]         <- tree_*/members.csv    行数={n_m}  (树 {k_m})")

    # 4) marriages
    mar_sources = [os.path.join(d, "marriages.csv") for d in tree_dirs]
    dst_mar = os.path.join(out_root, "marriages.csv")
    n_r, k_r = merge_csv_files(mar_sources, dst_mar)
    print(f"[marriages]       <- tree_*/marriages.csv  行数={n_r}  (树 {k_r})")

    # 5) tree_managers
    mgr_sources = [os.path.join(d, "tree_managers.csv") for d in tree_dirs]
    dst_mgr = os.path.join(out_root, "tree_managers.csv")
    n_g, k_g = merge_csv_files(mgr_sources, dst_mgr)
    print(f"[tree_managers]   <- tree_*/tree_managers.csv 行数={n_g}  (树 {k_g})")

    print(f"\n完成。请向数据库导入时按顺序: users → family_trees → members → marriages → tree_managers")
    print(f"文件前缀与表名一致（family_trees.csv 对应表 family_trees）。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
