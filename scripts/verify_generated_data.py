#!/usr/bin/env python3
"""
校验本仓库 data_generator 生成的 CSV 是否与生成规则、常识一致。

用法:
  python scripts/verify_generated_data.py --dir generated_data
  python scripts/verify_generated_data.py --dir generated_data --tree 1

与生成器保持一致的常量见 scripts/data_generator.py；此处复制最小日期逻辑，避免依赖 Faker。
"""

from __future__ import annotations

import argparse
import csv
import glob
import os
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, timedelta

# 与 data_generator.DATE_END 一致
DATE_END = date(2026, 5, 1)
MIN_MATERNAL_AGE_YEARS_AT_CHILDBIRTH = 17
MAX_CHILDREN_PER_MOTHER = 10


def add_years_safe(d: date, years: int) -> date:
    y = d.year + years
    m, day = d.month, d.day
    if m == 2 and day == 29:
        day = 28
    try:
        return date(y, m, day)
    except ValueError:
        return date(y, m, min(day, 28))


def parse_date(s: str | None) -> date | None:
    if s is None:
        return None
    t = str(s).strip()
    if not t:
        return None
    return date.fromisoformat(t)


def marriage_age_lo(hb: date, wb: date) -> date:
    ha = 18 if hb < date(1900, 1, 1) else 22
    wa = 16 if wb < date(1900, 1, 1) else 20
    return max(add_years_safe(hb, ha), add_years_safe(wb, wa))


def tree_kinship_connected(
    rows: list[list],
    marriage_rows: list[list],
) -> tuple[bool, str]:
    if len(rows) < 2:
        return False, "成员少于 2 人"

    ids = {int(r[0]) for r in rows}
    adj: dict[int, set[int]] = defaultdict(set)
    gen1: list[int] = []

    for r in rows:
        mid = int(r[0])
        gen = int(r[9])
        if gen == 1:
            gen1.append(mid)
        for idx in (7, 8):
            raw = str(r[idx]).strip() if r[idx] is not None else ""
            if raw != "":
                pid = int(raw)
                if pid not in ids:
                    return False, f"亲缘引用 member_id={pid} 不在本谱"
                adj[mid].add(pid)
                adj[pid].add(mid)

    for mr in marriage_rows:
        if len(mr) < 5:
            continue
        try:
            h, w = int(mr[2]), int(mr[3])
        except (TypeError, ValueError):
            continue
        if h in ids and w in ids:
            adj[h].add(w)
            adj[w].add(h)

    if not gen1:
        return False, "无第 1 代成员"

    start = min(gen1)
    stack = [start]
    seen = {start}
    while stack:
        u = stack.pop()
        for v in adj[u]:
            if v not in seen:
                seen.add(v)
                stack.append(v)

    if len(seen) != len(ids):
        sample = next(iter(ids - seen))
        return False, f"{len(ids - seen)} 人与主干亲缘图不连通，示例 member_id={sample}"

    return True, ""


@dataclass
class CheckStats:
    trees: int = 0
    members_scanned: int = 0
    marriages_scanned: int = 0
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    def err(self, tree_id: int, msg: str, cap: int = 30) -> None:
        line = f"tree_id={tree_id} {msg}"
        if len(self.errors) < cap:
            self.errors.append(line)
        elif len(self.errors) == cap:
            self.errors.append("…（后续同类错误省略，请修复后再跑）")

    def warn(self, tree_id: int, msg: str, cap: int = 20) -> None:
        line = f"tree_id={tree_id} {msg}"
        if len(self.warnings) < cap:
            self.warnings.append(line)


def load_csv_rows(path: str) -> list[list]:
    with open(path, newline="", encoding="utf-8-sig") as f:
        return list(csv.reader(f))


def verify_one_tree(
    tree_subdir: str,
    meta_path: str,
    st: CheckStats,
    *,
    max_issue_samples: int,
) -> None:
    rows = load_csv_rows(meta_path)
    if len(rows) < 2:
        st.err(-1, f"空或仅表头: {meta_path}", cap=max_issue_samples)
        return
    tree_id = int(rows[1][0])

    mem_path = os.path.join(tree_subdir, "members.csv")
    mar_path = os.path.join(tree_subdir, "marriages.csv")
    if not os.path.isfile(mem_path):
        st.err(tree_id, f"缺少 members.csv", cap=max_issue_samples)
        return

    m_raw = load_csv_rows(mem_path)
    if len(m_raw) < 2:
        st.err(tree_id, "members.csv 无数据行", cap=max_issue_samples)
        return
    header, body = m_raw[0], m_raw[1:]
    st.members_scanned += len(body)

    by_id: dict[int, list] = {}
    for r in body:
        try:
            mid = int(r[0])
        except (TypeError, ValueError):
            st.err(tree_id, f"非法 member_id 行: {r[:4]}…", cap=max_issue_samples)
            continue
        if mid in by_id:
            st.err(tree_id, f"重复 member_id={mid}", cap=max_issue_samples)
        by_id[mid] = r
        if len(r) < 10:
            st.err(tree_id, f"member_id={mid} 列数不足", cap=max_issue_samples)
            continue
        if int(r[1]) != tree_id:
            st.err(
                tree_id,
                f"member_id={mid} tree_id 列={r[1]} 与目录不符",
                cap=max_issue_samples,
            )

    marriage_rows: list[list] = []
    if os.path.isfile(mar_path):
        mar_raw = load_csv_rows(mar_path)
        if len(mar_raw) >= 2:
            marriage_rows = mar_raw[1:]
            st.marriages_scanned += len(marriage_rows)

    # --- 亲缘连通 ---
    ok_k, msg_k = tree_kinship_connected(body, marriage_rows)
    if not ok_k:
        st.err(tree_id, f"亲缘: {msg_k}", cap=max_issue_samples)

    # --- 按母统计子女（仅血系平民）---
    children_by_mother: defaultdict[int, int] = defaultdict(int)

    for r in body:
        mid = int(r[0])
        bio = str(r[6]).strip() if r[6] is not None else ""
        g = str(r[3]).strip()
        if g not in ("M", "F"):
            st.err(tree_id, f"member_id={mid} gender={r[3]!r}", cap=max_issue_samples)

        b = parse_date(r[4])
        if b is None:
            st.err(tree_id, f"member_id={mid} birth_date 无效", cap=max_issue_samples)
            continue
        if b > DATE_END:
            st.err(
                tree_id,
                f"member_id={mid} birth_date {b} 晚于截止 {DATE_END}",
                cap=max_issue_samples,
            )

        d = parse_date(r[5])
        if d is not None:
            if d > DATE_END:
                st.err(
                    tree_id,
                    f"member_id={mid} death_date 晚于截止",
                    cap=max_issue_samples,
                )
            if d < b:
                st.err(
                    tree_id,
                    f"member_id={mid} death_date 早于 birth_date",
                    cap=max_issue_samples,
                )

        try:
            gen = int(r[9])
        except (TypeError, ValueError):
            st.err(tree_id, f"member_id={mid} generation 无效", cap=max_issue_samples)
            continue
        if gen < 1:
            st.err(tree_id, f"member_id={mid} generation={gen}", cap=max_issue_samples)

        fs = str(r[7]).strip() if r[7] is not None else ""
        ms = str(r[8]).strip() if r[8] is not None else ""

        if bio == "平民" and fs and ms:
            try:
                fid, motid = int(fs), int(ms)
            except ValueError:
                st.err(tree_id, f"member_id={mid} 父母 id 非法", cap=max_issue_samples)
                continue
            if fid not in by_id or motid not in by_id:
                st.err(
                    tree_id,
                    f"member_id={mid} 父母不在本谱 fid={fid} mid={motid}",
                    cap=max_issue_samples,
                )
                continue
            fr, mr = by_id[fid], by_id[motid]
            fb = parse_date(fr[4])
            mb_ = parse_date(mr[4])
            if fb is None or mb_ is None:
                continue
            if b <= fb or b <= mb_:
                st.err(
                    tree_id,
                    f"member_id={mid} 子女出生不晚于父母",
                    cap=max_issue_samples,
                )

            mother_min = add_years_safe(mb_, MIN_MATERNAL_AGE_YEARS_AT_CHILDBIRTH)
            if b < mother_min:
                st.err(
                    tree_id,
                    f"member_id={mid} 母亲分娩时未满 {MIN_MATERNAL_AGE_YEARS_AT_CHILDBIRTH} 周岁 "
                    f"(母生 {mb_} 子生 {b})",
                    cap=max_issue_samples,
                )

            fg = int(fr[9])
            mg = int(mr[9])
            if fg != mg:
                st.warn(
                    tree_id,
                    f"member_id={mid} 父母世代不一致 father_gen={fg} mother_gen={mg}",
                    cap=max_issue_samples,
                )
            elif gen != fg + 1:
                st.err(
                    tree_id,
                    f"member_id={mid} 平民子女世代应为父母+1: child={gen} parent={fg}",
                    cap=max_issue_samples,
                )

            fd = parse_date(fr[5])
            md = parse_date(mr[5])
            if md is not None and b > md:
                st.err(
                    tree_id,
                    f"member_id={mid} 出生日晚于母亲卒日",
                    cap=max_issue_samples,
                )
            if fd is not None and b > fd:
                st.warn(
                    tree_id,
                    f"member_id={mid} 出生日晚于父亲卒日（可能再婚/录入误差）",
                    cap=max_issue_samples,
                )

            children_by_mother[motid] += 1

    for motid, cnt in children_by_mother.items():
        if cnt > MAX_CHILDREN_PER_MOTHER:
            st.err(
                tree_id,
                f"mother_member_id={motid} 本代血系子女数 {cnt} > {MAX_CHILDREN_PER_MOTHER}",
                cap=max_issue_samples,
            )

    # --- 婚姻 ---
    for mr in marriage_rows:
        if len(mr) < 5:
            continue
        try:
            mtid = int(mr[1])
            hid, wid = int(mr[2]), int(mr[3])
        except (TypeError, ValueError):
            continue
        if mtid != tree_id:
            st.err(
                tree_id,
                f"marriage tree_id={mtid} 与目录不符 hid={hid} wid={wid}",
                cap=max_issue_samples,
            )
        if hid not in by_id or wid not in by_id:
            st.err(
                tree_id,
                f"婚姻引用不存在 husband={hid} wife={wid}",
                cap=max_issue_samples,
            )
            continue
        hr, wr = by_id[hid], by_id[wid]
        if str(hr[3]).strip() != "M" or str(wr[3]).strip() != "F":
            st.warn(
                tree_id,
                f"婚姻 gender 非标准夫M妻F: husband={hid} wife={wid}",
                cap=max_issue_samples,
            )
        hb = parse_date(hr[4])
        wb = parse_date(wr[4])
        sd = parse_date(mr[4])
        if hb is None or wb is None or sd is None:
            st.err(
                tree_id,
                f"婚姻日期解析失败 h={hid} w={wid}",
                cap=max_issue_samples,
            )
            continue
        if sd > DATE_END:
            st.err(
                tree_id,
                f"婚日 {sd} 晚于截止 marriage h={hid} w={wid}",
                cap=max_issue_samples,
            )
        lo = marriage_age_lo(hb, wb)
        if sd < lo:
            st.err(
                tree_id,
                f"婚日早于法定婚龄下限 marriage h={hid} w={wid} start={sd} 须>={lo}",
                cap=max_issue_samples,
            )

    st.trees += 1


def main() -> int:
    ap = argparse.ArgumentParser(
        description="校验族谱生成 CSV 的日期、亲缘、婚龄、生育年龄等是否合理",
    )
    ap.add_argument(
        "--dir",
        default="generated_data",
        help="生成根目录（含 tree_*/members.csv）",
    )
    ap.add_argument(
        "--tree",
        type=int,
        default=None,
        metavar="N",
        help="只校验 tree_NNN（如 1 表示 tree_001）",
    )
    ap.add_argument(
        "--max-samples",
        type=int,
        default=40,
        help="每种检查最多记录多少条示例错误后省略",
    )
    args = ap.parse_args()
    root = os.path.abspath(args.dir)

    if args.tree is not None:
        pattern = os.path.join(root, f"tree_{args.tree:03d}", "family_tree.csv")
        metas = sorted(glob.glob(pattern))
    else:
        pattern = os.path.join(root, "tree_*", "family_tree.csv")
        metas = sorted(glob.glob(pattern))

    if not metas:
        print(f"[错误] 未找到 family_tree.csv：{pattern}", file=sys.stderr)
        return 2

    st = CheckStats()
    for meta_path in metas:
        sub = os.path.dirname(meta_path)
        verify_one_tree(sub, meta_path, st, max_issue_samples=args.max_samples)

    print(f"已扫描 {st.trees} 棵族谱，成员行 {st.members_scanned}，婚姻行 {st.marriages_scanned}")
    print(f"截止日（与生成器一致）: {DATE_END.isoformat()}")
    print(f"检查项摘要: 日期≤截止、卒≥生、亲缘连通、父母引用、平民世代+1、母亲分娩≥{MIN_MATERNAL_AGE_YEARS_AT_CHILDBIRTH}周岁、母卒≥分娩、每母血系子女≤{MAX_CHILDREN_PER_MOTHER}、婚龄（近代男22女20 / 近代前男18女16）")

    if st.warnings:
        print(f"\n[警告] 共 {len(st.warnings)} 条（前若干条）:")
        for w in st.warnings[:25]:
            print(" -", w)
        if len(st.warnings) > 25:
            print(f" … 另有 {len(st.warnings) - 25} 条")

    if st.errors:
        print(f"\n[未通过] 共 {len(st.errors)} 条:")
        for e in st.errors:
            print(" -", e)
        return 1

    print("\n[通过] 未发现错误（警告可酌情处理）。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
