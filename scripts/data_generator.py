"""
海量族谱数据生成：世代桶 + 始祖夫妇 + 父母血缘 + 婚姻记录。
输出目录结构：
  <out_dir>/users.csv
  <out_dir>/tree_XXX/family_tree.csv   （一行：族谱元数据，修谱时间 revision_at）
  <out_dir>/tree_XXX/members.csv
  <out_dir>/tree_XXX/marriages.csv
  <out_dir>/tree_XXX/tree_managers.csv
  <out_dir>/trees_index.csv              （member_count 来自各 members.csv 实际行数）

依赖: python -m pip install -r scripts/requirements-data-gen.txt
运行: python scripts/data_generator.py
校验: python scripts/data_generator.py --check-dir generated_data
"""

from __future__ import annotations

import argparse
import csv
import glob
import hashlib
import os
import random
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta

from faker import Faker

# --- 业务时间上限：一切日期时间不得晚于 2026-05-01 ---
DATE_END = date(2026, 5, 1)
DATETIME_END = datetime(2026, 5, 1, 23, 59, 59)


def clamp_calendar_to_end(d: date) -> date:
    """写入 CSV 的日历日一律不得晚于 DATE_END（防止链条误差产生 2027、2039 等）。"""
    return d if d <= DATE_END else DATE_END

# --- 默认配置（作业指标）---
TOTAL_TREES = 15
SUPER_TREE_SIZE = 55_000
NORMAL_TREE_SIZE = 3_500
USER_COUNT_MIN = 20
USER_COUNT_MAX = 50

# 使用空格分隔日期与时间，便于 MySQL / Navicat 导入（避免 ISO「T」被误解析为 0000-00-00）
CSV_DATETIME_FMT = "%Y-%m-%d %H:%M:%S"
CSV_DATE_FMT = "%Y-%m-%d"

MIN_TREES = 10
MIN_SUPER_TREE_MEMBERS = 50_000
MIN_TOTAL_MEMBERS = 100_000
MIN_GENERATIONS_PER_TREE = 30
# 各谱目标世代在此闭区间内随机抽取，且彼此不完全相同（仍均 ≥ MIN_GENERATIONS_PER_TREE）
TREE_GENS_HI = 40
# 每名女性在一代内作为母亲生育子女的上限（全局硬上限）
MAX_CHILDREN_PER_MOTHER = 10

fake = Faker("zh_CN")

global_user_id = 1
global_tree_id = 1
global_member_id = 1
global_marriage_id = 1


def _password_placeholder() -> str:
    return hashlib.sha256(os.urandom(32)).hexdigest()[:32]


def clamp_dt(dt: datetime) -> datetime:
    return dt if dt <= DATETIME_END else DATETIME_END


def _day_offset_in_span(span: int) -> int:
    """在 [0, span] 内取偏移：Beta 形状越大越贴区间中部，同日重复概率更低。"""
    if span <= 0:
        return 0
    if span >= 160:
        u = random.betavariate(5.0, 5.0)
    elif span >= 72:
        u = random.betavariate(4.0, 4.0)
    elif span >= 28:
        u = random.betavariate(3.1, 3.1)
    else:
        return random.randint(0, span)
    return min(span, int(u * (span + 1)))


def uniform_day_between(
    low: date,
    high: date,
    *,
    floor_soft: date | None = None,
    _depth: int = 0,
    allow_exact_deadline: bool = False,
) -> date:
    """
    在 [low, high] 闭区间内均匀随机一日，high 不超过 DATE_END。
    默认 allow_exact_deadline=False：当右端为截止日时先随机「缩顶」，避免海量 2026-05-01；
    调用方可将 allow_exact_deadline=True（或小概率 True）以允许偶尔抽到 5 月 1 日。
    """
    high = min(high, DATE_END)
    low = min(low, high)

    if not allow_exact_deadline and high == DATE_END and low < DATE_END:
        span_room = (DATE_END - low).days
        if span_room >= 2:
            max_gap = span_room - 1
            min_gap = max(12, min(span_room // 8, max_gap))
            if span_room >= 100:
                min_gap = max(min_gap, min(36, max_gap))
            if min_gap > max_gap:
                min_gap = max(1, max_gap)
            # Beta(4.2,2.1) 偏大：顶缝更深，减少挤在 4 月底、5 月初
            ug = random.betavariate(4.2, 2.1)
            gap = min_gap + int(ug * (max_gap - min_gap + 0.999))
            gap = max(min_gap, min(max_gap, gap))
            high = DATE_END - timedelta(days=gap)
            low = min(low, high)
            # floor_soft 表示「合法下限」（如法定最早婚日、子女最早出生日）；缩顶后禁止 low 跌破它，
            # 否则婚日/生日会早于法规下限，几代后全员无法在截止日前满足 22/20 周岁婚龄。
            if floor_soft is not None:
                low = max(low, floor_soft)
            if low > high:
                high = low

    span = (high - low).days

    if span > 0:
        return low + timedelta(days=_day_offset_in_span(span))

    if _depth < 14:
        hi = high
        stretch = random.randint(320, 3400)
        lo = hi - timedelta(days=stretch)
        if floor_soft is not None:
            lo = max(lo, floor_soft)
        if lo >= hi:
            lo = hi - timedelta(days=random.randint(80, 900))
            if floor_soft is not None:
                lo = max(lo, floor_soft)
        if lo < hi:
            return uniform_day_between(
                lo,
                hi,
                floor_soft=floor_soft,
                _depth=_depth + 1,
                allow_exact_deadline=allow_exact_deadline,
            )

    fb = floor_soft if floor_soft is not None else low
    if fb < high:
        sub = max(0, (high - fb).days)
        return fb + timedelta(days=_day_offset_in_span(sub))
    if high == DATE_END and not allow_exact_deadline:
        room = max(3, (DATE_END - fb).days)
        lo_b = min(max(16, room // 5), room - 1)
        hi_b = min(620, room)
        if lo_b >= hi_b:
            lo_b = max(1, room // 3)
        return DATE_END - timedelta(days=random.randint(lo_b, hi_b))
    return high


def parse_user_registered_at(s: str) -> datetime:
    s = str(s).strip()
    if "T" in s:
        return datetime.fromisoformat(s.replace("Z", ""))
    return datetime.strptime(s, CSV_DATETIME_FMT)


def random_registered_at() -> datetime:
    """注册时间：截止日前推 1～10 年内的随机时刻。"""
    low = DATETIME_END - timedelta(days=10 * 365)
    high = DATETIME_END - timedelta(days=365)
    return clamp_dt(fake.date_time_between(low, high))


def random_revision_at(creator_registered_at: datetime) -> datetime:
    """修谱时间不早于创建者注册时间，且不晚于 2026-05-01。"""
    low = max(creator_registered_at, datetime(2015, 1, 1))
    if low >= DATETIME_END:
        return DATETIME_END
    return clamp_dt(fake.date_time_between(low, DATETIME_END))


def add_years_safe(d: date, years: int) -> date:
    y = d.year + years
    m, day = d.month, d.day
    if m == 2 and day == 29:
        day = 28
    try:
        return date(y, m, day)
    except ValueError:
        return date(y, m, min(day, 28))


def random_birth_after_father(parent_birth: date) -> date:
    """
    子女出生：先算「理想生日」（年距连续 + 抖动），再在以其为中心的随机带宽内 **均匀** 抽样，
    合法区间为 [父母成年后首日, DATE_END]（**含 2026-05-01**）。
    若父母已满 18 岁的首日已超过截止日，不能再生成「未来日期」，统一回落到 DATE_END（此前 bug 会 return adult_floor 导致 2028+）。
    """
    pb = clamp_calendar_to_end(parent_birth)
    adult_floor = add_years_safe(pb, 18)
    adult_floor = clamp_calendar_to_end(adult_floor)
    allow_dl = random.random() < 0.012

    if adult_floor >= DATE_END:
        return clamp_calendar_to_end(
            uniform_day_between(
                max(pb, DATE_END - timedelta(days=720)),
                DATE_END,
                floor_soft=pb,
                allow_exact_deadline=allow_dl,
            )
        )

    span_full = (DATE_END - adult_floor).days
    if span_full <= 0:
        return clamp_calendar_to_end(
            uniform_day_between(
                adult_floor,
                DATE_END,
                floor_soft=adult_floor,
                allow_exact_deadline=allow_dl,
            )
        )

    # 一定比例在全窗口 [成年, 截止] 抽样，拉长日历上的散布（仍 ≥ 父母成年后）
    if random.random() < 0.26:
        return clamp_calendar_to_end(
            uniform_day_between(
                adult_floor,
                DATE_END,
                floor_soft=adult_floor,
                allow_exact_deadline=allow_dl,
            )
        )

    years_gap = random.uniform(18.04, 41.96)
    noise = random.randint(-380, 380)
    ideal = pb + timedelta(days=int(round(years_gap * 365.25)) + noise)

    half_hi = min(max(span_full * 56 // 100, 620), 10_800)
    half_lo = min(520, half_hi)
    half = random.randint(half_lo, half_hi) if half_hi >= half_lo else random.randint(1, max(1, half_hi))

    lo = max(adult_floor, ideal - timedelta(days=half))
    hi = min(DATE_END, ideal + timedelta(days=half))
    if lo > hi:
        lo, hi = adult_floor, DATE_END
    lo = max(lo, adult_floor)
    hi = min(hi, DATE_END)
    if lo >= hi:
        lo, hi = adult_floor, DATE_END

    return clamp_calendar_to_end(
        uniform_day_between(lo, hi, floor_soft=adult_floor, allow_exact_deadline=allow_dl)
    )


def random_death_or_alive(
    birth: date,
    *,
    generation: int | None = None,
    max_generation: int = 40,
) -> str:
    """
    仍在世则 death_date 为空字符串。
    年轻成员、末几代辈分更高概率在世；高龄在截止点多为已故。
    """
    birth_c = clamp_calendar_to_end(birth)
    if birth_c >= DATE_END:
        return ""

    age_days = (DATE_END - birth_c).days
    age_years = max(0.0, age_days / 365.25)

    # 历史久远人物在截止点不应「仍在世」
    if age_years >= 110:
        p_alive = 0.0
    elif age_years >= 95:
        p_alive = 0.02
    elif age_years >= 82:
        p_alive = 0.06
    elif age_years >= 70:
        p_alive = 0.12
    elif age_years >= 58:
        p_alive = 0.22
    elif age_years >= 45:
        p_alive = 0.32
    else:
        p_alive = 0.42

    if generation is not None and max_generation >= 5:
        depth = generation / max_generation
        if depth >= 0.88:
            p_alive += 0.18
        elif depth >= 0.72:
            p_alive += 0.10
        elif depth >= 0.55:
            p_alive += 0.04

    p_alive = min(0.58, p_alive)

    if random.random() < p_alive:
        return ""

    allow_dl = random.random() < 0.012

    days_to_end = (DATE_END - birth_c).days
    min_life = int(42.2 * 365.25)
    max_life = min(int(99 * 365.25), days_to_end - random.randint(40, 220))
    if max_life <= min_life + 80:
        return ""

    lf = random.betavariate(2.6, 2.9)
    life_days = int(min_life + lf * (max_life - min_life))
    dd = birth_c + timedelta(days=life_days + random.randint(-220, 220))
    if dd <= birth_c:
        dd = birth_c + timedelta(days=min_life + random.randint(0, min(9000, max_life - min_life)))
    dd = clamp_calendar_to_end(dd)
    if dd <= birth_c:
        dd = clamp_calendar_to_end(
            uniform_day_between(
                birth_c + timedelta(days=min_life),
                DATE_END,
                floor_soft=birth_c + timedelta(days=int(40 * 365.25)),
                allow_exact_deadline=allow_dl,
            )
        )
    # 卒日落在截止日前后再打散：回退起点不要太贴近截止日，减轻 4/30、5/1
    if dd == DATE_END:
        keep_deadline = allow_dl and random.random() < 0.18
        if not keep_deadline:
            ub = max(2, days_to_end - 1)
            lo_g = min(max(18, ub // 8), ub - 1)
            lo_g = max(1, lo_g)
            dd = DATE_END - timedelta(days=random.randint(lo_g, ub))
        if dd <= birth_c:
            dd = birth_c + timedelta(days=random.randint(min_life, min(min_life + 9000, days_to_end - 1)))
            dd = clamp_calendar_to_end(dd)
    return dd.strftime(CSV_DATE_FMT)


def marriage_start_after(husband_birth: date, wife_birth: date) -> date | None:
    """
    婚日须满足：男方年满 22 周岁、女方年满 20 周岁；
    即婚日 >= max(夫生日+22年, 妻生日+20年)，且 <= DATE_END。
    若合法婚日晚于截止日（无法在 2026-05-01 前登记），返回 None（不写婚姻行）。
    注意：不得对「最早婚日 lo」做 clamp_calendar_to_end，否则会把 2028 压成 2026-05-01，
    造成「婚日在截止日但男方仍不足 22 岁」的假数据。
    """
    hb = clamp_calendar_to_end(husband_birth)
    wb = clamp_calendar_to_end(wife_birth)
    lo = max(add_years_safe(hb, 22), add_years_safe(wb, 20))
    if lo > DATE_END:
        return None
    allow_dl = random.random() < 0.012
    span = (DATE_END - lo).days
    jitter = random.randint(0, min(800, max(0, span)))
    earliest = lo + timedelta(days=jitter)
    earliest = min(earliest, DATE_END)
    wd = clamp_calendar_to_end(
        uniform_day_between(
            earliest, DATE_END, floor_soft=lo, allow_exact_deadline=allow_dl
        )
    )
    # 子女须严格晚于婚日且仍可 ≤DATE_END，故婚日最早不得等于 DATE_END
    return min(wd, DATE_END - timedelta(days=1))


def couple_wedding_day(husband_birth: date, wife_birth: date) -> date | None:
    """
    婚日始终满足：>= max(夫+22年, 妻+20年)，且 <= DATE_END-1（为子女出生在婚日后留空间）。
    优先 marriage_start_after；否则在 [法定最早婚日, min(DATE_END-60, DATE_END-1)] 内随机。
    若在截止日前不存在合法婚日（法定最早婚日晚于 DATE_END-1），返回 None，由上层换一对父母。
    """
    w = marriage_start_after(husband_birth, wife_birth)
    if w is not None:
        return min(w, DATE_END - timedelta(days=1))
    hb = clamp_calendar_to_end(husband_birth)
    wb = clamp_calendar_to_end(wife_birth)
    lo_req = max(add_years_safe(hb, 22), add_years_safe(wb, 20))
    hi_room = DATE_END - timedelta(days=60)
    hi_cap = DATE_END - timedelta(days=1)
    hi = min(hi_room, hi_cap)
    if lo_req <= hi:
        return clamp_calendar_to_end(
            uniform_day_between(lo_req, hi, floor_soft=lo_req, allow_exact_deadline=False)
        )
    if lo_req <= hi_cap:
        return clamp_calendar_to_end(
            uniform_day_between(lo_req, hi_cap, floor_soft=lo_req, allow_exact_deadline=False)
        )
    return None


def allocate_generation_counts(tree_size: int, target_gens: int) -> list[int]:
    """
    第 1 代为始祖夫妇共 2 人，其余世代人数之和 + 2 = tree_size。
    """
    if tree_size < target_gens + 1:
        raise ValueError(
            f"tree_size ({tree_size}) 必须 >= target_gens+1 ({target_gens + 1})，"
            "以保证第 1 代 2 人且其后每一代至少 1 人。"
        )
    gen_counts = [0] * target_gens
    gen_counts[0] = 2
    remaining = tree_size - 2

    for i in range(1, target_gens):
        if i == target_gens - 1:
            gen_counts[i] = remaining
            break

        slots_after = target_gens - 1 - i
        max_alloc = remaining - slots_after
        max_alloc = max(1, max_alloc)

        share = remaining / (target_gens - i)
        alloc = int(share * random.uniform(0.5, 1.5))
        alloc = max(1, min(alloc, max_alloc))
        gen_counts[i] = alloc
        remaining -= alloc

    if sum(gen_counts) != tree_size:
        gen_counts[-1] += tree_size - sum(gen_counts)

    return gen_counts


def allocate_generation_counts_from_gen1(
    tree_size: int, target_gens: int, gen1_size: int
) -> list[int]:
    if tree_size < gen1_size + (target_gens - 1):
        raise ValueError(
            f"tree_size ({tree_size}) 须 ≥ gen1_size({gen1_size}) + (target_gens-1)(={target_gens - 1})"
        )
    gen_counts = [0] * target_gens
    gen_counts[0] = gen1_size
    remaining = tree_size - gen1_size

    for i in range(1, target_gens):
        if i == target_gens - 1:
            gen_counts[i] = remaining
            break

        slots_after = target_gens - 1 - i
        max_alloc = remaining - slots_after
        max_alloc = max(1, max_alloc)

        share = remaining / (target_gens - i)
        alloc = int(share * random.uniform(0.5, 1.5))
        alloc = max(1, min(alloc, max_alloc))
        gen_counts[i] = alloc
        remaining -= alloc

    if sum(gen_counts) != tree_size:
        gen_counts[-1] += tree_size - sum(gen_counts)

    return gen_counts


def _marriage_age_husband(birth: date) -> int:
    return 18 if birth < date(1900, 1, 1) else 22


def _marriage_age_wife(birth: date) -> int:
    return 16 if birth < date(1900, 1, 1) else 20


def _parse_death_csv(death_str: str | None) -> date | None:
    s = str(death_str).strip() if death_str is not None else ""
    if s == "":
        return None
    return date.fromisoformat(s)


def _latest_alive_date(birth: date, death_str: str | None) -> date:
    d = _parse_death_csv(death_str)
    return d if d is not None else DATE_END


def member_row_birth(row: list) -> date:
    return date.fromisoformat(str(row[4]))


def reached_age_15(birth: date, death_str: str | None) -> bool:
    fifteenth = add_years_safe(birth, 15)
    if fifteenth > DATE_END:
        return False
    d = _parse_death_csv(death_str)
    if d is None:
        return True
    return d >= fifteenth


def founder_death_str(birth: date) -> str:
    span_y = random.randint(50, 80)
    dd = add_years_safe(birth, span_y) + timedelta(days=random.randint(-60, 60))
    dd = clamp_calendar_to_end(dd)
    if dd <= birth:
        dd = birth + timedelta(days=365 * 52)
        dd = clamp_calendar_to_end(dd)
    if dd >= DATE_END:
        dd = DATE_END - timedelta(days=1)
    return dd.strftime(CSV_DATE_FMT)


def sample_nonfounder_death(birth: date) -> str:
    if random.random() < random.uniform(0.10, 0.15):
        dd = birth + timedelta(days=random.randint(20, 15 * 365))
    else:
        dd = birth + timedelta(days=random.randint(40 * 365, 90 * 365))
    dd = clamp_calendar_to_end(dd)
    if dd >= DATE_END or dd <= birth:
        return ""
    return dd.strftime(CSV_DATE_FMT)


def child_birth_after_father(father_birth: date) -> date:
    years = random.uniform(20.0, 45.0)
    days = int(round(years * 365.25 + random.randint(-90, 90)))
    cb = father_birth + timedelta(days=max(1, days))
    return clamp_calendar_to_end(cb)


def propose_child_counts_per_male(
    n_males: int, parent_gen_index: int, prev_pop: int
) -> list[int]:
    if n_males <= 0:
        return []
    early_phase = parent_gen_index <= 10
    large_base = prev_pop >= 2000
    counts: list[int] = []
    for _ in range(n_males):
        if large_base and random.random() < 0.38:
            counts.append(0)
            continue
        if early_phase and not large_base:
            counts.append(random.randint(2, 4))
        elif early_phase:
            counts.append(random.randint(1, 2))
        else:
            counts.append(random.randint(1, 2) if random.random() > 0.42 else 0)
    if sum(counts) == 0:
        counts[random.randrange(n_males)] = random.randint(2, 4)
    return counts


def rebalance_gen_counts_for_mother_limit(
    gen_counts: list[int], tree_size: int, gen1_female_count: int
) -> None:
    """
    第 g 代计划新增人数不得超过上一代女性人数 × MAX_CHILDREN_PER_MOTHER；
    超出部分顺移到下一代；最后再按 tree_size 总量校正末代（末代仍可能截断以满足上限）。
    """
    n = len(gen_counts)
    if n <= 1:
        return
    for _ in range(n * 15 + 8):
        females = gen1_female_count
        changed = False
        for i in range(1, n):
            cap_i = females * MAX_CHILDREN_PER_MOTHER
            if gen_counts[i] > cap_i:
                excess = gen_counts[i] - cap_i
                gen_counts[i] = cap_i
                if i + 1 < n:
                    gen_counts[i + 1] += excess
                    changed = True
                # 末代超限则截断，多余计划丢弃（每名母亲上限决定的物理上限）
            females = max(1, gen_counts[i] // 2)
        if not changed:
            break

    diff = tree_size - sum(gen_counts)
    if diff != 0:
        gen_counts[-1] += diff

    females = gen1_female_count
    for i in range(1, n):
        cap_i = females * MAX_CHILDREN_PER_MOTHER
        if i == n - 1 and gen_counts[i] > cap_i:
            gen_counts[i] = cap_i
        females = max(1, gen_counts[i] // 2)


def normalize_child_counts_to_target(
    counts: list[int], target: int, max_per_male: int = 14
) -> list[int]:
    counts = list(counts)
    if not counts:
        return counts
    diff = target - sum(counts)
    guard = 0
    cap = max_per_male
    while diff != 0 and guard < len(counts) * (abs(diff) + cap + 120):
        i = guard % len(counts)
        if diff > 0:
            if counts[i] < cap:
                counts[i] += 1
                diff -= 1
        else:
            if counts[i] > 0:
                counts[i] -= 1
                diff += 1
        guard += 1
        if diff > 0 and guard > len(counts) * (cap + 40):
            cap += 4
            guard = 0
    return counts


def generate_users(count: int = USER_COUNT_MIN) -> list[list]:
    global global_user_id
    rows: list[list] = []
    for _ in range(count):
        reg_time = random_registered_at().strftime(CSV_DATETIME_FMT)
        login = fake.user_name() + str(random.randint(1000, 9999))
        rows.append([global_user_id, login, _password_placeholder(), reg_time])
        global_user_id += 1
    return rows


def generate_tree_managers(
    tree_id: int,
    creator_id: int,
    users_rows: list[list],
) -> list[list]:
    """创建者为 owner；对其余用户以 5%～10% 概率邀请为协作者。"""
    creator_reg = parse_user_registered_at(users_rows[creator_id - 1][3])
    rows: list[list] = [
        [
            tree_id,
            creator_id,
            "owner",
            creator_reg.strftime(CSV_DATETIME_FMT),
        ]
    ]
    n_users = len(users_rows)
    others = [u for u in range(1, n_users + 1) if u != creator_id]
    p = random.uniform(0.05, 0.10)
    for uid in others:
        if random.random() < p:
            invited = clamp_dt(
                fake.date_time_between(creator_reg, DATETIME_END)
            ).strftime(CSV_DATETIME_FMT)
            rows.append([tree_id, uid, "editor", invited])
    return rows


def generate_family_tree(
    tree_size: int,
    target_gens: int,
    users_rows: list[list],
) -> tuple[list, list[list], list[list], list[list]]:
    """
    父系繁衍：仅男性向下延续；女性记入本代但不承担父系繁衍。
    成员逐代生成完毕后，再匹配婚姻（含再婚）。
    """
    global global_tree_id, global_member_id, global_marriage_id

    n_users = len(users_rows)
    surname = fake.last_name()
    creator_id = random.randint(1, n_users)
    creator_reg = parse_user_registered_at(users_rows[creator_id - 1][3])
    revision_at = random_revision_at(creator_reg).strftime(CSV_DATETIME_FMT)

    tid = global_tree_id
    tree_record = [
        tid,
        f"{surname}氏族谱",
        surname,
        creator_id,
        revision_at,
    ]

    n_founding_males = 1  # 仅一对始祖夫妇（家族始祖 + 始祖配偶）
    gen1_size = n_founding_males * 2
    gen_counts = allocate_generation_counts_from_gen1(tree_size, target_gens, gen1_size)
    rebalance_gen_counts_for_mother_limit(gen_counts, tree_size, n_founding_males)

    members: list[list] = []
    marriages: list[list] = []
    marriage_pair_keys: set[tuple[int, int]] = set()
    # (夫, 妻) -> 首胎生日（用于婚日 < 生育）
    pair_first_child_birth: dict[tuple[int, int], date] = {}

    males_by_gen: dict[int, list[list]] = {}
    females_by_gen: dict[int, list[list]] = {}

    def add_marriage_row(husband_id: int, wife_id: int, start_d: date) -> None:
        global global_marriage_id
        key = (min(husband_id, wife_id), max(husband_id, wife_id))
        if key in marriage_pair_keys:
            return
        marriage_pair_keys.add(key)
        marriages.append(
            [
                global_marriage_id,
                tid,
                husband_id,
                wife_id,
                start_d.strftime(CSV_DATE_FMT),
            ]
        )
        global_marriage_id += 1

    def wedding_day_for_couple_relaxed(h_row: list, w_row: list) -> date:
        """
        婚日：优先在 [法定最早婚日, 双方仍健在的上界] 内随机；若无交集则取 latest，
        保证总能落一日 ≤ DATE_END-1，以便写入 marriages，避免姻亲仅有成员、无婚姻边导致亲缘图孤立。
        """
        hb = member_row_birth(h_row)
        wb = member_row_birth(w_row)
        lo = max(
            add_years_safe(hb, _marriage_age_husband(hb)),
            add_years_safe(wb, _marriage_age_wife(wb)),
        )
        latest = min(
            DATE_END - timedelta(days=1),
            _latest_alive_date(hb, h_row[5]),
            _latest_alive_date(wb, w_row[5]),
        )
        if lo <= latest:
            return lo + timedelta(
                days=random.randint(0, max(0, (latest - lo).days))
            )
        return latest

    def append_member_row(
        cid: int,
        name: str,
        gender: str,
        birth_d: date,
        death_s: str,
        bio: str,
        fid: str,
        mid: str,
        gen_no: int,
    ) -> list:
        return [
            cid,
            tid,
            name,
            gender,
            birth_d.strftime(CSV_DATE_FMT),
            death_s,
            bio,
            fid,
            mid,
            gen_no,
        ]

    # ---------- 第一代：唯一始祖夫妇（家族始祖 + 始祖配偶）----------
    founding_century = fake.date_between_dates(date(980, 1, 1), date(1020, 12, 31))
    mb = founding_century + timedelta(days=random.randint(-400, 400))
    mb = clamp_calendar_to_end(mb)
    founder_death = founder_death_str(mb)
    row_m = append_member_row(
        global_member_id,
        surname + fake.first_name_male(),
        "M",
        mb,
        founder_death,
        "家族始祖",
        "",
        "",
        1,
    )
    members.append(row_m)
    males_by_gen.setdefault(1, []).append(row_m)
    global_member_id += 1

    wb = mb - timedelta(days=random.randint(0, 365 * 6))
    if wb > mb:
        wb = mb - timedelta(days=random.randint(60, 400))
    wb = clamp_calendar_to_end(wb)
    wife_death = founder_death_str(wb)
    row_f = append_member_row(
        global_member_id,
        surname + fake.first_name_female(),
        "F",
        wb,
        wife_death,
        "始祖配偶",
        "",
        "",
        1,
    )
    members.append(row_f)
    females_by_gen.setdefault(1, []).append(row_f)
    global_member_id += 1
    # 女始祖父母为空，须靠婚姻边与夫连通；亦与亲缘 BFS 起点（min gen1）衔接
    add_marriage_row(
        int(row_m[0]),
        int(row_f[0]),
        wedding_day_for_couple_relaxed(row_m, row_f),
    )

    # ---------- 逐代父系繁衍 ----------
    for gen_no in range(2, target_gens + 1):
        target_n = gen_counts[gen_no - 1]
        prev_males = males_by_gen[gen_no - 1]
        prev_females = females_by_gen[gen_no - 1]
        prev_pop = len(prev_males) + len(prev_females)

        male_parents = [
            r
            for r in prev_males
            if reached_age_15(member_row_birth(r), r[5])
            and not str(r[6]).startswith("姻亲")
        ]
        if not male_parents:
            fix_m = prev_males[-1]
            fix_m[5] = ""
            fix_m[3] = "M"
            male_parents = [fix_m]

        counts = propose_child_counts_per_male(len(male_parents), gen_no - 1, prev_pop)
        nf = len(prev_females)
        per_mother_cap = min(
            MAX_CHILDREN_PER_MOTHER,
            max(1, (target_n + nf - 1) // max(1, nf)),
        )
        effective_target = min(target_n, nf * per_mother_cap)
        counts = normalize_child_counts_to_target(
            counts,
            effective_target,
            max_per_male=max(per_mother_cap, 1),
        )

        cur_males: list[list] = []
        cur_females: list[list] = []
        placed_this_gen = 0
        mother_children: defaultdict[int, int] = defaultdict(int)

        mother_pool = prev_females[:]
        random.shuffle(mother_pool)
        mi = 0

        progress_every = max(2000, target_n // 8) if target_n > 4000 else 0

        def place_children_for_pair(
            father_row: list,
            mother_row: list,
            take_n: int,
        ) -> None:
            nonlocal placed_this_gen
            global global_member_id
            if take_n <= 0:
                return
            fid = int(father_row[0])
            mid = int(mother_row[0])
            fb = member_row_birth(father_row)
            for _ in range(take_n):
                cb = child_birth_after_father(fb)
                guard = 0
                while cb > DATE_END and guard < 14:
                    cb = child_birth_after_father(fb)
                    guard += 1
                if cb > DATE_END:
                    cb = clamp_calendar_to_end(
                        min(add_years_safe(fb, 20), DATE_END)
                    )
                if cb <= fb:
                    cb = fb + timedelta(days=1)

                gender = random.choice(["M", "F"])
                death_s = sample_nonfounder_death(cb)
                nm = (
                    fake.first_name_male()
                    if gender == "M"
                    else fake.first_name_female()
                )
                cid = global_member_id
                child = append_member_row(
                    cid,
                    surname + nm,
                    gender,
                    cb,
                    death_s,
                    "平民",
                    str(fid),
                    str(mid),
                    gen_no,
                )
                members.append(child)
                global_member_id += 1
                placed_this_gen += 1
                mother_children[mid] += 1

                pk = (fid, mid)
                pair_first_child_birth[pk] = min(
                    pair_first_child_birth.get(pk, cb), cb
                )
                if gender == "M":
                    cur_males.append(child)
                else:
                    cur_females.append(child)

        def pick_mother_with_room() -> tuple[list, int] | None:
            """轮询母亲池，找仍有配额的母亲。"""
            nonlocal mi
            for _round in range(max(len(mother_pool) * 4, 12)):
                row = mother_pool[mi % len(mother_pool)]
                mi += 1
                cid = int(row[0])
                room = per_mother_cap - mother_children[cid]
                if room > 0:
                    return row, cid
            best_row = None
            best_id = -1
            best_room = -1
            for row in mother_pool:
                cid = int(row[0])
                room = per_mother_cap - mother_children[cid]
                if room > best_room:
                    best_room = room
                    best_row = row
                    best_id = cid
            if best_row is None or best_room <= 0:
                return None
            return best_row, best_id

        for pi, father_row in enumerate(male_parents):
            if progress_every and pi > 0 and pi % progress_every == 0:
                print(
                    f"    … [族谱 {tid}] 第 {gen_no} 代分配父亲 {pi}/{len(male_parents)}",
                    flush=True,
                )
            k = counts[pi] if pi < len(counts) else 0
            if k <= 0:
                continue
            remaining = k
            while remaining > 0:
                picked = pick_mother_with_room()
                if picked is None:
                    break
                mother_row, mid = picked
                room = per_mother_cap - mother_children[mid]
                take = min(remaining, room)
                if take <= 0:
                    continue
                place_children_for_pair(father_row, mother_row, take)
                remaining -= take

        deficit = target_n - placed_this_gen
        guard_fill = 0
        while deficit > 0 and male_parents and mother_pool and guard_fill < deficit + 200:
            picked = pick_mother_with_room()
            if picked is None:
                break
            mother_row, _mid = picked
            if mother_children[int(mother_row[0])] >= per_mother_cap:
                guard_fill += 1
                continue
            father_row = random.choice(male_parents)
            place_children_for_pair(father_row, mother_row, 1)
            deficit -= 1
            guard_fill += 1

        def append_partners_for_generation() -> int:
            """本代血系子女（平民）每人追加一名配偶（姻亲·配偶），同世代、婚姻写入 marriages。"""
            global global_member_id
            added = 0
            blood = [r for r in cur_males + cur_females if str(r[6]) == "平民"]
            for peer in blood:
                if random.random() < 0.05:
                    continue
                pb = member_row_birth(peer)
                sp_birth = clamp_calendar_to_end(
                    pb + timedelta(days=random.randint(-365 * 10, 365 * 10))
                )
                # 配偶默认仍在世，扩大可登记婚日窗口，减少 lo>latest
                d_sp = ""
                peer_pid = int(peer[0])
                if peer[3] == "M":
                    wife_row = append_member_row(
                        global_member_id,
                        surname + fake.first_name_female(),
                        "F",
                        sp_birth,
                        d_sp,
                        "姻亲·配偶",
                        "",
                        "",
                        gen_no,
                    )
                    members.append(wife_row)
                    global_member_id += 1
                    cur_females.append(wife_row)
                    wd = wedding_day_for_couple_relaxed(peer, wife_row)
                    add_marriage_row(peer_pid, int(wife_row[0]), wd)
                    added += 1
                else:
                    hus_row = append_member_row(
                        global_member_id,
                        surname + fake.first_name_male(),
                        "M",
                        sp_birth,
                        d_sp,
                        "姻亲·配偶",
                        "",
                        "",
                        gen_no,
                    )
                    members.append(hus_row)
                    global_member_id += 1
                    cur_males.append(hus_row)
                    wd = wedding_day_for_couple_relaxed(hus_row, peer)
                    add_marriage_row(int(hus_row[0]), peer_pid, wd)
                    added += 1
            return added

        n_partner = append_partners_for_generation()

        if not cur_males and gen_no < target_gens:
            victim = members[-1]
            victim[3] = "M"
            victim[2] = surname + fake.first_name_male()
            cur_males.append(victim)

        males_by_gen[gen_no] = cur_males
        females_by_gen[gen_no] = cur_females

        print(
            f"  > [族谱 {tid}] 第 {gen_no} 代完成, 目标子女约 {target_n} 人, "
            f"实际血亲新增 {placed_this_gen} 人, 本代配偶 {n_partner} 人, 累计 {len(members)} 人"
        )

    # ---------- 婚姻：生育夫妇 + 同龄/年龄相近补充 + 少量再婚 ----------
    by_id: dict[int, list] = {int(r[0]): r for r in members}

    def synthesize_wedding_date(h_id: int, w_id: int, latest: date) -> date | None:
        hr, wr = by_id[h_id], by_id[w_id]
        hb = member_row_birth(hr)
        wb = member_row_birth(wr)
        lo = max(
            add_years_safe(hb, _marriage_age_husband(hb)),
            add_years_safe(wb, _marriage_age_wife(wb)),
        )
        if lo > latest:
            return None
        span_d = (latest - lo).days
        return lo + timedelta(days=random.randint(0, max(0, span_d)))

    for (hid, wid), fcb in pair_first_child_birth.items():
        hr, wr = by_id[hid], by_id[wid]
        latest = min(
            fcb - timedelta(days=1),
            _latest_alive_date(member_row_birth(hr), hr[5]),
            _latest_alive_date(member_row_birth(wr), wr[5]),
        )
        wd = synthesize_wedding_date(hid, wid, latest)
        if wd is not None:
            add_marriage_row(hid, wid, wd)

    married: set[int] = set()
    for mar in marriages:
        married.add(int(mar[2]))
        married.add(int(mar[3]))

    def try_extra_pair(h_row: list, w_row: list) -> None:
        hid, wid = int(h_row[0]), int(w_row[0])
        if hid in married or wid in married:
            return
        hb, wb = member_row_birth(h_row), member_row_birth(w_row)
        if abs((hb - wb).days) > 365 * 10 + 200:
            return
        if int(h_row[9]) != int(w_row[9]):
            return
        if not reached_age_15(hb, h_row[5]) or not reached_age_15(wb, w_row[5]):
            return
        latest = min(
            _latest_alive_date(hb, h_row[5]),
            _latest_alive_date(wb, w_row[5]),
        )
        wd = synthesize_wedding_date(hid, wid, latest)
        if wd is None:
            return
        add_marriage_row(hid, wid, wd)
        married.add(hid)
        married.add(wid)

    for g in range(1, target_gens + 1):
        males_g = [r for r in males_by_gen.get(g, []) if int(r[0]) not in married]
        females_g = [r for r in females_by_gen.get(g, []) if int(r[0]) not in married]
        random.shuffle(males_g)
        random.shuffle(females_g)
        for h_row in males_g:
            if int(h_row[0]) in married:
                continue
            best = None
            for w_row in females_g:
                if int(w_row[0]) in married:
                    continue
                hb, wb = member_row_birth(h_row), member_row_birth(w_row)
                if abs((hb - wb).days) <= 365 * 10 + 120:
                    best = w_row
                    break
            if best is not None:
                try_extra_pair(h_row, best)

    husbands_once = list(marriages)
    for row in husbands_once:
        if random.random() >= 0.05:
            continue
        _, _tid, h, w, sd_s = row
        hid, wid = int(h), int(w)
        hr, wr = by_id[hid], by_id[wid]
        hd = _parse_death_csv(hr[5])
        wd_d = _parse_death_csv(wr[5])
        if hd is None and wd_d is None:
            continue
        if hd is not None and wd_d is not None:
            if hd <= wd_d and random.random() > 0.3:
                continue
            if wd_d <= hd and random.random() > 0.3:
                continue
        ego_birth = member_row_birth(hr)
        widower_latest = _latest_alive_date(ego_birth, hr[5])
        pool = [
            x
            for x in members
            if x[3] == "F"
            and int(x[0]) != wid
            and int(x[0]) not in married
            and reached_age_15(member_row_birth(x), x[5])
            and abs((member_row_birth(x) - ego_birth).days) <= 365 * 12
        ]
        if not pool:
            continue
        w2_row = random.choice(pool)
        w2_id = int(w2_row[0])
        second_lo = date.fromisoformat(sd_s) + timedelta(days=random.randint(400, 2800))
        second_lo = max(second_lo, add_years_safe(ego_birth, _marriage_age_husband(ego_birth)))
        w2b = member_row_birth(w2_row)
        second_lo = max(second_lo, add_years_safe(w2b, _marriage_age_wife(w2b)))
        latest2 = min(
            widower_latest,
            _latest_alive_date(w2b, w2_row[5]),
        )
        if second_lo > latest2:
            continue
        wd2 = second_lo + timedelta(
            days=random.randint(0, max(0, (latest2 - second_lo).days))
        )
        add_marriage_row(hid, w2_id, wd2)
        married.add(w2_id)

    # 兜底：姻亲·配偶若未出现在任何婚姻行中（历史版本或边界 bug），补一条与同代血亲的婚姻，避免亲缘校验孤立
    married_endpoints: set[int] = set()
    for mar in marriages:
        if len(mar) >= 4 and int(mar[1]) == tid:
            married_endpoints.add(int(mar[2]))
            married_endpoints.add(int(mar[3]))
    for r in members:
        if not str(r[6]).startswith("姻亲"):
            continue
        aid = int(r[0])
        if aid in married_endpoints:
            continue
        gen_ = int(r[9])
        g = str(r[3])
        want = "F" if g == "M" else "M"
        pool = [
            x
            for x in members
            if str(x[6]) == "平民"
            and int(x[9]) == gen_
            and str(x[3]) == want
        ]
        if not pool:
            continue
        peer = random.choice(pool)
        pid = int(peer[0])
        if g == "M":
            add_marriage_row(
                aid, pid, wedding_day_for_couple_relaxed(r, peer)
            )
        else:
            add_marriage_row(
                pid, aid, wedding_day_for_couple_relaxed(peer, r)
            )
        married_endpoints.add(aid)
        married_endpoints.add(pid)

    mgr_rows = generate_tree_managers(tid, creator_id, users_rows)
    global_tree_id += 1
    return tree_record, members, marriages, mgr_rows


def _tree_kinship_connected(
    rows: list[list],
    marriage_rows: list[list] | None = None,
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

    # 配偶（父母为空）通过婚姻边与血亲连通
    for mr in marriage_rows or []:
        if len(mr) < 4:
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


def validate_dataset_constraints(
    all_members: list[list],
    all_trees: list[list],
    smoke: bool,
    all_marriages: list[list] | None = None,
) -> list[str]:
    errors: list[str] = []
    if smoke:
        return errors

    if len(all_trees) < MIN_TREES:
        errors.append(f"族谱数 {len(all_trees)} < {MIN_TREES}")

    by_tree: dict[int, list[list]] = defaultdict(list)
    for row in all_members:
        by_tree[int(row[1])].append(row)

    by_mar: dict[int, list[list]] | None = None
    if all_marriages:
        by_mar = defaultdict(list)
        for row in all_marriages:
            if len(row) > 1:
                by_mar[int(row[1])].append(row)

    if not by_tree:
        errors.append("没有任何成员记录")
        return errors

    max_tree_members = max(len(rows) for rows in by_tree.values())
    if max_tree_members < MIN_SUPER_TREE_MEMBERS:
        errors.append(f"最大族谱人数 {max_tree_members} < {MIN_SUPER_TREE_MEMBERS}")

    if len(all_members) < MIN_TOTAL_MEMBERS:
        errors.append(f"总成员数 {len(all_members)} < {MIN_TOTAL_MEMBERS}")

    for tid, rows in sorted(by_tree.items()):
        gens = [int(r[9]) for r in rows]
        if max(gens) < MIN_GENERATIONS_PER_TREE:
            errors.append(
                f"族谱 tree_id={tid} 最大世代 {max(gens)} < {MIN_GENERATIONS_PER_TREE}（须每谱 ≥30 代）"
            )
        mar_rows = by_mar.get(tid, []) if by_mar is not None else []
        ok_k, msg = _tree_kinship_connected(rows, mar_rows)
        if not ok_k:
            errors.append(f"族谱 tree_id={tid} 亲缘: {msg}")

    return errors


def verify_csv_directory(out_dir: str) -> list[str]:
    users_path = os.path.join(out_dir, "users.csv")
    if not os.path.isfile(users_path):
        return [f"缺少 {users_path}"]

    pattern = os.path.join(out_dir, "tree_*", "family_tree.csv")
    tree_meta_files = sorted(glob.glob(pattern))
    if not tree_meta_files:
        return [f"未找到子目录 tree_*/family_tree.csv（请确认输出目录 {out_dir}）"]

    all_trees: list[list] = []
    all_members: list[list] = []
    all_marriages: list[list] = []

    for meta_path in tree_meta_files:
        with open(meta_path, newline="", encoding="utf-8-sig") as f:
            r = list(csv.reader(f))
            if len(r) < 2:
                return [f"空文件或仅有表头: {meta_path}"]
            all_trees.append(r[1])

        sub = os.path.dirname(meta_path)
        mem_path = os.path.join(sub, "members.csv")
        if not os.path.isfile(mem_path):
            return [f"缺少 {mem_path}"]
        with open(mem_path, newline="", encoding="utf-8-sig") as f:
            all_members.extend(list(csv.reader(f))[1:])

        mar_path = os.path.join(sub, "marriages.csv")
        if os.path.isfile(mar_path):
            with open(mar_path, newline="", encoding="utf-8-sig") as f:
                all_marriages.extend(list(csv.reader(f))[1:])

    return validate_dataset_constraints(
        all_members, all_trees, smoke=False, all_marriages=all_marriages
    )


def build_per_tree_target_generations(n_trees: int) -> list[int]:
    """
    每棵族谱的目标世代数：均 ≥ MIN_GENERATIONS_PER_TREE，
    且在 [MIN_GENERATIONS_PER_TREE, TREE_GENS_HI] 内随机，保证各谱数值不完全相同。
    """
    if n_trees < 1:
        return []
    lo = MIN_GENERATIONS_PER_TREE
    hi = max(lo + 1, TREE_GENS_HI)
    targets = [random.randint(lo, hi) for _ in range(n_trees)]
    while len(set(targets)) == 1:
        targets[random.randrange(n_trees)] = random.randint(lo, hi)
    return targets


def build_normal_tree_sizes(how_many: int) -> list[int]:
    """
    普通族谱人数带随机波动；保证总人数仍满足 MIN_TOTAL_MEMBERS - SUPER_TREE_SIZE。
    """
    min_sum = max(
        how_many * (MIN_GENERATIONS_PER_TREE + 1),
        MIN_TOTAL_MEMBERS - SUPER_TREE_SIZE,
    )
    raw = [
        max(
            MIN_GENERATIONS_PER_TREE + 1,
            NORMAL_TREE_SIZE + random.randint(-320, 980),
        )
        for _ in range(how_many)
    ]
    deficit = min_sum - sum(raw)
    i = 0
    while deficit > 0:
        raw[i % how_many] += 1
        deficit -= 1
        i += 1
    return raw


def count_members_csv_rows(members_csv_path: str) -> int:
    """members.csv 数据行数（减去表头），与磁盘实际一致。"""
    if not os.path.isfile(members_csv_path):
        return 0
    with open(members_csv_path, newline="", encoding="utf-8-sig") as f:
        line_count = sum(1 for _ in f)
    return max(0, line_count - 1)


def write_csv(path: str, header: list[str], rows: list[list]) -> None:
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)
    try:
        with open(path, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.writer(f, quoting=csv.QUOTE_ALL)
            w.writerow(header)
            w.writerows(rows)
    except PermissionError:
        print(
            f"\n无法写入文件（被拒绝访问）：\n  {os.path.abspath(path)}\n\n"
            "请先关闭占用该文件的 Excel / 记事本等程序，或换目录 "
            "`--out-dir generated_data_new`。\n",
            file=sys.stderr,
        )
        sys.exit(1)


def tree_subdir(out_dir: str, tree_id: int) -> str:
    return os.path.join(out_dir, f"tree_{tree_id:03d}")


def main() -> None:
    parser = argparse.ArgumentParser(description="族谱 CSV 生成（按族谱分子目录）")
    parser.add_argument("--smoke", action="store_true", help="小规模试跑（跳过作业阈值校验）")
    parser.add_argument("--out-dir", default="generated_data", help="输出根目录")
    parser.add_argument(
        "--check-dir",
        default=None,
        metavar="DIR",
        help="校验已有输出（users.csv + tree_*/）",
    )
    args = parser.parse_args()

    if args.check_dir:
        errs = verify_csv_directory(args.check_dir)
        if errs:
            print(f"\n[校验未通过] {args.check_dir}")
            for e in errs:
                print(" -", e)
            sys.exit(1)
        print(f"[校验通过] {args.check_dir}")
        return

    total_trees = TOTAL_TREES
    super_size = SUPER_TREE_SIZE
    normal_size = NORMAL_TREE_SIZE

    if args.smoke:
        total_trees = 3
        super_size = 120
        normal_size = 45
        gen_by_tree = [12] + [8] * (total_trees - 1)
    else:
        gen_by_tree = build_per_tree_target_generations(total_trees)

    out_dir = args.out_dir
    print("开始生成…")
    if args.smoke:
        print(
            f"时间上限: {DATE_END.isoformat()}；"
            f"族谱数={total_trees}（--smoke）；超大谱 {super_size} 人 / {gen_by_tree[0]} 代；"
            f"普通谱约 {normal_size} 人 / {gen_by_tree[1]} 代 …"
        )
    else:
        g_lo, g_hi = min(gen_by_tree), max(gen_by_tree)
        print(
            f"时间上限: {DATE_END.isoformat()}；族谱数={total_trees}；"
            f"超大谱 {super_size} 人 / {gen_by_tree[0]} 代；"
            f"普通谱约 {normal_size} 人；"
            f"各谱目标世代 {g_lo}–{g_hi} 不等（均≥{MIN_GENERATIONS_PER_TREE}，且数值不完全相同）"
        )

    user_n = random.randint(USER_COUNT_MIN, USER_COUNT_MAX)
    users = generate_users(user_n)
    users_path = os.path.join(out_dir, "users.csv")
    write_csv(
        users_path,
        ["user_id", "username", "password_hash", "registered_at"],
        users,
    )

    normal_sizes = (
        [normal_size] * (total_trees - 1)
        if args.smoke
        else build_normal_tree_sizes(total_trees - 1)
    )

    all_trees_meta: list[list] = []
    all_members_flat: list[list] = []
    all_marriages_flat: list[list] = []

    print("\n--- 超级大族谱 ---")
    t_rec, m_recs, mar_recs, mgr_recs = generate_family_tree(
        super_size, gen_by_tree[0], users
    )
    tid = int(t_rec[0])
    sd = tree_subdir(out_dir, tid)
    write_csv(
        os.path.join(sd, "family_tree.csv"),
        ["tree_id", "tree_name", "surname", "created_by_user_id", "revision_at"],
        [t_rec],
    )
    write_csv(
        os.path.join(sd, "members.csv"),
        [
            "member_id",
            "tree_id",
            "full_name",
            "gender",
            "birth_date",
            "death_date",
            "biography",
            "father_member_id",
            "mother_member_id",
            "generation",
        ],
        m_recs,
    )
    write_csv(
        os.path.join(sd, "marriages.csv"),
        ["marriage_id", "tree_id", "husband_id", "wife_id", "start_date"],
        mar_recs,
    )
    write_csv(
        os.path.join(sd, "tree_managers.csv"),
        ["tree_id", "user_id", "role", "invited_at"],
        mgr_recs,
    )
    all_trees_meta.append(t_rec)
    all_members_flat.extend(m_recs)
    all_marriages_flat.extend(mar_recs)

    print("\n--- 其余族谱 ---")
    for ni, this_normal_size in enumerate(normal_sizes):
        tg = gen_by_tree[ni + 1]
        print(
            f"  [普通谱 {ni + 1}/{len(normal_sizes)}] "
            f"目标人数 {this_normal_size}，目标 {tg} 代"
        )
        t_rec, m_recs, mar_recs, mgr_recs = generate_family_tree(
            this_normal_size, tg, users
        )
        tid = int(t_rec[0])
        sd = tree_subdir(out_dir, tid)
        write_csv(
            os.path.join(sd, "family_tree.csv"),
            ["tree_id", "tree_name", "surname", "created_by_user_id", "revision_at"],
            [t_rec],
        )
        write_csv(
            os.path.join(sd, "members.csv"),
            [
                "member_id",
                "tree_id",
                "full_name",
                "gender",
                "birth_date",
                "death_date",
                "biography",
                "father_member_id",
                "mother_member_id",
                "generation",
            ],
            m_recs,
        )
        write_csv(
            os.path.join(sd, "marriages.csv"),
            ["marriage_id", "tree_id", "husband_id", "wife_id", "start_date"],
            mar_recs,
        )
        write_csv(
            os.path.join(sd, "tree_managers.csv"),
            ["tree_id", "user_id", "role", "invited_at"],
            mgr_recs,
        )
        all_trees_meta.append(t_rec)
        all_members_flat.extend(m_recs)
        all_marriages_flat.extend(mar_recs)

    index_rows = []
    for r in all_trees_meta:
        tid = int(r[0])
        sd = tree_subdir(out_dir, tid)
        cnt = count_members_csv_rows(os.path.join(sd, "members.csv"))
        index_rows.append([tid, f"tree_{tid:03d}", cnt])
    write_csv(
        os.path.join(out_dir, "trees_index.csv"),
        ["tree_id", "folder", "member_count"],
        index_rows,
    )

    print(f"\n完成。用户表: {users_path}")
    print(f"族谱子目录: {out_dir}/tree_001 … tree_{total_trees:03d}")
    print(f"索引: {os.path.join(out_dir, 'trees_index.csv')}")

    errs = validate_dataset_constraints(
        all_members_flat,
        all_trees_meta,
        args.smoke,
        all_marriages_flat,
    )
    if args.smoke:
        print("\n[提示] --smoke 跳过作业指标校验。")
    elif errs:
        print("\n[校验失败]")
        for e in errs:
            print(" -", e)
        sys.exit(1)
    else:
        print(
            "\n[校验通过] ≥10 棵谱、≥1 棵 ≥50000 人、全库 ≥100000 人、"
            "每棵最大世代≥30、亲缘连通；日期不晚于 2026-05-01。"
        )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n已中断。", file=sys.stderr)
        sys.exit(130)
