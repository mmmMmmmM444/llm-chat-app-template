#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
tw_eod_scan.py  —  台股盤後動能選股掃描（上市 + 上櫃）

資料來源（官方公開 JSON，免金鑰）：
  TWSE 上市  每日收盤行情  MI_INDEX
  TWSE 上市  三大法人買賣超 T86
  TWSE 上市  融資融券餘額  MI_MARGN
  TPEx 上櫃  每日收盤行情 / 三大法人 / 融資融券（新版 www API，失敗時退回舊版 web API）

流程：
  1. 以本機 SQLite 快取（預設 ./data/tw_eod.sqlite）為主，只補抓缺少的交易日。
  2. 計算量價 / 技術 / 籌碼指標，依策略權重打分數並排名。
  3. 輸出 終端表格 + CSV + JSON + Markdown（預設 ./out/）。

用法：
  python tw_eod_scan.py                      # 抓最近 60 個交易日，跑 momentum 策略
  python tw_eod_scan.py --date 20260904      # 指定基準日
  python tw_eod_scan.py --strategy breakout  # momentum | breakout | inst | pullback
  python tw_eod_scan.py --offline            # 只用快取不連網
  python tw_eod_scan.py --selftest           # 離線合成資料自我測試（不連網）
  python tw_eod_scan.py --watch 2330,2454    # 觀察名單一律列出

只依賴 Python 3.9+ 標準庫。
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import math
import os
import random
import re
import sqlite3
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from typing import Callable, Dict, Iterable, List, Optional, Sequence, Tuple

# --------------------------------------------------------------------------- #
# 設定
# --------------------------------------------------------------------------- #

TZ_TAIPEI = dt.timezone(dt.timedelta(hours=8))
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)
REQUEST_GAP_SEC = 3.0        # 交易所會封鎖連續快速請求，每次請求間隔
REQUEST_TIMEOUT_SEC = 40
REQUEST_RETRIES = 3

# 每個資料集可有多個候選 URL（新版優先，舊版備援）。{ymd}=YYYYMMDD, {slash}=YYYY/MM/DD, {roc}=民國 yyy/MM/DD
ENDPOINTS: Dict[str, List[str]] = {
    "twse_quotes": [
        "https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX?date={ymd}&type=ALLBUT0999&response=json",
        "https://www.twse.com.tw/exchangeReport/MI_INDEX?response=json&date={ymd}&type=ALLBUT0999",
    ],
    "twse_inst": [
        "https://www.twse.com.tw/rwd/zh/fund/T86?date={ymd}&selectType=ALLBUT0999&response=json",
        "https://www.twse.com.tw/fund/T86?response=json&date={ymd}&selectType=ALLBUT0999",
    ],
    "twse_margin": [
        "https://www.twse.com.tw/rwd/zh/marginTrading/MI_MARGN?date={ymd}&selectType=ALL&response=json",
        "https://www.twse.com.tw/exchangeReport/MI_MARGN?response=json&date={ymd}&selectType=ALL",
    ],
    "tpex_quotes": [
        "https://www.tpex.org.tw/www/zh-tw/afterTrading/otc?date={slash}&id=&response=json",
        "https://www.tpex.org.tw/www/zh-tw/afterTrading/dailyQuotes?date={slash}&id=&response=json",
        "https://www.tpex.org.tw/web/stock/aftertrading/otc_quotes_no1430/stk_wn1430_result.php?l=zh-tw&d={roc}&se=EW&o=json",
    ],
    "tpex_inst": [
        "https://www.tpex.org.tw/www/zh-tw/insti/dailyTrade?type=Daily&sect=EW&date={slash}&response=json",
        "https://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_hedge_result.php?l=zh-tw&se=EW&t=D&d={roc}&o=json",
    ],
    "tpex_margin": [
        "https://www.tpex.org.tw/www/zh-tw/margin/balance?date={slash}&response=json",
        "https://www.tpex.org.tw/web/stock/margin_trading/margin_balance/margin_bal_result.php?l=zh-tw&d={roc}&o=json",
    ],
}

STRATEGIES = {
    "momentum": "綜合動能：突破 + 量增 + 多頭排列 + 法人買超（預設）",
    "breakout": "突破策略：收盤創 20 日新高且量增 1.5 倍以上",
    "inst":     "法人策略：投信連買 2 日以上 或 外資連買 3 日以上，且當日三大法人淨買",
    "pullback": "多頭回測：多頭排列中量縮回測 MA10/MA20（±3%）",
}

# --------------------------------------------------------------------------- #
# 小工具
# --------------------------------------------------------------------------- #

def log(msg: str) -> None:
    ts = dt.datetime.now(TZ_TAIPEI).strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", file=sys.stderr, flush=True)


def to_num(s) -> Optional[float]:
    """交易所數字字串 -> float。逗號、全形符號、'--'、'---'、空白 皆處理。"""
    if s is None:
        return None
    if isinstance(s, (int, float)):
        return float(s)
    t = str(s).strip()
    t = re.sub(r"<[^>]+>", "", t)  # 去除 <p style=...> 之類 HTML
    t = t.replace(",", "").replace("＋", "+").replace("－", "-").replace("　", "").strip()
    if t in ("", "--", "---", "----", "X", "除權", "除息", "除權息", "NA", "N/A", "null"):
        return None
    try:
        return float(t)
    except ValueError:
        m = re.search(r"-?\d+(?:\.\d+)?", t)
        return float(m.group(0)) if m else None


def sign_from_html(s) -> int:
    """TWSE 漲跌(+/-) 欄位可能是 '<p style= color:green>-</p>' 或 '+'。"""
    t = re.sub(r"<[^>]+>", "", str(s or "")).strip()
    if "+" in t or "＋" in t:
        return 1
    if "-" in t or "－" in t:
        return -1
    return 0


def ymd(d: dt.date) -> str:
    return d.strftime("%Y%m%d")


def fmt_urls(key: str, d: dt.date) -> List[str]:
    ctx = {
        "ymd": ymd(d),
        "slash": d.strftime("%Y/%m/%d"),
        "roc": f"{d.year - 1911}/{d.month:02d}/{d.day:02d}",
    }
    return [u.format(**ctx) for u in ENDPOINTS[key]]


def is_common_stock(code: str) -> bool:
    """只留一般股票（4 碼數字）。排除 ETF/ETN(00xx)、權證(6碼)、特別股(含英文)、TDR 等。"""
    if not re.fullmatch(r"\d{4}", code):
        return False
    if code.startswith("00"):
        return False
    return True


def find_col(fields: Sequence[str], *needles: str, exclude: Iterable[str] = (), nth: int = 0) -> Optional[int]:
    """在表頭中找第 nth 個「包含所有 needles、且不含 exclude」的欄位索引。"""
    hits = []
    for i, f in enumerate(fields):
        f2 = str(f).replace(" ", "")
        if all(n in f2 for n in needles) and not any(x in f2 for x in exclude):
            hits.append(i)
    return hits[nth] if len(hits) > nth else None


# --------------------------------------------------------------------------- #
# HTTP
# --------------------------------------------------------------------------- #

class Http:
    def __init__(self, gap: float = REQUEST_GAP_SEC):
        self.gap = gap
        self._last = 0.0

    def get_json(self, url: str) -> Optional[dict]:
        wait = self.gap - (time.time() - self._last)
        if wait > 0:
            time.sleep(wait)
        last_err: Optional[Exception] = None
        for attempt in range(REQUEST_RETRIES):
            try:
                req = urllib.request.Request(url, headers={
                    "User-Agent": USER_AGENT,
                    "Accept": "application/json,text/plain,*/*",
                    "Referer": "https://www.tpex.org.tw/" if "tpex" in url else "https://www.twse.com.tw/",
                })
                with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT_SEC) as resp:
                    raw = resp.read()
                self._last = time.time()
                text = raw.decode("utf-8", errors="replace").strip()
                if not text:
                    return None
                return json.loads(text)
            except (urllib.error.URLError, urllib.error.HTTPError, json.JSONDecodeError, TimeoutError, OSError) as e:
                last_err = e
                self._last = time.time()
                time.sleep(2.0 * (attempt + 1))
        log(f"  ✗ 請求失敗 {url} : {last_err}")
        return None


# --------------------------------------------------------------------------- #
# 回應解析：把 TWSE/TPEx 各種 JSON 形狀統一成 (fields, rows)
# --------------------------------------------------------------------------- #

def extract_tables(payload: dict) -> List[Tuple[List[str], List[list]]]:
    """回傳 payload 內所有 (fields, rows) 表格。支援：
       - 新版 {"tables":[{"fields":[],"data":[]}]}
       - 舊版 TWSE {"fields":[], "data":[]} / {"fields1":[], "data1":[]} ...
       - 舊版 TPEx {"aaData":[[...]]}（無表頭 -> fields=[]）
    """
    out: List[Tuple[List[str], List[list]]] = []
    if not isinstance(payload, dict):
        return out
    if isinstance(payload.get("tables"), list):
        for t in payload["tables"]:
            if isinstance(t, dict) and isinstance(t.get("data"), list) and t["data"]:
                out.append((list(t.get("fields") or []), t["data"]))
    for k, v in payload.items():
        m = re.fullmatch(r"data(\d*)", k)
        if m and isinstance(v, list) and v:
            fields = payload.get("fields" + m.group(1)) or []
            out.append((list(fields), v))
    if isinstance(payload.get("aaData"), list) and payload["aaData"]:
        out.append(([], payload["aaData"]))
    return out


def pick_table(tables, must_have: Sequence[str], min_cols: int = 0):
    """挑出表頭同時包含 must_have 所有關鍵字的表；無表頭時回傳欄數最多者。"""
    best = None
    for fields, rows in tables:
        if fields:
            joined = "".join(str(f) for f in fields)
            if all(k in joined for k in must_have) and len(fields) >= min_cols:
                return fields, rows
        else:
            if rows and len(rows[0]) >= min_cols:
                if best is None or len(rows[0]) > len(best[1][0]):
                    best = (fields, rows)
    return best


# --------------------------------------------------------------------------- #
# 資料模型
# --------------------------------------------------------------------------- #

@dataclass
class Quote:
    date: str
    code: str
    name: str
    market: str          # TWSE / TPEX
    open: Optional[float]
    high: Optional[float]
    low: Optional[float]
    close: Optional[float]
    volume: float        # 張
    amount: float        # 元
    txns: float
    change: Optional[float] = None   # 漲跌價差（含正負）


@dataclass
class Inst:
    date: str
    code: str
    foreign_net: float   # 張
    trust_net: float
    dealer_net: float
    total_net: float


@dataclass
class Margin:
    date: str
    code: str
    margin_prev: Optional[float]   # 張
    margin_bal: Optional[float]
    short_prev: Optional[float]
    short_bal: Optional[float]


# --------------------------------------------------------------------------- #
# 各資料集解析
# --------------------------------------------------------------------------- #

def parse_twse_quotes(payload: dict, date: str) -> List[Quote]:
    tables = extract_tables(payload)
    t = pick_table(tables, ["證券代號", "收盤價"], min_cols=9)
    if not t:
        return []
    fields, rows = t
    c_code = find_col(fields, "證券代號"); c_name = find_col(fields, "證券名稱")
    c_vol = find_col(fields, "成交股數"); c_amt = find_col(fields, "成交金額"); c_tx = find_col(fields, "成交筆數")
    c_o = find_col(fields, "開盤價"); c_h = find_col(fields, "最高價"); c_l = find_col(fields, "最低價"); c_c = find_col(fields, "收盤價")
    c_sign = find_col(fields, "漲跌(+/-)"); c_diff = find_col(fields, "漲跌價差")
    out = []
    for r in rows:
        try:
            code = str(r[c_code]).strip()
        except (IndexError, TypeError):
            continue
        if not is_common_stock(code):
            continue
        close = to_num(r[c_c])
        if close is None:
            continue
        vol = to_num(r[c_vol]) or 0.0
        chg = None
        if c_diff is not None:
            d = to_num(r[c_diff])
            if d is not None:
                chg = d * (sign_from_html(r[c_sign]) if c_sign is not None else 1)
        out.append(Quote(date, code, str(r[c_name]).strip(), "TWSE",
                         to_num(r[c_o]), to_num(r[c_h]), to_num(r[c_l]), close,
                         vol / 1000.0, to_num(r[c_amt]) or 0.0, to_num(r[c_tx]) or 0.0, chg))
    return out


TPEX_QUOTE_LEGACY_ORDER = ["代號", "名稱", "收盤", "漲跌", "開盤", "最高", "最低", "成交股數", "成交金額", "成交筆數"]


def parse_tpex_quotes(payload: dict, date: str) -> List[Quote]:
    tables = extract_tables(payload)
    t = pick_table(tables, ["代號", "收盤"], min_cols=10)
    if not t:
        return []
    fields, rows = t
    if not fields:
        fields = TPEX_QUOTE_LEGACY_ORDER + [""] * (len(rows[0]) - len(TPEX_QUOTE_LEGACY_ORDER))
    c_code = find_col(fields, "代號"); c_name = find_col(fields, "名稱")
    c_c = find_col(fields, "收盤"); c_chg = find_col(fields, "漲跌", exclude=("幅",))
    c_o = find_col(fields, "開盤"); c_h = find_col(fields, "最高"); c_l = find_col(fields, "最低")
    c_vol = find_col(fields, "成交股數"); c_amt = find_col(fields, "成交金額"); c_tx = find_col(fields, "成交筆數")
    out = []
    for r in rows:
        try:
            code = str(r[c_code]).strip()
        except (IndexError, TypeError):
            continue
        if not is_common_stock(code):
            continue
        close = to_num(r[c_c])
        if close is None:
            continue
        vol = to_num(r[c_vol]) or 0.0
        out.append(Quote(date, code, str(r[c_name]).strip(), "TPEX",
                         to_num(r[c_o]), to_num(r[c_h]), to_num(r[c_l]), close,
                         vol / 1000.0, to_num(r[c_amt]) or 0.0, to_num(r[c_tx]) or 0.0,
                         to_num(r[c_chg]) if c_chg is not None else None))
    return out


def _parse_inst_generic(payload: dict, date: str, legacy_cols: Optional[Dict[str, int]]) -> List[Inst]:
    tables = extract_tables(payload)
    t = pick_table(tables, ["代號", "買賣超"], min_cols=10)
    if not t:
        return []
    fields, rows = t
    if fields:
        net_cols = [i for i, f in enumerate(fields) if "買賣超" in str(f)]
        c_code = find_col(fields, "代號")
        c_for = next((i for i in net_cols if ("外資" in fields[i] or "外陸資" in fields[i])
                      and "外資自營商" not in fields[i].replace("不含外資自營商", "")), None)
        if c_for is None:
            c_for = next((i for i in net_cols if "外資" in fields[i] or "外陸資" in fields[i]), None)
        c_tru = next((i for i in net_cols if "投信" in fields[i]), None)
        dealer_cands = [i for i in net_cols if "自營商" in fields[i] and "外資" not in fields[i]]
        # 自營商合計通常是「自營商買賣超股數」（不含 自行/避險 字樣）；否則取最後一個
        c_dea = next((i for i in dealer_cands if "自行" not in fields[i] and "避險" not in fields[i]), None)
        if c_dea is None and dealer_cands:
            c_dea = dealer_cands[-1]
        c_tot = next((i for i in net_cols if "三大法人" in fields[i] or "合計" in fields[i]), None)
        if c_tot is None and net_cols:
            c_tot = net_cols[-1]
    else:
        if not legacy_cols:
            return []
        c_code, c_for, c_tru, c_dea, c_tot = (legacy_cols["code"], legacy_cols["foreign"],
                                              legacy_cols["trust"], legacy_cols["dealer"], legacy_cols["total"])
    if c_code is None:
        return []
    out = []
    for r in rows:
        try:
            code = str(r[c_code]).strip()
        except (IndexError, TypeError):
            continue
        if not is_common_stock(code):
            continue
        g = lambda c: (to_num(r[c]) or 0.0) / 1000.0 if c is not None and c < len(r) else 0.0
        f, tr, de = g(c_for), g(c_tru), g(c_dea)
        tot = g(c_tot) if c_tot is not None else f + tr + de
        out.append(Inst(date, code, f, tr, de, tot))
    return out


def parse_twse_inst(payload: dict, date: str) -> List[Inst]:
    return _parse_inst_generic(payload, date, None)


def parse_tpex_inst(payload: dict, date: str) -> List[Inst]:
    # 舊版 aaData 順序（24 欄）：0代號 1名稱 2-4外資(不含自營) 5-7外資自營 8-10外資合計 11-13投信 14-16自營自行 17-19自營避險 20-22自營合計 23總計
    legacy = {"code": 0, "foreign": 10, "trust": 13, "dealer": 22, "total": 23}
    return _parse_inst_generic(payload, date, legacy)


def parse_twse_margin(payload: dict, date: str) -> List[Margin]:
    tables = extract_tables(payload)
    t = None
    for fields, rows in tables:
        if fields and any("代號" in str(f) for f in fields) and len(fields) >= 14:
            t = (fields, rows); break
    if not t:
        return []
    fields, rows = t
    c_code = find_col(fields, "代號")
    prev_cols = [i for i, f in enumerate(fields) if "前日餘額" in str(f)]
    today_cols = [i for i, f in enumerate(fields) if "今日餘額" in str(f)]
    if len(prev_cols) < 2 or len(today_cols) < 2:
        return []
    out = []
    for r in rows:
        try:
            code = str(r[c_code]).strip()
        except (IndexError, TypeError):
            continue
        if not is_common_stock(code):
            continue
        out.append(Margin(date, code, to_num(r[prev_cols[0]]), to_num(r[today_cols[0]]),
                          to_num(r[prev_cols[1]]), to_num(r[today_cols[1]])))
    return out


TPEX_MARGIN_LEGACY_ORDER = ["代號", "名稱", "前資餘額(張)", "資買", "資賣", "現償", "資餘額", "資屬證金", "資使用率(%)", "資限額",
                            "前券餘額(張)", "券賣", "券買", "券償", "券餘額", "券屬證金", "券使用率(%)", "券限額", "資券相抵(張)", "備註"]


def parse_tpex_margin(payload: dict, date: str) -> List[Margin]:
    tables = extract_tables(payload)
    t = pick_table(tables, ["代號", "資餘額"], min_cols=15)
    if not t:
        return []
    fields, rows = t
    if not fields:
        fields = TPEX_MARGIN_LEGACY_ORDER + [""] * max(0, len(rows[0]) - len(TPEX_MARGIN_LEGACY_ORDER))
    c_code = find_col(fields, "代號")
    c_mp = find_col(fields, "前資餘額"); c_mb = find_col(fields, "資餘額", exclude=("前",))
    c_sp = find_col(fields, "前券餘額"); c_sb = find_col(fields, "券餘額", exclude=("前",))
    if None in (c_code, c_mp, c_mb):
        return []
    out = []
    for r in rows:
        try:
            code = str(r[c_code]).strip()
        except (IndexError, TypeError):
            continue
        if not is_common_stock(code):
            continue
        out.append(Margin(date, code, to_num(r[c_mp]), to_num(r[c_mb]),
                          to_num(r[c_sp]) if c_sp is not None else None,
                          to_num(r[c_sb]) if c_sb is not None else None))
    return out


PARSERS: Dict[str, Callable[[dict, str], list]] = {
    "twse_quotes": parse_twse_quotes, "tpex_quotes": parse_tpex_quotes,
    "twse_inst": parse_twse_inst, "tpex_inst": parse_tpex_inst,
    "twse_margin": parse_twse_margin, "tpex_margin": parse_tpex_margin,
}


# --------------------------------------------------------------------------- #
# SQLite 快取
# --------------------------------------------------------------------------- #

SCHEMA = """
CREATE TABLE IF NOT EXISTS quotes(
  date TEXT, code TEXT, name TEXT, market TEXT,
  open REAL, high REAL, low REAL, close REAL, volume REAL, amount REAL, txns REAL, change REAL,
  PRIMARY KEY(date, code));
CREATE TABLE IF NOT EXISTS inst(
  date TEXT, code TEXT, foreign_net REAL, trust_net REAL, dealer_net REAL, total_net REAL,
  PRIMARY KEY(date, code));
CREATE TABLE IF NOT EXISTS margin(
  date TEXT, code TEXT, margin_prev REAL, margin_bal REAL, short_prev REAL, short_bal REAL,
  PRIMARY KEY(date, code));
-- 抓取紀錄：status = ok | empty(休市/尚未公布) | fail
CREATE TABLE IF NOT EXISTS fetch_log(
  dataset TEXT, date TEXT, status TEXT, rows INTEGER, url TEXT, fetched_at TEXT,
  PRIMARY KEY(dataset, date));
CREATE INDEX IF NOT EXISTS idx_quotes_code ON quotes(code, date);
CREATE INDEX IF NOT EXISTS idx_inst_code ON inst(code, date);
CREATE INDEX IF NOT EXISTS idx_margin_code ON margin(code, date);
"""


class Store:
    def __init__(self, path: str):
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        self.conn = sqlite3.connect(path)
        self.conn.executescript(SCHEMA)

    def fetch_status(self, dataset: str, date: str) -> Optional[str]:
        row = self.conn.execute("SELECT status FROM fetch_log WHERE dataset=? AND date=?", (dataset, date)).fetchone()
        return row[0] if row else None

    def mark(self, dataset: str, date: str, status: str, rows: int, url: str) -> None:
        self.conn.execute("INSERT OR REPLACE INTO fetch_log VALUES(?,?,?,?,?,?)",
                          (dataset, date, status, rows, url, dt.datetime.now(TZ_TAIPEI).isoformat(timespec="seconds")))
        self.conn.commit()

    def save(self, dataset: str, items: list) -> None:
        if not items:
            return
        kind = dataset.split("_")[1]
        if kind == "quotes":
            self.conn.executemany(
                "INSERT OR REPLACE INTO quotes VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                [(q.date, q.code, q.name, q.market, q.open, q.high, q.low, q.close, q.volume, q.amount, q.txns, q.change) for q in items])
        elif kind == "inst":
            self.conn.executemany("INSERT OR REPLACE INTO inst VALUES(?,?,?,?,?,?)",
                                  [(i.date, i.code, i.foreign_net, i.trust_net, i.dealer_net, i.total_net) for i in items])
        elif kind == "margin":
            self.conn.executemany("INSERT OR REPLACE INTO margin VALUES(?,?,?,?,?,?)",
                                  [(m.date, m.code, m.margin_prev, m.margin_bal, m.short_prev, m.short_bal) for m in items])
        self.conn.commit()

    def trading_dates(self) -> List[str]:
        return [r[0] for r in self.conn.execute("SELECT DISTINCT date FROM quotes ORDER BY date")]

    def load_quotes(self, dates: Sequence[str]) -> Dict[str, List[Quote]]:
        if not dates:
            return {}
        qs = ",".join("?" * len(dates))
        by_code: Dict[str, List[Quote]] = {}
        for r in self.conn.execute(f"SELECT * FROM quotes WHERE date IN ({qs}) ORDER BY code, date", tuple(dates)):
            by_code.setdefault(r[1], []).append(Quote(*r))
        return by_code

    def load_inst(self, dates: Sequence[str]) -> Dict[str, Dict[str, Inst]]:
        if not dates:
            return {}
        qs = ",".join("?" * len(dates))
        out: Dict[str, Dict[str, Inst]] = {}
        for r in self.conn.execute(f"SELECT * FROM inst WHERE date IN ({qs})", tuple(dates)):
            out.setdefault(r[1], {})[r[0]] = Inst(*r)
        return out

    def load_margin(self, dates: Sequence[str]) -> Dict[str, Dict[str, Margin]]:
        if not dates:
            return {}
        qs = ",".join("?" * len(dates))
        out: Dict[str, Dict[str, Margin]] = {}
        for r in self.conn.execute(f"SELECT * FROM margin WHERE date IN ({qs})", tuple(dates)):
            out.setdefault(r[1], {})[r[0]] = Margin(*r)
        return out


# --------------------------------------------------------------------------- #
# 抓取
# --------------------------------------------------------------------------- #

def fetch_dataset(http: Http, store: Store, dataset: str, d: dt.date, force: bool = False) -> int:
    """抓一個資料集一天；回傳筆數。有快取(ok/empty 且非今日)則跳過。"""
    date = ymd(d)
    status = store.fetch_status(dataset, date)
    today = dt.datetime.now(TZ_TAIPEI).date()
    if not force and status == "ok":
        return -1
    if not force and status == "empty" and d < today:
        return 0
    for url in fmt_urls(dataset, d):
        payload = http.get_json(url)
        if payload is None:
            continue
        stat = str(payload.get("stat", payload.get("status", "ok"))).lower()
        items = PARSERS[dataset](payload, date)
        if items:
            store.save(dataset, items)
            store.mark(dataset, date, "ok", len(items), url)
            log(f"  ✓ {dataset} {date}: {len(items)} 筆  ({url.split('?')[0].rsplit('/', 1)[-1]})")
            return len(items)
        if stat and stat not in ("ok", "200"):
            # 明確回應無資料（休市 / 尚未公布）
            break
    store.mark(dataset, date, "empty", 0, "")
    return 0


def ensure_history(http: Http, store: Store, end: dt.date, lookback: int, chips: bool = True,
                   max_calendar_days: int = 130) -> List[str]:
    """確保 end 往前有 lookback 個交易日的行情；籌碼只補最近 chip_days。回傳升冪交易日列表。"""
    dates_ok: List[str] = []
    d = end
    scanned = 0
    while len(dates_ok) < lookback and scanned < max_calendar_days:
        scanned += 1
        if d.weekday() >= 5:
            d -= dt.timedelta(days=1)
            continue
        got = 0
        for ds in ("twse_quotes", "tpex_quotes"):
            n = fetch_dataset(http, store, ds, d)
            got += (1 if n != 0 else 0)
        if got:
            dates_ok.append(ymd(d))
        d -= dt.timedelta(days=1)
    dates_ok.sort()
    if chips:
        for date in dates_ok[-25:]:
            dd = dt.datetime.strptime(date, "%Y%m%d").date()
            for ds in ("twse_inst", "tpex_inst", "twse_margin", "tpex_margin"):
                fetch_dataset(http, store, ds, dd)
    return dates_ok


# --------------------------------------------------------------------------- #
# 指標與評分
# --------------------------------------------------------------------------- #

@dataclass
class Signal:
    code: str
    name: str
    market: str
    close: float
    chg_pct: float
    vol: float
    vol_ratio5: float
    vol_ratio20: float
    close_pos: float
    ma5: float; ma10: float; ma20: float; ma60: Optional[float]
    bias20: float
    ret5: float
    ret20: float
    hi20_break: bool
    hi60_break: bool
    bull_align: bool
    limit_up: bool
    gap_up: bool
    foreign_net: float
    trust_net: float
    total_net: float
    foreign_streak: int
    trust_streak: int
    net5_ratio: float          # 近 5 日法人合計買超 / 近 5 日成交量
    margin_chg5: Optional[float]   # 近 5 日融資增減（張）
    margin_chg5_pct: Optional[float]
    short_margin_ratio: Optional[float]   # 券資比
    chips_date: Optional[str]
    score: float = 0.0
    tags: List[str] = field(default_factory=list)


def mean(xs: Sequence[float]) -> float:
    xs = [x for x in xs if x is not None]
    return sum(xs) / len(xs) if xs else float("nan")


def streak(values: Sequence[float]) -> int:
    """從最新往回數連續 >0 的天數。"""
    n = 0
    for v in reversed(values):
        if v is not None and v > 0:
            n += 1
        else:
            break
    return n


def build_signal(qs: List[Quote], inst: Dict[str, Inst], marg: Dict[str, Margin], dates: List[str]) -> Optional[Signal]:
    qs = [q for q in qs if q.close is not None and q.close > 0]
    if len(qs) < 21:
        return None
    qs.sort(key=lambda q: q.date)
    last = qs[-1]
    if last.date != dates[-1]:
        return None  # 最新日沒成交（停牌 / 處置）
    closes = [q.close for q in qs]
    vols = [q.volume for q in qs]
    highs = [q.high if q.high is not None else q.close for q in qs]
    prev = qs[-2]
    prev_close = prev.close
    if last.change is not None and abs((last.close - last.change) - prev_close) > max(0.5, prev_close * 0.02):
        # 快取與交易所漲跌差異過大（除權息等），以交易所提供的漲跌為準
        prev_close = last.close - last.change
    chg = last.close / prev_close - 1 if prev_close else 0.0
    ma5, ma10, ma20 = mean(closes[-5:]), mean(closes[-10:]), mean(closes[-20:])
    ma60 = mean(closes[-60:]) if len(closes) >= 60 else None
    v5 = mean(vols[-6:-1]); v20 = mean(vols[-21:-1])
    vr5 = last.volume / v5 if v5 else 0.0
    vr20 = last.volume / v20 if v20 else 0.0
    hi, lo = last.high or last.close, last.low or last.close
    close_pos = (last.close - lo) / (hi - lo) if hi > lo else 1.0
    hi20 = max(highs[-21:-1]); hi60 = max(highs[-61:-1]) if len(highs) >= 61 else max(highs[:-1])
    ret5 = last.close / closes[-6] - 1
    ret20 = last.close / closes[-21] - 1
    limit_up = chg >= 0.095
    gap_up = last.open is not None and prev.high is not None and last.open > prev.high

    # 籌碼（最新可用日，可能落後行情一天）
    inst_dates = [d for d in dates[-10:] if d in inst]
    chips_date = inst_dates[-1] if inst_dates else None
    f_series = [inst[d].foreign_net if d in inst else 0.0 for d in dates[-10:]]
    t_series = [inst[d].trust_net if d in inst else 0.0 for d in dates[-10:]]
    tot5 = sum(inst[d].total_net for d in dates[-5:] if d in inst)
    vol5_sum = sum(vols[-5:]) or 1.0
    latest_inst = inst.get(chips_date) if chips_date else None
    if inst_dates:
        # streak 只算到有資料的最後一天
        idx = dates[-10:].index(chips_date) + 1
        f_streak, t_streak = streak(f_series[:idx]), streak(t_series[:idx])
    else:
        f_streak = t_streak = 0

    m_dates = [d for d in dates[-10:] if d in marg]
    margin_chg5 = margin_chg5_pct = smr = None
    if m_dates:
        m_last = marg[m_dates[-1]]
        base = None
        for d in reversed(dates[-10:]):
            if d in marg and dates.index(d) <= dates.index(m_dates[-1]) - 5:
                base = marg[d]; break
        if base is None:
            base = marg[m_dates[0]]
        if m_last.margin_bal is not None and base.margin_prev is not None:
            margin_chg5 = m_last.margin_bal - base.margin_prev
            margin_chg5_pct = margin_chg5 / base.margin_prev if base.margin_prev else None
        if m_last.margin_bal and m_last.short_bal is not None:
            smr = m_last.short_bal / m_last.margin_bal

    return Signal(
        code=last.code, name=last.name, market=last.market, close=last.close, chg_pct=chg,
        vol=last.volume, vol_ratio5=vr5, vol_ratio20=vr20, close_pos=close_pos,
        ma5=ma5, ma10=ma10, ma20=ma20, ma60=ma60, bias20=last.close / ma20 - 1,
        ret5=ret5, ret20=ret20,
        hi20_break=last.close >= hi20, hi60_break=last.close >= hi60,
        bull_align=last.close > ma5 > ma10 > ma20, limit_up=limit_up, gap_up=gap_up,
        foreign_net=latest_inst.foreign_net if latest_inst else 0.0,
        trust_net=latest_inst.trust_net if latest_inst else 0.0,
        total_net=latest_inst.total_net if latest_inst else 0.0,
        foreign_streak=f_streak, trust_streak=t_streak, net5_ratio=tot5 / vol5_sum,
        margin_chg5=margin_chg5, margin_chg5_pct=margin_chg5_pct, short_margin_ratio=smr,
        chips_date=chips_date,
    )


def score_signal(s: Signal, strategy: str) -> None:
    sc = 0.0
    tags: List[str] = []
    # ---- 技術 / 價
    if s.hi60_break:
        sc += 25; tags.append("創60日高")
    elif s.hi20_break:
        sc += 15; tags.append("創20日高")
    if s.bull_align:
        sc += 10; tags.append("多頭排列")
    if s.ma60 is not None and s.close > s.ma60:
        sc += 5
    if s.gap_up:
        sc += 4; tags.append("跳空")
    if s.limit_up:
        sc += 8; tags.append("漲停")
    elif s.chg_pct >= 0.02:
        sc += min(10.0, s.chg_pct * 100)
    elif s.chg_pct < -0.03:
        sc -= 8
    if s.close_pos >= 0.8:
        sc += 6; tags.append("收高")
    elif s.close_pos <= 0.3:
        sc -= 5; tags.append("留上影")
    if s.ret5 >= 0.05:
        sc += 4
    # ---- 量
    if s.vol_ratio5 >= 2.5:
        sc += 14; tags.append(f"量增{s.vol_ratio5:.1f}x")
    elif s.vol_ratio5 >= 1.5:
        sc += 8; tags.append(f"量增{s.vol_ratio5:.1f}x")
    if s.vol_ratio20 >= 1.5:
        sc += 4
    # ---- 籌碼
    if s.total_net > 0:
        sc += 5
    if s.trust_streak >= 2:
        sc += 8 + min(4, s.trust_streak - 2); tags.append(f"投信連買{s.trust_streak}日")
    elif s.trust_net > 0:
        sc += 3; tags.append("投信買")
    if s.foreign_streak >= 3:
        sc += 6 + min(4, s.foreign_streak - 3); tags.append(f"外資連買{s.foreign_streak}日")
    if s.net5_ratio >= 0.10:
        sc += 6; tags.append("法人吃貨")
    elif s.net5_ratio <= -0.10:
        sc -= 6; tags.append("法人倒貨")
    if s.margin_chg5_pct is not None:
        if s.margin_chg5_pct < -0.02 and s.ret5 > 0:
            sc += 4; tags.append("資減價漲")
        elif s.margin_chg5_pct > 0.08:
            sc -= 4; tags.append("融資追高")
    if s.short_margin_ratio is not None and s.short_margin_ratio >= 0.20:
        sc += 3; tags.append(f"券資比{s.short_margin_ratio*100:.0f}%")
    # ---- 過熱懲罰
    if s.bias20 > 0.20:
        sc -= 10; tags.append("乖離大")
    if s.ret20 > 0.50:
        sc -= 5
    # ---- 策略微調
    if strategy == "breakout":
        sc += 10 if (s.hi20_break and s.vol_ratio5 >= 1.5) else -100
    elif strategy == "inst":
        ok = (s.trust_streak >= 2 or s.foreign_streak >= 3) and s.total_net > 0
        sc += 10 if ok else -100
    elif strategy == "pullback":
        near = min(abs(s.close / s.ma10 - 1), abs(s.close / s.ma20 - 1)) <= 0.03
        ok = s.bull_align or (s.ma5 > s.ma10 > s.ma20 and near)
        ok = ok and near and s.vol_ratio5 < 0.8
        if ok:
            sc += 25; tags.append("量縮回測")
        else:
            sc -= 100
    s.score = round(sc, 1)
    s.tags = tags


# --------------------------------------------------------------------------- #
# 掃描 & 輸出
# --------------------------------------------------------------------------- #

def run_scan(store: Store, dates: List[str], strategy: str, min_price: float, max_price: float,
             min_avg_vol: float, min_amount: float, market: str, watch: List[str]) -> Tuple[List[Signal], List[Signal]]:
    quotes = store.load_quotes(dates)
    inst = store.load_inst(dates[-10:])
    marg = store.load_margin(dates[-10:])
    results: List[Signal] = []
    watched: List[Signal] = []
    for code, qs in quotes.items():
        if market != "ALL" and qs[-1].market != market:
            continue
        s = build_signal(qs, inst.get(code, {}), marg.get(code, {}), dates)
        if s is None:
            continue
        score_signal(s, strategy)
        if code in watch:
            watched.append(s)
        avg_vol20 = mean([q.volume for q in qs[-20:]])
        amt = qs[-1].amount
        if not (min_price <= s.close <= max_price):
            continue
        if avg_vol20 < min_avg_vol or amt < min_amount:
            continue
        if s.score <= 0:
            continue
        results.append(s)
    results.sort(key=lambda x: (-x.score, -x.chg_pct))
    watched.sort(key=lambda x: -x.score)
    return results, watched


def signal_row(s: Signal) -> dict:
    return {
        "code": s.code, "name": s.name, "market": s.market, "score": s.score,
        "close": s.close, "chg_pct": round(s.chg_pct * 100, 2), "vol_lots": round(s.vol),
        "vol_ratio5": round(s.vol_ratio5, 2), "vol_ratio20": round(s.vol_ratio20, 2),
        "close_pos": round(s.close_pos, 2), "ma5": round(s.ma5, 2), "ma10": round(s.ma10, 2),
        "ma20": round(s.ma20, 2), "ma60": round(s.ma60, 2) if s.ma60 is not None else None,
        "bias20_pct": round(s.bias20 * 100, 2), "ret5_pct": round(s.ret5 * 100, 2), "ret20_pct": round(s.ret20 * 100, 2),
        "hi20_break": s.hi20_break, "hi60_break": s.hi60_break, "bull_align": s.bull_align,
        "limit_up": s.limit_up, "gap_up": s.gap_up,
        "foreign_net": round(s.foreign_net), "trust_net": round(s.trust_net), "total_net": round(s.total_net),
        "foreign_streak": s.foreign_streak, "trust_streak": s.trust_streak, "net5_ratio": round(s.net5_ratio, 3),
        "margin_chg5": round(s.margin_chg5) if s.margin_chg5 is not None else None,
        "margin_chg5_pct": round(s.margin_chg5_pct * 100, 2) if s.margin_chg5_pct is not None else None,
        "short_margin_ratio_pct": round(s.short_margin_ratio * 100, 1) if s.short_margin_ratio is not None else None,
        "chips_date": s.chips_date, "tags": " ".join(s.tags),
    }


def _w(s: str, width: int) -> str:
    """依東亞寬度對齊（中文佔 2 格）。"""
    import unicodedata
    n = sum(2 if unicodedata.east_asian_width(c) in "WF" else 1 for c in s)
    return s + " " * max(0, width - n)


def print_table(sigs: List[Signal], title: str, top: int) -> None:
    print(f"\n=== {title} ===")
    hdr = f"{_w('#',3)} {_w('代號',6)} {_w('名稱',10)} {_w('市',4)} {'分數':>5} {'收盤':>8} {'漲幅%':>6} {'量(張)':>8} {'量比5':>5} {'外資':>7} {'投信':>7} {'法人5日%':>7} {'融資5日':>7}  標籤"
    print(hdr)
    print("-" * 118)
    for i, s in enumerate(sigs[:top], 1):
        mchg = f"{s.margin_chg5:+.0f}" if s.margin_chg5 is not None else "  -"
        print(f"{_w(str(i),3)} {_w(s.code,6)} {_w(s.name[:5],10)} {_w('上市' if s.market=='TWSE' else '上櫃',4)} "
              f"{s.score:5.1f} {s.close:8.2f} {s.chg_pct*100:6.2f} {s.vol:8.0f} {s.vol_ratio5:5.1f} "
              f"{s.foreign_net:7.0f} {s.trust_net:7.0f} {s.net5_ratio*100:7.1f} {mchg:>7}  {' '.join(s.tags)}")


def write_outputs(sigs: List[Signal], watched: List[Signal], out_dir: str, date: str, strategy: str, top: int) -> List[str]:
    os.makedirs(out_dir, exist_ok=True)
    base = os.path.join(out_dir, f"tw_eod_scan_{date}_{strategy}")
    rows = [signal_row(s) for s in sigs[:top]]
    paths = []
    with open(base + ".csv", "w", newline="", encoding="utf-8-sig") as f:
        if rows:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            w.writeheader(); w.writerows(rows)
    paths.append(base + ".csv")
    with open(base + ".json", "w", encoding="utf-8") as f:
        json.dump({"date": date, "strategy": strategy, "generated_at": dt.datetime.now(TZ_TAIPEI).isoformat(timespec="seconds"),
                   "results": rows, "watch": [signal_row(s) for s in watched]}, f, ensure_ascii=False, indent=1)
    paths.append(base + ".json")
    with open(base + ".md", "w", encoding="utf-8") as f:
        f.write(f"# 台股盤後掃描 {date}  策略：{strategy}\n\n{STRATEGIES.get(strategy,'')}\n\n")
        f.write("| # | 代號 | 名稱 | 市場 | 分數 | 收盤 | 漲幅% | 量(張) | 量比5 | 外資 | 投信 | 投信連買 | 外資連買 | 融資5日 | 券資比% | 標籤 |\n")
        f.write("|--:|---|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|\n")
        for i, r in enumerate(rows, 1):
            f.write(f"| {i} | {r['code']} | {r['name']} | {'上市' if r['market']=='TWSE' else '上櫃'} | {r['score']} | {r['close']} | {r['chg_pct']} | "
                    f"{r['vol_lots']} | {r['vol_ratio5']} | {r['foreign_net']} | {r['trust_net']} | {r['trust_streak']} | {r['foreign_streak']} | "
                    f"{r['margin_chg5'] if r['margin_chg5'] is not None else '-'} | {r['short_margin_ratio_pct'] if r['short_margin_ratio_pct'] is not None else '-'} | {r['tags']} |\n")
        if watched:
            f.write("\n## 觀察名單\n\n| 代號 | 名稱 | 分數 | 收盤 | 漲幅% | 量比5 | 標籤 |\n|---|---|--:|--:|--:|--:|---|\n")
            for s in watched:
                f.write(f"| {s.code} | {s.name} | {s.score} | {s.close} | {s.chg_pct*100:.2f} | {s.vol_ratio5:.2f} | {' '.join(s.tags)} |\n")
        f.write("\n> 分數僅為量價籌碼綜合排序，不構成投資建議；盤中進出仍以即時量價與風控為準。\n")
    paths.append(base + ".md")
    return paths


# --------------------------------------------------------------------------- #
# 自我測試（離線合成資料）
# --------------------------------------------------------------------------- #

def _synthetic_store(path: str, n_days: int = 70, n_stocks: int = 40, seed: int = 7) -> List[str]:
    rnd = random.Random(seed)
    store = Store(path)
    d = dt.date(2026, 5, 1)
    dates = []
    while len(dates) < n_days:
        if d.weekday() < 5:
            dates.append(ymd(d))
        d += dt.timedelta(days=1)
    for k in range(n_stocks):
        code = f"{2300 + k:04d}" if k < 20 else f"{6100 + k:04d}"
        market = "TWSE" if k < 20 else "TPEX"
        px = rnd.uniform(20, 300)
        base_vol = rnd.uniform(500, 8000)
        prev_close = px
        margin = rnd.uniform(2000, 20000)
        drift = rnd.uniform(-0.002, 0.002)
        for i, date in enumerate(dates):
            r = rnd.gauss(drift, 0.02)
            vol = base_vol * rnd.uniform(0.6, 1.4)
            # 前 3 檔在最後一天做出「突破 + 量增 + 法人買」
            if k in (0, 1, 21) and i == n_days - 1:
                r = 0.06; vol = base_vol * 3.0
            if k == 2 and i >= n_days - 8:
                r = 0.03; vol = base_vol * 1.2      # 連漲 -> 乖離大
            close = round(prev_close * (1 + r), 2)
            high = round(max(close, prev_close) * (1 + abs(rnd.gauss(0, 0.005))), 2)
            low = round(min(close, prev_close) * (1 - abs(rnd.gauss(0, 0.005))), 2)
            opn = round(prev_close * (1 + rnd.gauss(0, 0.004)), 2)
            q = Quote(date, code, f"測試{k:02d}", market, opn, high, low, close, vol, close * vol * 1000, vol * 3, round(close - prev_close, 2))
            store.save("x_quotes", [q])
            f_net = rnd.gauss(0, base_vol * 0.05)
            t_net = rnd.gauss(0, base_vol * 0.02)
            if k in (0, 21) and i >= n_days - 4:
                f_net, t_net = base_vol * 0.15, base_vol * 0.08
            if k == 1 and i == n_days - 1:
                t_net = base_vol * 0.05
            dl = rnd.gauss(0, base_vol * 0.01)
            store.save("x_inst", [Inst(date, code, f_net, t_net, dl, f_net + t_net + dl)])
            m_prev = margin
            margin = max(0.0, margin + rnd.gauss(-margin * 0.005 if k == 0 else 0, margin * 0.01))
            store.save("x_margin", [Margin(date, code, m_prev, margin, margin * 0.1, margin * (0.25 if k == 1 else 0.1))])
            store.mark("twse_quotes", date, "ok", 1, "synthetic")
            prev_close = close
    store.conn.close()
    return dates


def selftest() -> int:
    tmp = tempfile.mkdtemp(prefix="tw_eod_selftest_")
    db = os.path.join(tmp, "test.sqlite")
    dates = _synthetic_store(db)
    store = Store(db)
    assert store.trading_dates() == dates, "快取交易日不一致"

    # 解析器：用交易所實際 JSON 形狀做煙霧測試
    twse_q = {"stat": "OK", "tables": [
        {"title": "大盤統計資訊", "fields": ["指數", "收盤指數"], "data": [["發行量加權", "22,000.00"]]},
        {"title": "每日收盤行情", "fields": ["證券代號", "證券名稱", "成交股數", "成交筆數", "成交金額", "開盤價", "最高價", "最低價", "收盤價", "漲跌(+/-)", "漲跌價差", "最後揭示買價", "最後揭示買量", "最後揭示賣價", "最後揭示賣量", "本益比"],
         "data": [["2330", "台積電", "25,000,000", "30,000", "24,000,000,000", "960.00", "975.00", "955.00", "970.00", "<p style= color:red>+</p>", "10.00", "969", "1", "970", "5", "20.1"],
                  ["0050", "元大台灣50", "5,000,000", "3,000", "900,000,000", "180", "182", "179", "181", "+", "1.0", "", "", "", "", ""],
                  ["2330A", "台積特", "1", "1", "1", "--", "--", "--", "--", "", "0.00", "", "", "", "", ""]]}]}
    qs = parse_twse_quotes(twse_q, "20260904")
    assert len(qs) == 1 and qs[0].code == "2330" and qs[0].volume == 25000 and qs[0].change == 10.0, qs
    twse_q_old = {"stat": "OK", "fields9": twse_q["tables"][1]["fields"], "data9": twse_q["tables"][1]["data"]}
    assert len(parse_twse_quotes(twse_q_old, "20260904")) == 1

    tpex_new = {"tables": [{"fields": ["代號", "名稱", "收盤", "漲跌", "開盤", "最高", "最低", "成交股數", "成交金額(元)", "成交筆數", "最後買價", "最後賣價", "發行股數", "次日參考價", "次日漲停價", "次日跌停價"],
                            "data": [["6488", "環球晶", "500.00", "-5.00", "505", "508", "498", "3,000,000", "1,500,000,000", "2,000", "499", "500", "1", "500", "550", "450"],
                                     ["00679B", "元大美債", "30", "0", "30", "30", "30", "1,000", "30,000", "10", "", "", "", "", "", ""]]}]}
    tq = parse_tpex_quotes(tpex_new, "20260904")
    assert len(tq) == 1 and tq[0].market == "TPEX" and tq[0].change == -5.0 and tq[0].volume == 3000, tq
    tpex_old = {"aaData": tpex_new["tables"][0]["data"], "iTotalRecords": 2}
    assert parse_tpex_quotes(tpex_old, "20260904")[0].close == 500.0

    t86 = {"stat": "OK", "fields": ["證券代號", "證券名稱", "外陸資買進股數(不含外資自營商)", "外陸資賣出股數(不含外資自營商)", "外陸資買賣超股數(不含外資自營商)", "外資自營商買進股數", "外資自營商賣出股數", "外資自營商買賣超股數", "投信買進股數", "投信賣出股數", "投信買賣超股數", "自營商買賣超股數", "自營商買進股數(自行買賣)", "自營商賣出股數(自行買賣)", "自營商買賣超股數(自行買賣)", "自營商買進股數(避險)", "自營商賣出股數(避險)", "自營商買賣超股數(避險)", "三大法人買賣超股數"],
           "data": [["2330", "台積電", "10,000,000", "8,000,000", "2,000,000", "0", "0", "0", "500,000", "100,000", "400,000", "-300,000", "0", "0", "-100,000", "0", "0", "-200,000", "2,100,000"]]}
    ins = parse_twse_inst(t86, "20260904")
    assert len(ins) == 1 and ins[0].foreign_net == 2000 and ins[0].trust_net == 400 and ins[0].dealer_net == -300 and ins[0].total_net == 2100, ins
    tpex_inst_old = {"aaData": [["6488", "環球晶"] + ["0"] * 8 + ["1,000,000"] + ["0", "0", "200,000"] + ["0"] * 8 + ["-50,000", "1,150,000"]]}
    ti = parse_tpex_inst(tpex_inst_old, "20260904")
    assert ti[0].foreign_net == 1000 and ti[0].trust_net == 200 and ti[0].dealer_net == -50 and ti[0].total_net == 1150, ti

    margn = {"stat": "OK", "tables": [
        {"fields": ["項目", "買進", "賣出"], "data": [["融資(交易單位)", "1", "2"]]},
        {"fields": ["股票代號", "股票名稱", "買進", "賣出", "現金償還", "前日餘額", "今日餘額", "次一營業日限額", "買進", "賣出", "現券償還", "前日餘額", "今日餘額", "次一營業日限額", "資券互抵", "註記"],
         "data": [["2330", "台積電", "500", "300", "10", "20,000", "20,190", "600,000", "10", "20", "0", "1,000", "1,010", "600,000", "5", ""]]}]}
    mg = parse_twse_margin(margn, "20260904")
    assert mg[0].margin_prev == 20000 and mg[0].margin_bal == 20190 and mg[0].short_bal == 1010, mg
    tpex_mg = {"aaData": [["6488", "環球晶", "5,000", "100", "50", "0", "5,050", "0", "3.1", "100,000", "200", "10", "5", "0", "205", "0", "0.1", "100,000", "0", ""]]}
    tm = parse_tpex_margin(tpex_mg, "20260904")
    assert tm[0].margin_prev == 5000 and tm[0].margin_bal == 5050 and tm[0].short_prev == 200 and tm[0].short_bal == 205, tm

    # 指標 & 評分
    results, watched = run_scan(store, dates, "momentum", 5, 5000, 100, 1e6, "ALL", ["2302"])
    assert results, "掃描無結果"
    top_codes = [s.code for s in results[:3]]
    assert set(top_codes) & {"2300", "2301", "6121"}, top_codes
    top = results[0]
    assert top.hi20_break and top.vol_ratio5 > 2 and top.chg_pct > 0.05, signal_row(top)
    s2302 = next(s for s in watched if s.code == "2302")
    assert "乖離大" in s2302.tags or s2302.bias20 > 0.15, signal_row(s2302)
    for strat in ("breakout", "inst", "pullback"):
        r, _ = run_scan(store, dates, strat, 5, 5000, 100, 1e6, "ALL", [])
        assert all(x.score > 0 for x in r)
    print_table(results, "selftest momentum（合成資料）", 10)
    out = write_outputs(results, watched, os.path.join(tmp, "out"), dates[-1], "momentum", 10)
    for p in out:
        assert os.path.getsize(p) > 0
    print(f"\n✓ selftest 通過（暫存：{tmp}）")
    return 0


# --------------------------------------------------------------------------- #
# main
# --------------------------------------------------------------------------- #

def default_scan_date() -> dt.date:
    now = dt.datetime.now(TZ_TAIPEI)
    d = now.date()
    if now.hour < 15:          # 15:00 前當日行情尚未完整公布
        d -= dt.timedelta(days=1)
    while d.weekday() >= 5:
        d -= dt.timedelta(days=1)
    return d


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="台股盤後動能選股掃描（上市+上櫃）", formatter_class=argparse.RawDescriptionHelpFormatter,
                                 epilog="策略：\n" + "\n".join(f"  {k:9s} {v}" for k, v in STRATEGIES.items()))
    ap.add_argument("--date", help="基準日 YYYYMMDD（預設：台北時間 15:00 後為今日，否則前一交易日）")
    ap.add_argument("--lookback", type=int, default=60, help="回溯交易日數（預設 60，最少 25）")
    ap.add_argument("--strategy", choices=list(STRATEGIES), default="momentum")
    ap.add_argument("--top", type=int, default=30)
    ap.add_argument("--market", choices=["ALL", "TWSE", "TPEX"], default="ALL")
    ap.add_argument("--min-price", type=float, default=10.0)
    ap.add_argument("--max-price", type=float, default=5000.0)
    ap.add_argument("--min-avg-vol", type=float, default=1000.0, help="20 日均量下限（張）")
    ap.add_argument("--min-amount", type=float, default=5e7, help="當日成交金額下限（元）")
    ap.add_argument("--watch", default="", help="觀察名單，逗號分隔代號")
    ap.add_argument("--db", default=os.path.join("data", "tw_eod.sqlite"))
    ap.add_argument("--out", default="out")
    ap.add_argument("--offline", action="store_true", help="不連網，只用快取")
    ap.add_argument("--no-chips", action="store_true", help="不抓籌碼（法人/融資）")
    ap.add_argument("--refresh", action="store_true", help="強制重抓基準日資料")
    ap.add_argument("--gap", type=float, default=REQUEST_GAP_SEC, help="請求間隔秒數")
    ap.add_argument("--selftest", action="store_true", help="離線自我測試")
    args = ap.parse_args(argv)

    try:
        sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
    except Exception:
        pass

    if args.selftest:
        return selftest()

    end = dt.datetime.strptime(args.date, "%Y%m%d").date() if args.date else default_scan_date()
    lookback = max(25, args.lookback)
    store = Store(args.db)

    if args.offline:
        dates = [d for d in store.trading_dates() if d <= ymd(end)][-lookback:]
    else:
        http = Http(gap=args.gap)
        if args.refresh:
            for ds in ENDPOINTS:
                fetch_dataset(http, store, ds, end, force=True)
        log(f"補抓行情至 {ymd(end)}，回溯 {lookback} 個交易日 …")
        dates = ensure_history(http, store, end, lookback, chips=not args.no_chips)
    if len(dates) < 25:
        log(f"交易日資料不足（{len(dates)} 天），無法計算指標。請確認網路或先不加 --offline。")
        return 2
    scan_date = dates[-1]
    if scan_date != ymd(end):
        log(f"注意：{ymd(end)} 無行情，改以最近交易日 {scan_date} 掃描")

    watch = [w.strip() for w in args.watch.split(",") if w.strip()]
    results, watched = run_scan(store, dates, args.strategy, args.min_price, args.max_price,
                                args.min_avg_vol, args.min_amount, args.market, watch)
    missing_chips = [s for s in results[:args.top] if s.chips_date != scan_date]
    print_table(results, f"{scan_date} 策略 {args.strategy}：{STRATEGIES[args.strategy]}", args.top)
    if watched:
        print_table(watched, "觀察名單", len(watched))
    if missing_chips and not args.no_chips:
        log(f"提醒：{len(missing_chips)} 檔籌碼資料落後行情日（法人/融資約 16:00~21:00 後才公布），可稍後加 --refresh 重跑。")
    paths = write_outputs(results, watched, args.out, scan_date, args.strategy, args.top)
    log("輸出：" + ", ".join(paths))
    return 0


if __name__ == "__main__":
    sys.exit(main())
