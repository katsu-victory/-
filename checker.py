#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
診療ガイドライン新着監視: checker.py（正確性最優先・完全修正版）
"""

import os
import re
import json
import io
import email.utils
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

# =========================
# 基本設定
# =========================

JST = timezone(timedelta(hours=+9))
TODAY = datetime.now(JST).strftime("%Y-%m-%d")
REPORT_FILE = "update_report.csv"

KEYWORDS = ["ガイドライン", "指針", "診療手引き", "診療指針", "治療指針", "取扱い規約"]
HEADERS = {"User-Agent": "Mozilla/5.0"}

TIMEOUT_GET = 30
TIMEOUT_HEAD = 20

# =========================
# 日付抽出
# =========================

PUB_LABELS = ["発行日", "発行", "発刊日", "発刊", "刊行", "発売", "公開", "公表", "掲載"]
REV_LABELS = ["改訂", "更新", "最終更新", "修正"]
BAD_CONTEXT = ["copyright", "all rights reserved", "©", "c)", "著作権"]

DATE_RE1 = re.compile(r"(20\d{2})[./-](\d{1,2})[./-](\d{1,2})")
DATE_RE2 = re.compile(r"(20\d{2})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日")

def _to_ymd(y: str, m: str, d: str) -> Optional[str]:
    try:
        yy, mm, dd = int(y), int(m), int(d)
        datetime(yy, mm, dd)
        return f"{yy:04d}-{mm:02d}-{dd:02d}"
    except Exception:
        return None

def _find_dates(line: str) -> List[str]:
    out = []
    for r in (DATE_RE1, DATE_RE2):
        for m in r.finditer(line):
            d = _to_ymd(m.group(1), m.group(2), m.group(3))
            if d:
                out.append(d)
    return out

def _has_any(s: str, words: List[str]) -> bool:
    s = s.lower()
    return any(w.lower() in s for w in words)

def _is_bad(s: str) -> bool:
    s = s.lower()
    return any(b in s for b in BAD_CONTEXT)

def _pick_labeled_date(
    lines: List[str],
    labels: List[str],
    window: int = 1,
    *,
    pick_oldest: bool = False,
    pick_latest: bool = False,
) -> Optional[Tuple[str, str]]:
    """
    検索対象行とその前後の行からラベル付き日付を抽出し、
    指定された戦略に従って最適な日付を返す。

    変更点:
    - 複数候補がある場合、発行日は最古のものを採用し、改訂日は最新のものを採用する。
    - BAD_CONTEXT を含む行は無視する。
    - 同じ行に日付がない場合は、前後の行も参照する。

    引数:
        lines: テキスト行リスト
        labels: 検索キーワードリスト
        window: ラベル行の前後何行までを見るか
        pick_oldest: True の場合は最も古い日付を選ぶ
        pick_latest: True の場合は最新の日付を選ぶ

    戻り値:
        (日付文字列, 証拠文字列) or None
    """
    candidates: List[Tuple[str, str]] = []
    for i, line in enumerate(lines):
        if _is_bad(line):
            continue
        if not _has_any(line, labels):
            continue
        # 自行の候補日付
        for d in _find_dates(line):
            candidates.append((d, line[:200]))
        # 前後の行にも日付があるか確認。ただし別のラベル行を避ける。
        for j in range(max(0, i - window), min(len(lines), i + window + 1)):
            if j == i:
                continue
            neighbour = lines[j]
            # 他のラベルが含まれている場合は飛ばす（発行と改訂を混同しない）
            if _has_any(neighbour, PUB_LABELS) or _has_any(neighbour, REV_LABELS):
                continue
            for d2 in _find_dates(neighbour):
                candidates.append((d2, f"{line} / {neighbour}"[:200]))
    if not candidates:
        return None
    # 重複排除
    unique = {}
    for d, ctx in candidates:
        # d がすでにある場合はより短いコンテキストを保持
        if d not in unique or len(ctx) < len(unique[d]):
            unique[d] = ctx
    # 日付を昇順にソート
    def to_dt(d: str) -> datetime:
        try:
            return datetime.strptime(d, "%Y-%m-%d")
        except Exception:
            return datetime.max
    items = sorted(unique.items(), key=lambda x: to_dt(x[0]))
    # pick_oldest なら最古、pick_latest なら最新、それ以外は先頭を返す
    if pick_oldest:
        d, ctx = items[0]
        return d, ctx
    if pick_latest:
        d, ctx = items[-1]
        return d, ctx
    d, ctx = items[0]
    return d, ctx

# =========================
# 日付モデル
# =========================

@dataclass
class DateEvidence:
    value: str
    level: str
    evidence: str
    source_url: str

def _unknown(url: str) -> DateEvidence:
    return DateEvidence("", "unknown", "", url)

# =========================
# HTTPヘッダ
# =========================

def get_last_modified(url: str) -> DateEvidence:
    try:
        r = requests.head(url, headers=HEADERS, timeout=TIMEOUT_HEAD, allow_redirects=True)
        lm = r.headers.get("Last-Modified")
        if not lm:
            return _unknown(url)
        dt = email.utils.parsedate_to_datetime(lm).astimezone(JST)
        return DateEvidence(dt.strftime("%Y-%m-%d"), "header", lm, url)
    except Exception:
        return _unknown(url)

# =========================
# HTML抽出
# =========================

def _extract_from_html(url: str, html: bytes) -> Dict[str, DateEvidence]:
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript", "header", "footer", "nav"]):
        tag.decompose()

    text = soup.get_text("\n", strip=True)
    lines = [l for l in (x.strip() for x in text.split("\n")) if 3 <= len(l) <= 200]

    def parse_iso10(s: str) -> Optional[str]:
        if s and re.match(r"^\d{4}-\d{2}-\d{2}", s):
            try:
                datetime.strptime(s[:10], "%Y-%m-%d")
                return s[:10]
            except Exception:
                return None
        return None

    json_pub = None
    json_rev = None

    for sc in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(sc.get_text(strip=True))
            items = data if isinstance(data, list) else [data]
            for it in items:
                if isinstance(it, dict):
                    if not json_pub and it.get("datePublished"):
                        p = parse_iso10(str(it["datePublished"]))
                        if p:
                            json_pub = DateEvidence(p, "meta", "jsonld:datePublished", url)
                    if not json_rev and it.get("dateModified"):
                        p = parse_iso10(str(it["dateModified"]))
                        if p:
                            json_rev = DateEvidence(p, "meta", "jsonld:dateModified", url)
        except Exception:
            continue

    meta_pub = None
    meta_rev = None

    for prop in ["article:published_time", "article:modified_time", "og:updated_time"]:
        m = soup.find("meta", property=prop)
        if m and m.get("content"):
            p = parse_iso10(m["content"])
            if p:
                if "published" in prop:
                    meta_pub = DateEvidence(p, "meta", prop, url)
                else:
                    meta_rev = DateEvidence(p, "meta", prop, url)

    # 発行日は候補が複数ある場合は古い日付を優先、改訂日は新しい日付を優先する
    pub_text = _pick_labeled_date(lines, PUB_LABELS, pick_oldest=True)
    rev_text = _pick_labeled_date(lines, REV_LABELS, pick_latest=True)

    pub = json_pub or meta_pub or (DateEvidence(pub_text[0], "text", pub_text[1], url) if pub_text else _unknown(url))
    rev = json_rev or meta_rev or (DateEvidence(rev_text[0], "text", rev_text[1], url) if rev_text else _unknown(url))

    return {"publication": pub, "revision": rev}

# =========================
# PDF抽出
# =========================

def _extract_from_pdf(url: str) -> Dict[str, DateEvidence]:
    try:
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT_GET)
        r.raise_for_status()
        data = r.content
    except Exception:
        return {"publication": _unknown(url), "revision": _unknown(url)}

    try:
        from pypdf import PdfReader
        reader = PdfReader(io.BytesIO(data))
    except Exception:
        return {"publication": _unknown(url), "revision": _unknown(url)}

    texts = []
    for i in range(min(2, len(reader.pages))):
        try:
            texts.append(reader.pages[i].extract_text() or "")
        except Exception:
            pass

    lines = [l.strip() for l in "\n".join(texts).split("\n") if 3 <= len(l.strip()) <= 200]

    # PDFでも同様に発行日は最古、改訂日は最新を選択
    pub = _pick_labeled_date(lines, PUB_LABELS, pick_oldest=True)
    rev = _pick_labeled_date(lines, REV_LABELS, pick_latest=True)

    return {
        "publication": DateEvidence(pub[0], "pdf", pub[1], url) if pub else _unknown(url),
        "revision": DateEvidence(rev[0], "pdf", rev[1], url) if rev else _unknown(url),
    }

# =========================
# 正規化
# =========================

def normalize_title(title: str) -> str:
    t = title.lower()
    t = re.sub(r"\d{4}", "", t)
    t = re.sub(r"[^\wぁ-んァ-ン一-龥]", "", t)
    return t[:80]

def extract_year_hint(text: str) -> str:
    m = re.search(r"(20\d{2})", text or "")
    return m.group(1) if m else ""

# =========================
# 監視対象
# =========================

TARGETS = [
    # 出版社系
    {
        "name": "医学図書出版",
        "publisher_key": "igakutosho",
        "url": "https://igakutosho.co.jp/collections/book",
        "selector": "div.grid-view-item, .product-card",
        "type": "html",
    },
    {
        "name": "メディカルレビュー社",
        "publisher_key": "medical_review",
        "url": "https://med.m-review.co.jp/merebo/products/book",
        "selector": ".product_list_item, li",
        "type": "html",
    },
    {
        "name": "診断と治療社",
        "publisher_key": "shindan",
        "url": "https://www.shindan.co.jp/",
        "selector": "dl, dt, li",
        "type": "html",
    },
    {
        "name": "南江堂",
        "publisher_key": "nankodo",
        "url": "https://www.nankodo.co.jp/shinkan/list.aspx?div=d",
        "selector": "tr, div.shinkan-item",
        "type": "html",
    },
    {
        "name": "医学書院",
        "publisher_key": "igakushoin",
        "url": "https://www.igaku-shoin.co.jp/",
        "selector": "div.book-item, li",
        "type": "html",
    },
    {
        "name": "金原出版(GL検索)",
        "publisher_key": "kanehara_gl",
        "url": "https://www.kanehara-shuppan.co.jp/books/search_list.html?d=08&c=02",
        "selector": "div.book_list_item, tr, li",
        "type": "html",
    },
    {
        "name": "金原出版(規約検索)",
        "publisher_key": "kanehara_rule",
        "url": "https://www.kanehara-shuppan.co.jp/books/search_list.html?d=08&c=01",
        "selector": "div.book_list_item, tr, li",
        "type": "html",
    },
    {
        "name": "金原出版(規約PDF)",
        "publisher_key": "kanehara_rule_pdf",
        "url": "https://www.kanehara-shuppan.co.jp/_data/books/ky_new.pdf",
        "type": "pdf",
    },
    {
        "name": "金原出版(GL PDF)",
        "publisher_key": "kanehara_gl_pdf",
        "url": "https://www.kanehara-shuppan.co.jp/_data/books/gl_new.pdf",
        "type": "pdf",
    },

    # 学会系
    {
        "name": "日本婦人科腫瘍学会",
        "publisher_key": "jsgo",
        "url": "https://jsgo.or.jp/guideline/",
        "selector": "a",
        "type": "html",
    },
    {
        "name": "日本肺癌学会",
        "publisher_key": "haigan",
        "url": "https://www.haigan.gr.jp/publication/guideline/examination/2025/",
        "selector": "a",
        "type": "html",
    },
    {
        "name": "日本泌尿器科学会",
        "publisher_key": "urol",
        "url": "https://www.urol.or.jp/other/guideline/",
        "selector": "a[href$='.pdf']",
        "type": "html_pdf_index",
    },
    {
        "name": "日本乳癌学会",
        "publisher_key": "jbcs",
        "url": "https://jbcs.xsrv.jp/guideline/2022/",
        "selector": "a",
        "type": "html",
    },
    {
        "name": "日本頭頸部癌学会",
        "publisher_key": "jshnc",
        "url": "http://www.jshnc.umin.ne.jp/guideline.html",
        "selector": "a",
        "type": "html",
    },
    {
        "name": "日本肝臓学会",
        "publisher_key": "jsh",
        "url": "https://www.jsh.or.jp/medical/guidelines/jsh_guidlines/medical/",
        "selector": "a",
        "type": "html",
    },
]

# =========================
# 抽出
# =========================

def extract_dates_for_url(url: str) -> Dict[str, DateEvidence]:
    if url.lower().endswith(".pdf"):
        return _extract_from_pdf(url)
    try:
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT_GET)
        r.raise_for_status()
        return _extract_from_html(url, r.content)
    except Exception:
        return {"publication": _unknown(url), "revision": _unknown(url)}

# =========================
# サイトチェック
# =========================

def check_site(target: Dict) -> List[Dict]:
    rows: List[Dict] = []

    try:
        # --- PDF単体（一覧ではなく単一PDFの監視） ---
        if target["type"] == "pdf":
            url = target["url"]
            dates = extract_dates_for_url(url)
            lm = get_last_modified(url)

            rows.append({
                "論理ID": f"{target['publisher_key']}_pdf",
                "正式タイトル": target["name"],
                "出版社": target["name"],
                "種別": "PDF",
                "版情報": "",
                "発刊日": dates["publication"].value,
                "発刊日_level": dates["publication"].level,
                "発刊日_evidence": dates["publication"].evidence,
                "改訂日": dates["revision"].value,
                "改訂日_level": dates["revision"].level,
                "改訂日_evidence": dates["revision"].evidence,
                "検知日": TODAY,
                "HTTP最終更新日": lm.value,
                "HTTP最終更新日_level": lm.level,
                "HTTP最終更新日_evidence": lm.evidence,
                "URL": url,
            })
            return rows

        # --- HTML一覧ページ取得 ---
        res = requests.get(target["url"], headers=HEADERS, timeout=TIMEOUT_GET)
        res.raise_for_status()
        soup = BeautifulSoup(res.content, "html.parser")

        # --- PDFリンク一覧（リンク先=PDFを読む） ---
        if target["type"] == "html_pdf_index":
            for a in soup.select(target["selector"]):
                title = a.get_text(strip=True) or ""
                if not title:
                    continue
                if not any(k in title for k in KEYWORDS):
                    continue

                href = a.get("href")
                if not href:
                    continue
                url = urljoin(target["url"], href)

                norm = normalize_title(title)
                dates = extract_dates_for_url(url)  # PDF本文からラベル付きのみ
                lm = get_last_modified(url)

                rows.append({
                    "論理ID": f"{target['publisher_key']}_{norm}",
                    "正式タイトル": title,
                    "出版社": target["name"],
                    "種別": "PDF",
                    "版情報": extract_year_hint(title),
                    "発刊日": dates["publication"].value,
                    "発刊日_level": dates["publication"].level,
                    "発刊日_evidence": dates["publication"].evidence,
                    "改訂日": dates["revision"].value,
                    "改訂日_level": dates["revision"].level,
                    "改訂日_evidence": dates["revision"].evidence,
                    "検知日": TODAY,
                    "HTTP最終更新日": lm.value,
                    "HTTP最終更新日_level": lm.level,
                    "HTTP最終更新日_evidence": lm.evidence,
                    "URL": url,
                })
            return rows

        # --- 通常HTML（リンク先ページを見てラベル付き日付を探す） ---
        # 以下の一覧ページでは、商品情報が tr や li にまとめられており、
        # 内部に複数の a タグ（画像リンク・タイトルリンク・在庫リンクなど）が存在する。
        # そのため、各要素を走査し、キーワードを含むタイトルリンクを取得して処理する。
        # 同一 URL の重複処理を防ぐために一度処理した URL はスキップする。
        seen_urls: set[str] = set()

        for el in soup.select(target["selector"]):
            anchor: Optional[BeautifulSoup] = None
            # el 自身が a タグで href を持つ場合、そのまま採用
            if el.name == "a" and el.get("href"):
                anchor = el
            else:
                # 内側の a タグを走査し、タイトルにガイドライン関連キーワードが含まれるものを採用
                for a in el.find_all("a"):
                    href = a.get("href")
                    if not href:
                        continue
                    t = a.get_text(strip=True) or ""
                    # 画像リンクや在庫リンクなどタイトル以外のリンクはスキップ
                    if not any(k in t for k in KEYWORDS):
                        continue
                    anchor = a
                    break
            # キーワードを含むリンクが見つからない場合は無視
            if not anchor:
                continue
            title = anchor.get_text(strip=True) or ""
            # 空白や極端に短い/長いタイトルはノイズとして除外
            if not title or not (8 < len(title) < 250):
                continue
            # ガイドラインを示すキーワードが含まれない場合は対象外
            if not any(k in title for k in KEYWORDS):
                continue
            href = anchor.get("href")
            if not href:
                continue
            url = urljoin(target["url"], href)
            # 既に同じ URL を処理済みならスキップ
            if url in seen_urls:
                continue
            seen_urls.add(url)
            norm = normalize_title(title)

            # ページ先で発刊日・改訂日を抽出
            dates = extract_dates_for_url(url)
            # HTTPヘッダから Last-Modified を取得
            lm = get_last_modified(url)

            rows.append({
                "論理ID": f"{target['publisher_key']}_{norm}",
                "正式タイトル": title,
                "出版社": target["name"],
                "種別": "Web",
                "版情報": extract_year_hint(title),
                "発刊日": dates["publication"].value,
                "発刊日_level": dates["publication"].level,
                "発刊日_evidence": dates["publication"].evidence,
                "改訂日": dates["revision"].value,
                "改訂日_level": dates["revision"].level,
                "改訂日_evidence": dates["revision"].evidence,
                "検知日": TODAY,
                "HTTP最終更新日": lm.value,
                "HTTP最終更新日_level": lm.level,
                "HTTP最終更新日_evidence": lm.evidence,
                "URL": url,
            })

    except Exception as e:
        print(f"[ERROR] {target.get('name','?')} {e}")

    return rows

# =========================
# メイン
# =========================

CSV_COLUMNS = [
    "論理ID","正式タイトル","出版社","種別","版情報",
    "発刊日","発刊日_level","発刊日_evidence",
    "改訂日","改訂日_level","改訂日_evidence",
    "検知日","HTTP最終更新日","HTTP最終更新日_level","HTTP最終更新日_evidence",
    "URL","ステータス","初回検知日","最終確認日","CSV更新日時"
]

def _ensure(df: pd.DataFrame) -> pd.DataFrame:
    for c in CSV_COLUMNS:
        if c not in df.columns:
            df[c] = ""
    return df[CSV_COLUMNS]

def main():
    print("=== Collecting current data ===")
    rows: List[Dict] = []

    for t in TARGETS:
        print(f"Checking {t['name']}")
        rows.extend(check_site(t))

    if not rows:
        print("No data collected.")
        return

    # 新規取得データをDataFrameに変換
    current = _ensure(pd.DataFrame(rows))

    # 既存のレポートを読み込む（存在しない場合は空）
    if os.path.exists(REPORT_FILE):
        old = _ensure(pd.read_csv(REPORT_FILE, dtype=str).fillna(""))
    else:
        old = pd.DataFrame(columns=CSV_COLUMNS)

    # レポート更新のために、論理ID単位で旧データと新規データをマージする。
    # concat で旧→新の順に結合し、同一IDであれば新規データを優先して選択する。
    combined = pd.concat([old, current], ignore_index=True, sort=False)

    final_rows = []
    today_dt = datetime.strptime(TODAY, "%Y-%m-%d")

    for logical_id, group in combined.groupby("論理ID", sort=False):
        # 最新行を選択：検知日が今日の行があればそれを使用、なければ最後の行を使用
        idx_new = group.index[group["検知日"] == TODAY]
        if len(idx_new) > 0:
            row = group.loc[idx_new[0]].copy()
        else:
            row = group.iloc[-1].copy()

        # 初回検知日を保持（旧レポートに存在する場合はそれを引き継ぐ）
        # group 内には旧データと新規データの両方が存在する可能性がある。
        first_detect = group["初回検知日"].dropna().iloc[0] if any(group["初回検知日"].astype(str).str.strip()) else ""
        # 初回検知日がない場合（初登場）は今日の日付を入れる
        if not first_detect:
            row["初回検知日"] = TODAY
        else:
            row["初回検知日"] = first_detect

        # 最終確認日は今回の実行日
        row["最終確認日"] = TODAY

        # ステータス判定：発刊日がある場合にその日付から7日以内を新着とする
        status = "既知"
        pub_date_str = str(row["発刊日"]).strip() if not pd.isna(row["発刊日"]) else ""
        try:
            if pub_date_str:
                pub_dt = datetime.strptime(pub_date_str, "%Y-%m-%d")
                delta_days = (today_dt - pub_dt).days
                if 0 <= delta_days <= 7:
                    status = "★新着"
            else:
                # 発刊日が不明でも今回初登場の場合は新着扱い
                if not first_detect or first_detect == TODAY:
                    status = "★新着"
        except Exception:
            # 日付解析に失敗した場合は既知とする
            pass

        row["ステータス"] = status

        # CSV更新日時は後で全体に一括設定する
        final_rows.append(row)

    updated_df = _ensure(pd.DataFrame(final_rows))
    updated_df["CSV更新日時"] = datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S %z")
    updated_df = updated_df.sort_values(["出版社", "論理ID"])
    updated_df.to_csv(REPORT_FILE, index=False, encoding="utf-8-sig")
    print(f"Saved {REPORT_FILE}")

if __name__ == "__main__":
    main()



