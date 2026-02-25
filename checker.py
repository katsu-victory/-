#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
診療ガイドライン新着監視: checker.py（正確性最優先版）

要点:
- 日付は「発刊日」「改訂日」「検知日」を厳密に分離
- 推測・補完は禁止（年だけ→1/1 などは絶対にしない）
- 誤った日付は欠損より悪いので、採用基準は「ラベル付き日付」に限定
- HTTP Last-Modified は参考情報として別列に隔離（発刊日/改訂日に混ぜない）
- 監査可能にするため、日付ごとに信頼レベル(level)と根拠(evidence)を保持
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
# 日付抽出: 推測禁止のため「ラベル付きのみ採用」
# =========================

PUB_LABELS = ["発行", "刊行", "発売", "公開", "公表", "掲載"]
REV_LABELS = ["改訂", "更新", "最終更新", "修正"]
BAD_CONTEXT = ["copyright", "all rights reserved", "©", "c)", "著作権"]

DATE_RE1 = re.compile(r"(20\d{2})[./-](\d{1,2})[./-](\d{1,2})")
DATE_RE2 = re.compile(r"(20\d{2})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日")

def _to_ymd(y: str, m: str, d: str) -> Optional[str]:
    """妥当な暦日だけ YYYY-MM-DD で返す。不正なら None。"""
    try:
        yy, mm, dd = int(y), int(m), int(d)
        if not (2000 <= yy <= 2099):
            return None
        # 厳密な暦チェック（誤日付 > 欠損）
        datetime(yy, mm, dd)
        return f"{yy:04d}-{mm:02d}-{dd:02d}"
    except Exception:
        return None

def _find_dates_in_line(line: str) -> List[str]:
    out: List[str] = []
    for m in DATE_RE1.finditer(line):
        d = _to_ymd(m.group(1), m.group(2), m.group(3))
        if d:
            out.append(d)
    for m in DATE_RE2.finditer(line):
        d = _to_ymd(m.group(1), m.group(2), m.group(3))
        if d:
            out.append(d)
    return out

def _has_any(s: str, keywords: List[str]) -> bool:
    s2 = s.lower()
    return any(k.lower() in s2 for k in keywords)

def _is_bad_context(s: str) -> bool:
    s2 = s.lower()
    return any(b in s2 for b in BAD_CONTEXT)

def _pick_labeled_date(lines: List[str], labels: List[str]) -> Optional[Tuple[str, str]]:
    """ラベル＋日付が同居する行から、最初の1件だけ採用。"""
    for line in lines:
        if _is_bad_context(line):
            continue
        if not _has_any(line, labels):
            continue
        ds = _find_dates_in_line(line)
        if ds:
            return ds[0], line.strip()[:200]
    return None

# =========================
# 監査用モデル
# =========================

@dataclass
class DateEvidence:
    value: str     # YYYY-MM-DD or ""(不明)
    level: str     # text/meta/pdf/header/unknown
    evidence: str  # short snippet
    source_url: str

def _unknown(url: str) -> DateEvidence:
    return DateEvidence(value="", level="unknown", evidence="", source_url=url)

# =========================
# HTTPヘッダ: Last-Modified（参考列）
# =========================

def get_last_modified(url: str) -> DateEvidence:
    try:
        res = requests.head(url, headers=HEADERS, timeout=TIMEOUT_HEAD, allow_redirects=True)
        lm = res.headers.get("Last-Modified")
        if not lm:
            return _unknown(url)
        dt = email.utils.parsedate_to_datetime(lm).astimezone(JST)
        return DateEvidence(
            value=dt.strftime("%Y-%m-%d"),
            level="header",
            evidence=f"Last-Modified: {lm}",
            source_url=url,
        )
    except Exception:
        return _unknown(url)

# =========================
# HTML抽出: 本文(text)→メタ(meta) の順
# =========================

def _extract_from_html(url: str, html: bytes) -> Dict[str, DateEvidence]:
    soup = BeautifulSoup(html, "html.parser")

    # フッター/ヘッダー等のノイズを除外（誤日付回避）
    for tag in soup(["script", "style", "noscript", "header", "footer", "nav"]):
        tag.decompose()

    text = soup.get_text("\n", strip=True)
    lines = [ln for ln in (x.strip() for x in text.split("\n")) if 3 <= len(ln) <= 200]

    pub = _pick_labeled_date(lines, PUB_LABELS)  # 学会Web: 公開日=発刊日として扱う（仕様固定）
    rev = _pick_labeled_date(lines, REV_LABELS)

    pub_ev = DateEvidence(value=pub[0], level="text", evidence=f"本文: {pub[1]}", source_url=url) if pub else _unknown(url)
    rev_ev = DateEvidence(value=rev[0], level="text", evidence=f"本文: {rev[1]}", source_url=url) if rev else _unknown(url)

    # meta/JSON-LD は本文で取れない時のみ、かつ ISO(YYYY-MM-DD...) の先頭10文字のみ採用
    def parse_iso10(s: str) -> Optional[str]:
        if not s:
            return None
        s = str(s).strip()
        if len(s) >= 10 and re.match(r"^\d{4}-\d{2}-\d{2}$", s[:10]):
            try:
                datetime.strptime(s[:10], "%Y-%m-%d")
                return s[:10]
            except Exception:
                return None
        return None

    meta_candidates: List[Tuple[str, str]] = []
    # OpenGraph / article
    for prop in ["article:published_time", "article:modified_time", "og:updated_time"]:
        m = soup.find("meta", property=prop)
        if m and m.get("content"):
            meta_candidates.append((prop, m["content"].strip()))
    # name系
    for name in ["date", "dc.date", "dc.date.issued", "dc.date.modified", "citation_publication_date", "citation_date"]:
        m = soup.find("meta", attrs={"name": name})
        if m and m.get("content"):
            meta_candidates.append((name, m["content"].strip()))

    # JSON-LD
    for sc in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(sc.get_text(strip=True))
            items = data if isinstance(data, list) else [data]
            for it in items:
                if not isinstance(it, dict):
                    continue
                dp = it.get("datePublished")
                dm = it.get("dateModified")
                if dp:
                    meta_candidates.append(("jsonld:datePublished", str(dp)))
                if dm:
                    meta_candidates.append(("jsonld:dateModified", str(dm)))
        except Exception:
            continue

    if pub_ev.value == "":
        for k, v in meta_candidates:
            p = parse_iso10(v)
            if not p:
                continue
            # 発刊(公開)は published/issued 系だけ
            lk = k.lower()
            if "published" in lk or "issued" in lk or "citation_publication_date" in lk:
                pub_ev = DateEvidence(value=p, level="meta", evidence=f"meta({k}): {v[:120]}", source_url=url)
                break

    if rev_ev.value == "":
        for k, v in meta_candidates:
            p = parse_iso10(v)
            if not p:
                continue
            lk = k.lower()
            if "modified" in lk or "updated" in lk:
                rev_ev = DateEvidence(value=p, level="meta", evidence=f"meta({k}): {v[:120]}", source_url=url)
                break

    return {"publication": pub_ev, "revision": rev_ev}

# =========================
# PDF抽出: 本文1-2ページのみ（ラベル付きのみ採用）
# =========================

def _extract_from_pdf(url: str) -> Dict[str, DateEvidence]:
    try:
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT_GET)
        r.raise_for_status()
        data = r.content
    except Exception:
        return {"publication": _unknown(url), "revision": _unknown(url)}

    reader = None
    try:
        from pypdf import PdfReader  # type: ignore
        reader = PdfReader(io.BytesIO(data))
    except Exception:
        try:
            from PyPDF2 import PdfReader  # type: ignore
            reader = PdfReader(io.BytesIO(data))
        except Exception:
            return {"publication": _unknown(url), "revision": _unknown(url)}

    texts: List[str] = []
    try:
        pages = getattr(reader, "pages", [])
        for i in range(min(2, len(pages))):
            try:
                t = pages[i].extract_text() or ""
                texts.append(t)
            except Exception:
                continue
    except Exception:
        pass

    joined = "\n".join(texts)
    lines = [ln.strip() for ln in joined.split("\n") if 3 <= len(ln.strip()) <= 200]

    # PDF本文に「更新：YYYY年…」しか無い場合は改訂日として扱い、発刊日は不明（仕様固定）
    pub = _pick_labeled_date(lines, PUB_LABELS)
    rev = _pick_labeled_date(lines, REV_LABELS)

    pub_ev = DateEvidence(value=pub[0], level="pdf", evidence=f"PDF本文: {pub[1]}", source_url=url) if pub else _unknown(url)
    rev_ev = DateEvidence(value=rev[0], level="pdf", evidence=f"PDF本文: {rev[1]}", source_url=url) if rev else _unknown(url)

    return {"publication": pub_ev, "revision": rev_ev}

# =========================
# 正規化（論理ID安定化用）
# =========================

def normalize_title(title: str) -> str:
    t = (title or "").lower()
    t = re.sub(r"\s+", "", t)
    t = re.sub(r"\d{4}", "", t)  # 年は揺れるので除去
    t = re.sub(r"(年版|改訂|版|ver\.?|version)", "", t)
    t = re.sub(r"(について|概要|解説|about)", "", t)
    t = re.sub(r"[^\wぁ-んァ-ン一-龥]", "", t)
    return t.strip()[:80]  # 伸び過ぎ防止

def extract_year_hint(text: str) -> str:
    """版情報のヒントとして年だけ抽出（=日付とは別物）。取れなければ空欄。"""
    if not text:
        return ""
    m = re.search(r"(20\d{2})", text)
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
# URLの中身から発刊/改訂を抽出（typeに応じて）
# =========================

def extract_dates_for_url(url: str) -> Dict[str, DateEvidence]:
    # PDF判定（拡張子 or Content-Type）
    is_pdf = url.lower().endswith(".pdf")
    if not is_pdf:
        try:
            h = requests.head(url, headers=HEADERS, timeout=TIMEOUT_HEAD, allow_redirects=True)
            ct = (h.headers.get("Content-Type") or "").lower()
            if "application/pdf" in ct:
                is_pdf = True
        except Exception:
            pass

    if is_pdf:
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
        for el in soup.select(target["selector"]):
            # aタグでない場合もあるので「テキスト」と「リンク」を安全に扱う
            text = el.get_text(strip=True) or ""
            if not text:
                continue
            if not any(k in text for k in KEYWORDS):
                continue
            if not (8 < len(text) < 250):
                continue

            href = el.get("href")
            url = urljoin(target["url"], href) if href else target["url"]
            norm = normalize_title(text)

            dates = extract_dates_for_url(url)
            lm = get_last_modified(url)

            rows.append({
                "論理ID": f"{target['publisher_key']}_{norm}",
                "正式タイトル": text,
                "出版社": target["name"],
                "種別": "Web",
                "版情報": extract_year_hint(text),
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
    "論理ID",
    "正式タイトル",
    "出版社",
    "種別",
    "版情報",
    "発刊日",
    "発刊日_level",
    "発刊日_evidence",
    "改訂日",
    "改訂日_level",
    "改訂日_evidence",
    "検知日",
    "HTTP最終更新日",
    "HTTP最終更新日_level",
    "HTTP最終更新日_evidence",
    "URL",
    "ステータス",
    "初回検知日",
    "最終確認日",
    "CSV更新日時",
]

def _ensure_columns(df: pd.DataFrame) -> pd.DataFrame:
    for c in CSV_COLUMNS:
        if c not in df.columns:
            df[c] = ""
    return df[CSV_COLUMNS]

def main() -> None:
    print("=== Collecting current data ===")
    current_rows: List[Dict] = []
    for t in TARGETS:
        print(f"Checking {t['name']}")
        current_rows.extend(check_site(t))

    current_df = pd.DataFrame(current_rows)
    if current_df.empty:
        print("No data collected today.")
        return

    # 旧CSV読み込み（後方互換: 足りない列は空欄で補う）
    if os.path.exists(REPORT_FILE):
        old = pd.read_csv(REPORT_FILE, dtype=str).fillna("")
    else:
        old = pd.DataFrame(columns=CSV_COLUMNS)

    old = _ensure_columns(old)
    current_df = _ensure_columns(current_df)

    old = old.set_index("論理ID", drop=False)
    current_df = current_df.set_index("論理ID", drop=False)

    merged = old.copy()

    for lid, row in current_df.iterrows():
        if lid in merged.index:
            # 既知: 現在値で上書き（監視結果が最新）
            for c in CSV_COLUMNS:
                if c in ["ステータス", "初回検知日", "最終確認日", "CSV更新日時"]:
                    continue
                merged.loc[lid, c] = row.get(c, merged.loc[lid, c])

            merged.loc[lid, "ステータス"] = "既知"
            merged.loc[lid, "最終確認日"] = TODAY
            # 初回検知日は保持（空なら今回）
            if (merged.loc[lid, "初回検知日"] or "") == "":
                merged.loc[lid, "初回検知日"] = TODAY
        else:
            # 新規
            new_row = {c: row.get(c, "") for c in CSV_COLUMNS}
            new_row["ステータス"] = "★新着"
            new_row["初回検知日"] = TODAY
            new_row["最終確認日"] = TODAY
            merged.loc[lid] = new_row

    final_df = merged.reset_index(drop=True)

    # 重複整理（論理ID単位で1行に統合。URLの揺れがある場合は先頭優先）
    final_df = (
        final_df.sort_values(["論理ID", "URL"])
        .groupby("論理ID", as_index=False)
        .first()
    )

    # CSV更新日時（JSTで明示）
    final_df["CSV更新日時"] = datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S %z")

    # 表示用ソート（CSVとしての安定性）
    final_df = final_df.sort_values(["出版社", "論理ID"])

    final_df = _ensure_columns(final_df)
    final_df.to_csv(REPORT_FILE, index=False, encoding="utf-8-sig")
    print(f"Saved {REPORT_FILE}")

if __name__ == "__main__":
    main()
